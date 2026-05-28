"""
main.py
~~~~~~~
Entry point for the ``parquet_writer`` Cloud Run Job.

Role in the pipeline
---------------------
This job is the **consumer** half of the Pub/Sub-decoupled ingestion pipeline.
The ``fetch_users`` job (producer) publishes individual league-entry records as
JSON messages to the configured Pub/Sub topic.  This job pulls those messages,
accumulates them in an in-RAM Parquet buffer (128 MB threshold), and flushes
the buffer to GCS as snappy-compressed Parquet files.

Why a separate job?
-------------------
Decoupling via Pub/Sub provides:

* **Durability** — messages survive ``fetch_users`` failures.
* **Load balancing** — multiple ``parquet_writer`` instances can run concurrently
  consuming from the same subscription.
* **Retry semantics** — unprocessed messages are retried by Pub/Sub up to the
  configured retention period (7 days).

Execution model
---------------
The job pulls messages in batches of ``PUBSUB_MAX_MESSAGES`` (default 500).
After each batch the buffer is checked; if it meets the flush threshold the
buffer is serialised and uploaded to GCS.

The job stops naturally when the subscription is empty for
``PUBSUB_EMPTY_BACKOFF_MAX_S`` seconds (configurable) or when the wall-clock
``MAX_RUNTIME_SECONDS`` budget is exceeded, whichever comes first.

Exit codes
----------
0   Normal completion.
1   Fatal configuration or unrecoverable error.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time

# Allow importing from root 'config' package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from google.api_core.exceptions import DeadlineExceeded
from google.cloud import pubsub_v1  # type: ignore[import-untyped]

# buffer.py and uploader.py are copied from jobs/fetch_users/pipeline/ by the Dockerfile
from pipeline.buffer import ParquetBuffer
from pipeline.uploader import GCSUploader

from config.gcs_config import GCSConfig
from config.pipeline_config import PipelineConfig

# ---------------------------------------------------------------------------
# Logging — structured for Cloud Logging ingestion
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("parquet_writer")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DEFAULT_MAX_MESSAGES = 500
_DEFAULT_EMPTY_BACKOFF_MAX_S = 60  # stop if subscription empty for this long
_DEFAULT_EMPTY_POLL_INTERVAL_S = 5  # seconds between empty-subscription polls


def run(pipeline_cfg: PipelineConfig, gcs_cfg: GCSConfig) -> None:
    """Pull from Pub/Sub and flush Parquet → GCS.

    Args:
        pipeline_cfg: Pipeline execution configuration.
        gcs_cfg: GCS storage configuration.
    """
    start_time = time.monotonic()

    # -- Read runtime config ------------------------------------------------
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT", "")
    subscription_id = os.environ.get("PUBSUB_SUBSCRIPTION_ID", "")
    platform = os.environ.get("RIOT_PLATFORM", "unknown")
    max_messages = int(os.environ.get("PUBSUB_MAX_MESSAGES", str(_DEFAULT_MAX_MESSAGES)))
    empty_backoff_max = int(
        os.environ.get("PUBSUB_EMPTY_BACKOFF_MAX_S", str(_DEFAULT_EMPTY_BACKOFF_MAX_S))
    )
    poll_interval = int(
        os.environ.get("PUBSUB_POLL_INTERVAL_S", str(_DEFAULT_EMPTY_POLL_INTERVAL_S))
    )

    if not project_id or not subscription_id:
        raise ValueError(
            "GOOGLE_CLOUD_PROJECT and PUBSUB_SUBSCRIPTION_ID are required."
        )

    subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"

    # -- Initialise core objects --------------------------------------------
    subscriber = pubsub_v1.SubscriberClient()
    
    # We maintain a dictionary of buffers mapped by the partition key: (platform, tier, division)
    buffers: dict[tuple[str, str, str], ParquetBuffer] = {}
    
    # We maintain a dictionary of uploaders per platform
    uploaders: dict[str, GCSUploader] = {}

    logger.info(
        "parquet_writer started — project=%s subscription=%s platform=%s "
        "threshold=%d MB runtime_budget=%ds",
        project_id,
        subscription_id,
        platform,
        pipeline_cfg.parquet_buffer_mb,
        pipeline_cfg.max_runtime_seconds,
    )

    total_records = 0
    last_received_time = time.monotonic()

    try:
        while True:
            # ---- Time budget guard ----------------------------------------
            elapsed = time.monotonic() - start_time
            if elapsed >= pipeline_cfg.max_runtime_seconds:
                logger.warning(
                    "Time budget of %ds exceeded after %.0fs — flushing and exiting.",
                    pipeline_cfg.max_runtime_seconds,
                    elapsed,
                )
                break

            # ---- Empty-subscription guard ----------------------------------
            idle_time = time.monotonic() - last_received_time
            if idle_time >= empty_backoff_max:
                logger.info(
                    "No messages received for %.0fs — subscription appears empty. Exiting.",
                    idle_time,
                )
                break

            # ---- Pull a batch from Pub/Sub ---------------------------------
            try:
                response = subscriber.pull(
                    request={
                        "subscription": subscription_path,
                        "max_messages": max_messages,
                    },
                    timeout=poll_interval,
                )
            except DeadlineExceeded:
                logger.debug("pull() timed out — subscription empty, will retry.")
                continue

            messages = response.received_messages
            if not messages:
                logger.debug("Empty pull response — sleeping %ds.", poll_interval)
                time.sleep(poll_interval)
                continue

            # ---- Decode and accumulate records ----------------------------
            ack_ids: list[str] = []
            for msg in messages:
                ack_ids.append(msg.ack_id)
                attrs = msg.message.attributes
                msg_platform = attrs.get("platform", platform)
                msg_tier = attrs.get("tier", "UNKNOWN")
                msg_division = attrs.get("division", "I")
                
                partition_key = (msg_platform, msg_tier, msg_division)

                if partition_key not in buffers:
                    buffers[partition_key] = ParquetBuffer(
                        platform=msg_platform,
                        compression=pipeline_cfg.parquet_compression,
                        flush_threshold_bytes=pipeline_cfg.parquet_buffer_bytes,
                    )
                
                if msg_platform not in uploaders:
                    uploaders[msg_platform] = GCSUploader(
                        bucket_name=gcs_cfg.bronze_bucket,
                        prefix=gcs_cfg.league_prefix,
                        platform=msg_platform,
                    )

                try:
                    record = json.loads(msg.message.data.decode("utf-8"))
                    buffers[partition_key].add([record])
                    total_records += 1
                except (json.JSONDecodeError, KeyError) as exc:
                    logger.warning("Skipping malformed message: %s", exc)

            last_received_time = time.monotonic()
            
            total_buffer_bytes = sum(buf.size_bytes for buf in buffers.values())
            logger.debug(
                "Received %d messages (total=%d). Combined buffer size: ~%.2f MB.",
                len(messages),
                total_records,
                total_buffer_bytes / 1024 / 1024,
            )

            # ---- Flush if threshold reached for any partition --------------
            for partition_key, buf in list(buffers.items()):
                if buf.should_flush:
                    p, t, d = partition_key
                    _flush_and_upload(buf, uploaders[p], t, d)

            # ---- Acknowledge successfully processed messages ---------------
            if ack_ids:
                subscriber.acknowledge(
                    request={
                        "subscription": subscription_path,
                        "ack_ids": ack_ids,
                    }
                )

    finally:
        # Final flush for any remaining records in all buffers
        for partition_key, buf in buffers.items():
            if not buf.is_empty:
                p, t, d = partition_key
                logger.info(
                    "Final flush for %s/%s/%s: %d remaining records.",
                    p,
                    t,
                    d,
                    buf.record_count,
                )
                _flush_and_upload(buf, uploaders[p], t, d)

        subscriber.close()

    elapsed = time.monotonic() - start_time
    logger.info(
        "parquet_writer complete — platform=%s total_records=%d elapsed=%.1fs",
        platform,
        total_records,
        elapsed,
    )


def _flush_and_upload(
    buffer: ParquetBuffer,
    uploader: GCSUploader,
    tier: str,
    division: str,
) -> None:
    """Flush the buffer to Parquet bytes and upload to GCS.

    Args:
        buffer: The in-RAM accumulation buffer.
        uploader: The GCS uploader instance.
        tier: Current tier for partition path.
        division: Current division for partition path.
    """
    data = buffer.flush()
    if data is not None:
        uploader.upload(data, tier, division)


def main() -> None:
    """Load config from environment and run the writer job."""
    try:
        pipeline_cfg = PipelineConfig.from_env()
        gcs_cfg = GCSConfig.from_env()
    except ValueError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)

    try:
        run(pipeline_cfg, gcs_cfg)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unrecoverable error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
