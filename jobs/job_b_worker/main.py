"""Job B Worker: Fetch match IDs for PUUIDs in a shard.

This worker processes a shard of PUUID records, fetching match IDs from
the Riot API and buffering results to GCS. It supports:
- Normal execution: process all PUUIDs that need updates
- Resume execution: continue from checkpoint after rate limiting
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml
from google.cloud.storage import Client as GCSClient


# Add src to path for local development
src_path = Path(__file__).resolve().parents[2] / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from datarift.gcs.paths import (  # noqa: E402
    puuid_shard_path,
)
from datarift.parquet.buffer import ParquetBuffer  # noqa: E402
from datarift.parquet.io import (  # noqa: E402
    overwrite_parquet,
    read_parquet_files,
)
from datarift.rate_limit.checkpoint import (  # noqa: E402
    CheckpointData,
    delete as checkpoint_delete,
    exists as checkpoint_exists,
    load as checkpoint_load,
    save as checkpoint_save,
)
from datarift.rate_limit.scheduler import schedule_retry  # noqa: E402
from datarift.rate_limit.tmp_buffer import (  # noqa: E402
    flush as tmp_buffer_flush,
    restore as tmp_buffer_restore,
)
from datarift.workers.puuid_fetcher import create_job_b_worker_fn  # noqa: E402
from datarift.workers.threaded_queue import (  # noqa: E402
    JobContext,
    ThreadedQueueResult,
    run_threaded,
)


# =============================================================================
# Configuration
# =============================================================================

GCS_BUCKET = os.environ.get("GCS_BUCKET", "datarift-lakehouse")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "us-central1")
CLOUD_TASKS_QUEUE = os.environ.get(
    "CLOUD_TASKS_QUEUE", "datarift-rate-limit-retry"
)


@dataclass(frozen=True)
class JobBWorkerConfig:
    """Configuration for Job B Worker."""

    thread_pool_size: int
    buffer_flush_mb: int


# =============================================================================
# Schema
# =============================================================================

# Input: PUUID shard schema (from Distributor)
SHARD_SCHEMA = pa.schema(
    [
        ("puuid", pa.string()),
        ("last_read", pa.string()),  # ISO date string or NULL
        ("region", pa.string()),
        ("platform", pa.string()),
    ]
)


# =============================================================================
# Config Loading
# =============================================================================


def _load_config() -> JobBWorkerConfig:
    """Load configuration from YAML file."""
    conf_dir = Path(__file__).resolve().parents[2] / "conf"
    job_b_path = conf_dir / "job_b.yaml"

    if not job_b_path.exists():
        raise FileNotFoundError(f"Config not found: {job_b_path}")

    with open(job_b_path) as f:
        data = yaml.safe_load(f)

    return JobBWorkerConfig(
        thread_pool_size=data["thread_pool_size"],
        buffer_flush_mb=data["buffer_flush_mb"],
    )


# =============================================================================
# Shard Loading
# =============================================================================


def _load_shard(
    gcs_client: GCSClient,
    shard_id: int,
    bucket: str,
) -> list[dict[str, Any]]:
    """Load PUUID shard from GCS.

    Args:
        gcs_client: Authenticated GCS client.
        shard_id: Shard index (0-3).
        bucket: GCS bucket name.

    Returns:
        List of PUUID records with puuid, last_read, region, platform.

    """
    shard_path = puuid_shard_path(shard_id)
    full_gcs_path = f"gs://{bucket}/{shard_path}data.parquet"

    table = read_parquet_files(gcs_client, full_gcs_path)

    if table.num_rows == 0:
        return []

    records: list[dict[str, Any]] = []
    for i in range(table.num_rows):
        puuid = table.column("puuid")[i].as_py()
        last_read = table.column("last_read")[i].as_py()
        region = table.column("region")[i].as_py()
        platform = table.column("platform")[i].as_py()

        records.append(
            {
                "puuid": puuid,
                "last_read": last_read,
                "region": region,
                "platform": platform,
            }
        )

    return records


def _parse_last_read(last_read: Any) -> date | None:
    """Parse last_read field to date object.

    Args:
        last_read: Can be None (NULL), date object, or date string.

    Returns:
        date object or None if last_read is NULL/None.

    """
    if last_read is None:
        return None
    if isinstance(last_read, date):
        return last_read
    if isinstance(last_read, datetime):
        return last_read.date()
    if isinstance(last_read, str):
        return date.fromisoformat(last_read)
    return None


def _filter_records(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter records: keep only those needing processing.

    - Keep: last_read IS NULL OR last_read < today
    - Skip: last_read == today (already processed today)

    Args:
        records: List of PUUID records.

    Returns:
        Filtered list of records.

    """
    today_utc = date.today()
    filtered: list[dict[str, Any]] = []
    for record in records:
        last_read = record.get("last_read")
        last_read_date = _parse_last_read(last_read)
        if last_read_date is None or last_read_date < today_utc:
            filtered.append(record)
    return filtered


# =============================================================================
# Shard Update (last_read)
# =============================================================================


def _update_shard_last_read(
    gcs_client: GCSClient,
    records: list[dict[str, Any]],
    shard_id: int,
    bucket: str,
) -> None:
    """Update last_read in shard records and write back to GCS.

    Args:
        gcs_client: Authenticated GCS client.
        records: Full list of PUUID records (modified in-place).
        shard_id: Shard index.
        bucket: GCS bucket name.

    """
    # Build updated table
    table = pa.table(
        {
            "puuid": [r["puuid"] for r in records],
            "last_read": [r["last_read"] for r in records],
            "region": [r["region"] for r in records],
            "platform": [r["platform"] for r in records],
        },
        schema=SHARD_SCHEMA,
    )

    shard_path = puuid_shard_path(shard_id)
    gcs_data_path = f"{shard_path}data.parquet"

    overwrite_parquet(table, gcs_client, gcs_data_path, bucket)


# =============================================================================
# Buffer Factory
# =============================================================================


def create_buffer(
    thread_id: int,
    context: JobContext,
    gcs_client: GCSClient,
    gcs_bucket: str,
) -> ParquetBuffer:
    """Create a new streaming ParquetBuffer for a thread.

    Args:
        thread_id: Thread ID.
        context: Job context.
        gcs_client: Authenticated GCS client.
        gcs_bucket: GCS bucket name.

    Returns:
        New streaming ParquetBuffer instance.

    """
    flush_threshold_bytes = 1 * 1024 * 1024  # 1MB
    gcs_path = f"workspace/matchID/{context.shard_id}/{thread_id}/"
    return ParquetBuffer(
        flush_threshold_bytes=flush_threshold_bytes,
        streaming=True,
        gcs_client=gcs_client,
        gcs_bucket=gcs_bucket,
        gcs_path=gcs_path,
        row_group_size=1000,
    )


# =============================================================================
# Checkpoint Functions
# =============================================================================


def save_checkpoint(
    gcs_client: GCSClient,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
    checkpoint_data: dict[str, Any],
) -> str:
    """Save checkpoint to GCS.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.
        job_type: Job type (e.g., "b").
        job_id: Unique job identifier.
        thread_id: Thread ID.
        checkpoint_data: Checkpoint data dict.

    Returns:
        GCS path of saved checkpoint.

    """
    return checkpoint_save(
        gcs_client,
        bucket,
        job_type,
        job_id,
        thread_id,
        checkpoint_data,
    )


def load_checkpoint(
    gcs_client: GCSClient,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
) -> CheckpointData:
    """Load checkpoint from GCS.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.
        job_type: Job type (e.g., "b").
        job_id: Unique job identifier.
        thread_id: Thread ID.

    Returns:
        CheckpointData with restored state.

    """
    return checkpoint_load(
        gcs_client,
        bucket,
        job_type,
        job_id,
        thread_id,
    )


def checkpoint_exists_fn(
    gcs_client: GCSClient,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
) -> bool:
    """Check if checkpoint exists in GCS.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.
        job_type: Job type (e.g., "b").
        job_id: Unique job identifier.
        thread_id: Thread ID.

    Returns:
        True if checkpoint exists.

    """
    return checkpoint_exists(
        gcs_client,
        bucket,
        job_type,
        job_id,
        thread_id,
    )


# =============================================================================
# Scheduler Retry
# =============================================================================


def schedule_retry_task(
    project_id: str,
    location: str,
    job_name: str,
    retry_after: int,
) -> None:
    """Schedule a Cloud Tasks retry.

    Args:
        project_id: GCP project ID.
        location: GCP region.
        job_name: Job name for the retry.
        retry_after: Seconds to wait before retry.

    """
    if project_id and location:
        schedule_retry(
            project_id=project_id,
            location=location,
            job_name=job_name,
            retry_after=retry_after,
            queue=CLOUD_TASKS_QUEUE,
        )
    else:
        print(
            f"[Job B Worker] WARNING: Cannot schedule retry - "
            f"project_id={project_id!r}, location={location!r}"
        )


# =============================================================================
# Tmp Buffer Functions
# =============================================================================


def flush_tmp_buffer(
    gcs_client: GCSClient,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
    table: pa.Table,
) -> str:
    """Flush table to GCS tmp buffer.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.
        job_type: Job type (e.g., "b").
        job_id: Unique job identifier.
        thread_id: Thread ID.
        table: PyArrow Table to flush.

    Returns:
        GCS path of saved tmp buffer.

    """
    return tmp_buffer_flush(
        gcs_client,
        bucket,
        job_type,
        job_id,
        thread_id,
        table,
    )


def restore_tmp_buffer(
    gcs_client: GCSClient,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
) -> pa.Table:
    """Restore table from GCS tmp buffer.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.
        job_type: Job type (e.g., "b").
        job_id: Unique job identifier.
        thread_id: Thread ID.

    Returns:
        Restored PyArrow Table.

    """
    return tmp_buffer_restore(
        gcs_client,
        bucket,
        job_type,
        job_id,
        thread_id,
    )


# =============================================================================
# Resume Cleanup
# =============================================================================


def _cleanup_resume_checkpoints(
    gcs_client: GCSClient,
    bucket: str,
    parent_job_id: str,
    thread_count: int,
) -> None:
    """Delete checkpoints from parent job after successful resume.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.
        parent_job_id: Parent job ID to clean up.
        thread_count: Number of threads to clean up.

    """
    job_type = "b"
    for thread_id in range(thread_count):
        try:
            checkpoint_delete(
                gcs_client, bucket, job_type, parent_job_id, thread_id
            )
        except Exception:
            pass  # Ignore if already deleted

        try:
            from datarift.rate_limit.tmp_buffer import delete as tmp_delete

            tmp_delete(gcs_client, bucket, job_type, parent_job_id, thread_id)
        except Exception:
            pass  # Ignore if already deleted


# =============================================================================
# Main Entry Point
# =============================================================================


def main() -> None:
    """Execute the Job B Worker workflow."""
    # Step 1: Read environment variables
    shard_id = int(os.environ["SHARD_ID"])
    job_id = os.environ["JOB_ID"]
    resume_mode = os.environ.get("RESUME_MODE", "false").lower() == "true"
    parent_job_id = os.environ.get("PARENT_JOB_ID") if resume_mode else None
    bucket = GCS_BUCKET

    print(
        f"[Job B Worker] Starting - Job ID: {job_id}, Shard: {shard_id}, Resume: {resume_mode}"
    )

    # Load configuration
    config = _load_config()
    print(
        f"[Job B Worker] Config: {config.thread_pool_size} threads, "
        f"{config.buffer_flush_mb}MB buffer"
    )

    # Step 2: Load shard file
    gcs_client = GCSClient()
    records = _load_shard(gcs_client, shard_id, bucket)
    print(f"[Job B Worker] Loaded {len(records)} PUUIDs from shard {shard_id}")

    # Step 3: Filter records
    work_items = _filter_records(records)
    print(
        f"[Job B Worker] Filtered to {len(work_items)} PUUIDs needing processing"
    )

    if not work_items:
        print("[Job B Worker] No items to process, exiting")
        return

    # Create job context
    context = JobContext(
        job_id=job_id,
        job_type="b",
        shard_id=shard_id,
    )

    # Create worker function using factory
    worker_fn = create_job_b_worker_fn(gcs_client, bucket)

    # Create buffer factory with GCS params
    def buffer_factory(
        thread_id: int,
        ctx: JobContext,
    ) -> ParquetBuffer:
        return create_buffer(thread_id, ctx, gcs_client, bucket)

    # Step 4: Run threaded queue
    result = run_threaded(
        work_items=work_items,
        context=context,
        thread_count=config.thread_pool_size,
        worker_fn=worker_fn,
        buffer_factory_fn=buffer_factory,
        checkpoint_save_fn=save_checkpoint,
        scheduler_retry_fn=schedule_retry_task,
        gcs_client=gcs_client,
        gcs_bucket=bucket,
        tmp_buffer_flush_fn=flush_tmp_buffer,
        tmp_buffer_restore_fn=restore_tmp_buffer,
        checkpoint_load_fn=load_checkpoint,
        checkpoint_exists_fn=checkpoint_exists,
        resume_mode=resume_mode,
        parent_job_id=parent_job_id,
    )

    # Step 5: Handle result
    if result == ThreadedQueueResult.RATE_LIMITED:
        print("[Job B Worker] Rate limited, retry scheduled")
        # Checkpoints and tmp buffers are already saved by run_threaded
        return
    else:
        print("[Job B Worker] All items processed")

    # Step 6: Finalize
    # Update shard file with new last_read values
    today_str = datetime.now(UTC).strftime("%Y-%m-%d")
    for record in work_items:
        record["last_read"] = today_str

    _update_shard_last_read(gcs_client, records, shard_id, bucket)
    print(f"[Job B Worker] Updated shard {shard_id} with new last_read values")

    # Resume cleanup: delete parent checkpoints after successful completion
    if resume_mode and parent_job_id:
        _cleanup_resume_checkpoints(
            gcs_client, bucket, parent_job_id, config.thread_pool_size
        )
        print(
            f"[Job B Worker] Cleaned up checkpoints from parent job {parent_job_id}"
        )

    print(f"[Job B Worker] Completed - Job ID: {job_id}")


if __name__ == "__main__":
    main()
