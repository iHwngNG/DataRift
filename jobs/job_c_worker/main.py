"""Job C Worker: Fetch match details and ingest to GCS.

This worker processes a shard of match ID records, fetching match details
from the Riot API and buffering results to GCS partitioned by
(region, platform, year, month, date). It supports:
- Normal execution: process all match IDs with is_ingested=0
- Resume execution: continue from checkpoint after rate limiting
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import yaml
from google.cloud.storage import Client as GCSClient


if TYPE_CHECKING:
    from datarift.parquet.buffer import ParquetPartitionBuffer


# Add src to path for local development
src_path = Path(__file__).resolve().parents[2] / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from datarift.gcs.paths import (  # noqa: E402
    matchid_shard_path,
)
from datarift.parquet.buffer import ParquetPartitionBuffer  # noqa: E402
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
from datarift.workers.match_fetcher import (  # noqa: E402
    MatchNotFoundError,
    derive_partition_date,
    fetch_match_detail,
)
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
RIOT_API_KEY = os.environ.get("RIOT_API_KEY", "")


@dataclass(frozen=True)
class JobCWorkerConfig:
    """Configuration for Job C Worker."""

    thread_pool_size: int
    buffer_flush_mb: int


# =============================================================================
# Schema
# =============================================================================

# Input: Match ID shard schema (from Distributor)
SHARD_SCHEMA = pa.schema(
    [
        ("match_id", pa.string()),
        ("is_ingested", pa.int8()),
        ("region", pa.string()),
        ("platform", pa.string()),
        ("puuid", pa.string()),
        ("source_file_path", pa.string()),
    ]
)

# Output: Match detail schema
MATCH_DETAIL_SCHEMA = pa.schema(
    [
        ("match_id", pa.string()),
        ("game_start_timestamp", pa.int64()),
        ("game_duration", pa.int32()),
        ("game_version", pa.string()),
        ("queue_id", pa.int32()),
        ("participants", pa.string()),  # JSON array
        ("teams", pa.string()),  # JSON object
        ("region", pa.string()),
        ("platform", pa.string()),
        ("_ingested_at", pa.string()),
    ]
)

# Partition key type: (region, platform, year, month, date)
PartitionKey = tuple[str, str, int, int, int]

# Type alias for buffer key (matches ParquetPartitionBuffer internals)
_BufferKey = tuple[str, ...]


def _partition_key_to_str(key: PartitionKey) -> str:
    """Convert partition key to string for dict lookup."""
    return f"{key[0]}_{key[1]}_{key[2]}_{key[3]:02d}_{key[4]:02d}"


# =============================================================================
# Config Loading
# =============================================================================


def _load_config() -> JobCWorkerConfig:
    """Load configuration from YAML file."""
    conf_dir = Path(__file__).resolve().parents[2] / "conf"
    job_c_path = conf_dir / "job_c.yaml"

    if not job_c_path.exists():
        raise FileNotFoundError(f"Config not found: {job_c_path}")

    with open(job_c_path) as f:
        data = yaml.safe_load(f)

    return JobCWorkerConfig(
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
    """Load match ID shard from GCS.

    Args:
        gcs_client: Authenticated GCS client.
        shard_id: Shard index (0-3).
        bucket: GCS bucket name.

    Returns:
        List of match ID records with match_id, is_ingested, region, platform,
        puuid, source_file_path.

    """
    shard_path = matchid_shard_path(shard_id)
    full_gcs_path = f"gs://{bucket}/{shard_path}data.parquet"

    table = read_parquet_files(gcs_client, full_gcs_path)

    if table.num_rows == 0:
        return []

    records: list[dict[str, Any]] = []
    for i in range(table.num_rows):
        record = {
            "match_id": table.column("match_id")[i].as_py(),
            "is_ingested": table.column("is_ingested")[i].as_py(),
            "region": table.column("region")[i].as_py(),
            "platform": table.column("platform")[i].as_py(),
            "puuid": table.column("puuid")[i].as_py(),
            "source_file_path": table.column("source_file_path")[i].as_py(),
        }
        records.append(record)

    return records


def _filter_uningested(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter records: keep only those with is_ingested=0.

    Args:
        records: List of match ID records.

    Returns:
        Filtered list of records needing ingestion.

    """
    return [r for r in records if r.get("is_ingested", 1) == 0]


def _build_match_id_map(
    records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build a mapping from match_id to record for later update.

    Args:
        records: List of match ID records.

    Returns:
        Dictionary mapping match_id to its record.

    """
    return {r["match_id"]: r for r in records}


# =============================================================================
# Buffer Factory
# =============================================================================


def create_partition_buffer(
    thread_id: int,
    context: JobContext,
    gcs_client: GCSClient,
    gcs_bucket: str,
    flush_threshold_mb: int,
) -> ParquetPartitionBuffer:
    """Create a new partition buffer for a thread.

    Args:
        thread_id: Thread ID.
        context: Job context.
        gcs_client: Authenticated GCS client.
        gcs_bucket: GCS bucket name.
        flush_threshold_mb: Flush threshold in MB per partition.

    Returns:
        New ParquetPartitionBuffer instance.

    """
    flush_threshold_bytes = flush_threshold_mb * 1024 * 1024
    return ParquetPartitionBuffer(
        flush_threshold_bytes=flush_threshold_bytes,
        streaming=True,
        gcs_client=gcs_client,
        gcs_bucket=gcs_bucket,
        row_group_size=1000,
    )


# =============================================================================
# Match Detail Processing
# =============================================================================


def _transform_match_response(
    match_id: str,
    response: dict[str, Any],
    region: str,
    platform: str,
) -> dict[str, Any]:
    """Transform Riot API response to match detail record.

    Args:
        match_id: Match ID.
        response: Raw API response.
        region: Region identifier.
        platform: Platform identifier.

    Returns:
        Record matching MATCH_DETAIL_SCHEMA.

    """
    metadata = response.get("metadata", {})
    info = response.get("info", {})

    return {
        "match_id": match_id,
        "game_start_timestamp": info.get("gameStartTimestamp", 0),
        "game_duration": info.get("gameDuration", 0),
        "game_version": info.get("gameVersion", ""),
        "queue_id": info.get("queueId", 0),
        "participants": json.dumps(metadata.get("participants", [])),
        "teams": json.dumps(info.get("teams", [])),
        "region": region,
        "platform": platform,
        "_ingested_at": datetime.now(UTC).isoformat(),
    }


async def _fetch_match_detail_async(
    match_id: str,
    region: str,
    api_key: str,
) -> dict[str, Any]:
    """Async wrapper for fetch_match_detail.

    Args:
        match_id: Match ID to fetch.
        region: Region identifier.
        api_key: Riot API key.

    Returns:
        Match detail response.

    Raises:
        RateLimitError: When API returns 429.
        MatchNotFoundError: When match returns 404.

    """
    return await fetch_match_detail(match_id, region, api_key)


def _worker_fn(
    item: dict[str, Any],
    thread_id: int,
    context: JobContext,
    buffer: ParquetPartitionBuffer,
    api_key: str,
    flush_threshold_mb: int,
) -> None:
    """Process a single match ID work item.

    Args:
        item: Work item with match_id, region, platform.
        thread_id: Thread ID for logging.
        context: Job context.
        buffer: Partition buffer for accumulating records.
        api_key: Riot API key.
        flush_threshold_mb: Buffer flush threshold in MB.

    Raises:
        RateLimitError: When API returns 429.
        MatchNotFoundError: When match returns 404.

    """
    match_id = item["match_id"]
    region = item["region"]
    platform = item["platform"]

    try:
        # Fetch match detail
        response = asyncio.get_event_loop().run_until_complete(
            _fetch_match_detail_async(match_id, region, api_key)
        )

        # Transform to output schema
        record = _transform_match_response(
            match_id, response, region, platform
        )

        # Derive partition key from game start timestamp
        game_start_ts = response.get("info", {}).get("gameStartTimestamp", 0)
        year, month, day = derive_partition_date(game_start_ts)
        partition_key: PartitionKey = (region, platform, year, month, day)

        # Add to partition buffer (cast to tuple[str, ...] for type compatibility)
        buffer.add(record, tuple(str(k) for k in partition_key))

        # Check if this partition should flush (>= 32MB)
        # Cast partition key for type compatibility
        buffer_key: _BufferKey = tuple(str(k) for k in partition_key)
        if buffer.should_flush(buffer_key):
            flushed = buffer.flush_partition(buffer_key)
            if flushed:
                print(
                    f"[Job C Worker] Thread {thread_id} flushed partition "
                    f"{partition_key} to {flushed}"
                )

    except MatchNotFoundError:
        print(
            f"[Job C Worker] Thread {thread_id} skipped match not found: {match_id}"
        )
        raise


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
    """Save checkpoint to GCS."""
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
    """Load checkpoint from GCS."""
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
    """Check if checkpoint exists in GCS."""
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
            f"[Job C Worker] WARNING: Cannot schedule retry - "
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
    """Flush table to GCS tmp buffer."""
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
    """Restore table from GCS tmp buffer."""
    return tmp_buffer_restore(
        gcs_client,
        bucket,
        job_type,
        job_id,
        thread_id,
    )


# =============================================================================
# Shard Update (is_ingested)
# =============================================================================


def _update_shard_is_ingested(
    gcs_client: GCSClient,
    records: list[dict[str, Any]],
    shard_id: int,
    bucket: str,
) -> None:
    """Update is_ingested in shard records and write back to GCS.

    Args:
        gcs_client: Authenticated GCS client.
        records: Full list of match ID records (modified in-place).
        shard_id: Shard index.
        bucket: GCS bucket name.

    """
    table = pa.table(
        {
            "match_id": [r["match_id"] for r in records],
            "is_ingested": [r["is_ingested"] for r in records],
            "region": [r["region"] for r in records],
            "platform": [r["platform"] for r in records],
            "puuid": [r["puuid"] for r in records],
            "source_file_path": [r["source_file_path"] for r in records],
        },
        schema=SHARD_SCHEMA,
    )

    shard_path = matchid_shard_path(shard_id)
    gcs_data_path = f"{shard_path}data.parquet"

    overwrite_parquet(table, gcs_client, gcs_data_path, bucket)


# =============================================================================
# Source File Propagation
# =============================================================================


def _propagate_is_ingested_to_sources(
    gcs_client: GCSClient,
    bucket: str,
    updated_match_ids: list[str],
    match_id_to_record: dict[str, dict[str, Any]],
) -> None:
    """Propagate is_ingested=1 updates to source matchID files.

    Groups updated match_ids by source_file_path, then updates each file.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.
        updated_match_ids: List of match_ids that were successfully ingested.
        match_id_to_record: Mapping from match_id to record.

    """
    # Group by source file path
    updates_by_source: dict[str, list[str]] = {}
    for match_id in updated_match_ids:
        record = match_id_to_record.get(match_id)
        if record:
            source_path = record["source_file_path"]
            if source_path not in updates_by_source:
                updates_by_source[source_path] = []
            updates_by_source[source_path].append(match_id)

    # Update each source file
    for source_path, match_ids_to_update in updates_by_source.items():
        match_ids_set = set(match_ids_to_update)

        try:
            # Read source file
            table = read_parquet_files(gcs_client, source_path)

            if table.num_rows == 0:
                continue

            # Update is_ingested for matching match_ids
            match_id_col = table.column("match_id")
            is_ingested_col = table.column("is_ingested")

            new_is_ingested = []
            for i in range(table.num_rows):
                mid = match_id_col[i].as_py()
                if mid in match_ids_set:
                    new_is_ingested.append(1)
                else:
                    new_is_ingested.append(is_ingested_col[i].as_py())

            # Build updated table
            columns = {name: table.column(name) for name in table.column_names}
            columns["is_ingested"] = pa.array(new_is_ingested)
            updated_table = pa.table(columns)

            # Overwrite source file (extract GCS path from gs://bucket/path)
            gcs_path = source_path.replace(f"gs://{bucket}/", "")
            overwrite_parquet(updated_table, gcs_client, gcs_path, bucket)

            print(
                f"[Job C Worker] Updated {len(match_ids_to_update)} records in "
                f"{gcs_path}"
            )

        except Exception as e:
            print(
                f"[Job C Worker] WARNING: Failed to update source file "
                f"{source_path}: {e}"
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
    job_type = "c"
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
# Threaded Worker Wrapper
# =============================================================================


class _WorkerContext:
    """Context for threaded worker function."""

    def __init__(
        self,
        api_key: str,
        flush_threshold_mb: int,
    ) -> None:
        self.api_key = api_key
        self.flush_threshold_mb = flush_threshold_mb


def _threaded_worker(
    item: dict[str, Any],
    thread_id: int,
    context: JobContext,
    buffer: ParquetPartitionBuffer,
) -> None:
    """Thread worker function passed to run_threaded.

    Args:
        item: Work item with match_id, region, platform.
        thread_id: Thread ID.
        context: Job context.
        buffer: Partition buffer.

    Raises:
        RateLimitError: When API returns 429.
        MatchNotFoundError: When match returns 404.

    """
    worker_ctx = _threaded_worker.worker_context  # type: ignore[attr-defined]
    _worker_fn(
        item,
        thread_id,
        context,
        buffer,
        worker_ctx.api_key,
        worker_ctx.flush_threshold_mb,
    )


_threaded_worker.worker_context = None  # type: ignore[attr-defined]


def _buffer_factory(
    thread_id: int,
    ctx: JobContext,
    gcs_client: GCSClient,
    gcs_bucket: str,
    flush_threshold_mb: int,
) -> ParquetPartitionBuffer:
    """Create partition buffer for a thread."""
    return create_partition_buffer(
        thread_id, ctx, gcs_client, gcs_bucket, flush_threshold_mb
    )


# =============================================================================
# Main Entry Point
# =============================================================================


def main() -> None:
    """Execute the Job C Worker workflow."""
    # Step 1: Read environment variables
    shard_id = int(os.environ["SHARD_ID"])
    job_id = os.environ["JOB_ID"]
    resume_mode = os.environ.get("RESUME_MODE", "false").lower() == "true"
    parent_job_id = os.environ.get("PARENT_JOB_ID") if resume_mode else None
    bucket = GCS_BUCKET
    api_key = RIOT_API_KEY

    if not api_key:
        print("[Job C Worker] ERROR: RIOT_API_KEY not set")
        return

    print(
        f"[Job C Worker] Starting - Job ID: {job_id}, Shard: {shard_id}, "
        f"Resume: {resume_mode}"
    )

    # Load configuration
    config = _load_config()
    print(
        f"[Job C Worker] Config: {config.thread_pool_size} threads, "
        f"{config.buffer_flush_mb}MB buffer"
    )

    # Step 2: Load shard file
    gcs_client = GCSClient()
    records = _load_shard(gcs_client, shard_id, bucket)
    print(
        f"[Job C Worker] Loaded {len(records)} match IDs from shard {shard_id}"
    )

    # Build match_id -> record mapping for later updates
    match_id_to_record = _build_match_id_map(records)

    # Step 3: Filter records with is_ingested=0
    work_items = _filter_uningested(records)
    print(
        f"[Job C Worker] Filtered to {len(work_items)} match IDs needing ingestion"
    )

    if not work_items:
        print("[Job C Worker] No items to process, exiting")
        return

    # Track which match_ids were successfully ingested
    ingested_match_ids: set[str] = set()

    # Create job context
    context = JobContext(
        job_id=job_id,
        job_type="c",
        shard_id=shard_id,
    )

    # Set worker context
    worker_ctx = _WorkerContext(
        api_key=api_key,
        flush_threshold_mb=config.buffer_flush_mb,
    )
    _threaded_worker.worker_context = worker_ctx  # type: ignore[attr-defined]

    # Create buffer factory
    def buffer_factory(
        thread_id: int,
        ctx: JobContext,
    ) -> ParquetPartitionBuffer:
        return create_partition_buffer(
            thread_id, ctx, gcs_client, bucket, config.buffer_flush_mb
        )

    # Step 4: Run threaded queue
    result = run_threaded(
        work_items=work_items,
        context=context,
        thread_count=config.thread_pool_size,
        worker_fn=_threaded_worker,  # type: ignore[arg-type]
        buffer_factory_fn=buffer_factory,  # type: ignore[arg-type]
        checkpoint_save_fn=save_checkpoint,
        scheduler_retry_fn=schedule_retry_task,
        gcs_client=gcs_client,
        gcs_bucket=bucket,
        tmp_buffer_flush_fn=flush_tmp_buffer,
        tmp_buffer_restore_fn=restore_tmp_buffer,
        checkpoint_load_fn=load_checkpoint,
        checkpoint_exists_fn=checkpoint_exists_fn,
        resume_mode=resume_mode,
        parent_job_id=parent_job_id,
    )

    # Track ingested match_ids (only in COMPLETED case)
    if result == ThreadedQueueResult.COMPLETED:
        for item in work_items:
            ingested_match_ids.add(item["match_id"])

    # Step 5: Handle result
    if result == ThreadedQueueResult.RATE_LIMITED:
        print("[Job C Worker] Rate limited, retry scheduled")
        # Checkpoints and tmp buffers are already saved by run_threaded
        return
    else:
        print("[Job C Worker] All items processed")

    # Step 6: Finalize
    # Update shard file with is_ingested=1 for processed items
    ingested_set = set(ingested_match_ids)
    for record in records:
        if record["match_id"] in ingested_set:
            record["is_ingested"] = 1

    _update_shard_is_ingested(gcs_client, records, shard_id, bucket)
    print(
        f"[Job C Worker] Updated shard {shard_id} with is_ingested=1 "
        f"for {len(ingested_match_ids)} matches"
    )

    # Propagate is_ingested=1 to source matchID files
    if ingested_match_ids:
        _propagate_is_ingested_to_sources(
            gcs_client,
            bucket,
            list(ingested_match_ids),
            match_id_to_record,
        )

    # Resume cleanup: delete parent checkpoints after successful completion
    if resume_mode and parent_job_id:
        _cleanup_resume_checkpoints(
            gcs_client, bucket, parent_job_id, config.thread_pool_size
        )
        print(
            f"[Job C Worker] Cleaned up checkpoints from parent job {parent_job_id}"
        )

    print(f"[Job C Worker] Completed - Job ID: {job_id}")


if __name__ == "__main__":
    main()
