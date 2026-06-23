"""PUUID-to-match-IDs fetcher for Job B Worker.

Fetches match IDs from Riot API for each PUUID, with support for:
- Case A: First run (last_read IS NULL) - fetch all matches up to 1000
- Case B: Incremental run (last_read < today) - fetch new matches with dedup
- Skip: last_read == today - no processing needed

The worker function is designed to be used with the threaded_queue.run_threaded()
pattern for parallel processing with rate-limit handling.

Usage with threaded_queue:
```python
from datarift.workers.threaded_queue import run_threaded, JobContext
from datarift.workers.puuid_fetcher import create_job_b_worker_fn
from datarift.parquet.buffer import ParquetBuffer

# Create worker with GCS configuration
worker_fn = create_job_b_worker_fn(gcs_client, gcs_bucket)

# Create buffer factory
def buffer_factory(thread_id: int, context: JobContext) -> ParquetBuffer:
    return ParquetBuffer(
        flush_threshold_bytes=1024 * 1024,  # 1MB
        streaming=True,
        gcs_client=gcs_client,
        gcs_bucket=gcs_bucket,
        gcs_path=f"workspace/matchID/{context.shard_id}/{thread_id}/",
    )

# Run the threaded queue
result = run_threaded(
    work_items=work_items,
    context=JobContext(job_id="job-b-123", job_type="b", shard_id=0),
    thread_count=8,
    worker_fn=worker_fn,
    buffer_factory_fn=buffer_factory,
    # ... other required parameters
)
```
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from datarift.gcs.paths import match_id_path
from datarift.parquet.io import read_parquet_files
from datarift.riot_client.client import RiotClient


if TYPE_CHECKING:
    from datarift.parquet.buffer import ParquetBuffer
    from datarift.workers.threaded_queue import JobContext


__all__ = [
    "create_job_b_worker_fn",
    "fetch_match_ids_for_puuid",
    "job_b_worker_fn",
]

# API configuration
PAGINATION_COUNT = 100
MAX_MATCH_IDS_PER_PUUID = 1000


def _is_null_or_na(value: Any) -> bool:
    """Check if a value is None (NULL in PyArrow) or NaT."""
    if value is None:
        return True
    if isinstance(value, float) and value != value:  # NaN check
        return True
    return False


def _parse_last_read(last_read: Any) -> date | None:
    """Parse last_read field to date object.

    Args:
        last_read: Can be None (NULL), date object, or date string.

    Returns:
        date object or None if last_read is NULL/None.

    """
    if _is_null_or_na(last_read):
        return None
    if isinstance(last_read, date):
        return last_read
    if isinstance(last_read, datetime):
        return last_read.date()
    if isinstance(last_read, str):
        return date.fromisoformat(last_read)
    if isinstance(last_read, int):
        # Unix timestamp in seconds
        return datetime.fromtimestamp(last_read, tz=UTC).date()
    # PyArrow stores dates as integer days since epoch
    if hasattr(last_read, "value") and isinstance(last_read.value, int):
        return date(1970, 1, 1).fromordinal(last_read.value)
    return None


async def fetch_match_ids_for_puuid(
    client: RiotClient,
    item: dict[str, Any],
    dedup_set: set[str],
) -> list[str]:
    """Fetch match IDs for a single PUUID from Riot API.

    Implements two cases:
    - Case A (first run): last_read IS NULL - fetch all matches up to 1000
    - Case B (incremental): last_read < today - fetch new matches with dedup
    - Skip: last_read == today - return empty list

    Args:
        client: Async RiotClient instance.
        item: Dict with puuid, last_read, region, platform.
        dedup_set: Set of existing match_ids already in GCS for this puuid.
            Used for early-stop in Case B.

    Returns:
        List of new match_ids (not already in dedup_set).

    Raises:
        RateLimitError: When API returns 429, propagated to caller for handling.

    """
    puuid: str = item["puuid"]
    last_read: Any = item.get("last_read")

    # Parse last_read to date
    last_read_date = _parse_last_read(last_read)
    today_utc = date.today()

    # Case: Skip - last_read == today
    if last_read_date is not None and last_read_date >= today_utc:
        return []

    # Build pagination parameters
    start = 0
    count = PAGINATION_COUNT
    collected: list[str] = []

    if last_read_date is None:
        # Case A: First run (last_read IS NULL)
        # No startTime filter - fetch all matches from the beginning
        while True:
            response = await client.get_json(
                f"/lol/match/v5/matches/by-puuid/{puuid}/ids",
                params={
                    "queue": 420,
                    "start": start,
                    "count": count,
                },
            )

            if not response:
                break

            collected.extend(response)

            if len(collected) >= MAX_MATCH_IDS_PER_PUUID:
                collected = collected[:MAX_MATCH_IDS_PER_PUUID]
                break

            start += PAGINATION_COUNT
    else:
        # Case B: Incremental (last_read < today)
        # Use startTime to only fetch matches since last_read
        start_time = int(
            datetime.combine(last_read_date, datetime.min.time())
            .replace(tzinfo=UTC)
            .timestamp()
        )

        while True:
            response = await client.get_json(
                f"/lol/match/v5/matches/by-puuid/{puuid}/ids",
                params={
                    "queue": 420,
                    "start": start,
                    "count": count,
                    "startTime": start_time,
                },
            )

            if not response:
                break

            # Check each match_id against dedup set for early stop
            for match_id in response:
                if match_id in dedup_set:
                    # Early stop: we've reached matches we've already seen
                    return collected
                collected.append(match_id)
                dedup_set.add(match_id)

            if len(collected) >= MAX_MATCH_IDS_PER_PUUID:
                collected = collected[:MAX_MATCH_IDS_PER_PUUID]
                break

            start += PAGINATION_COUNT

    return collected


def _load_existing_match_ids(
    gcs_client: Any,
    bucket: str,
    region: str,
    platform: str,
    puuid: str,
) -> set[str]:
    """Load existing match IDs from GCS for deduplication.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.
        region: Region identifier.
        platform: Platform identifier.
        puuid: Player UUID.

    Returns:
        Set of existing match_ids, empty set if directory doesn't exist.

    """
    prefix = match_id_path(region, platform, puuid)
    # Normalize bucket format
    if bucket.startswith("gs://"):
        clean_bucket = bucket.removeprefix("gs://").rstrip("/")
        gcs_prefix = f"gs://{clean_bucket}/{prefix}"
    else:
        gcs_prefix = f"gs://{bucket}/{prefix}"

    try:
        table = read_parquet_files(gcs_client, gcs_prefix)
        if table.num_rows == 0:
            return set()

        if "match_id" in table.column_names:
            return set(table.column("match_id").to_pylist())
        return set()
    except Exception:
        # Directory doesn't exist or other GCS error
        return set()


def job_b_worker_fn(
    item: dict[str, Any],
    thread_id: int,
    context: JobContext,
    buffer: ParquetBuffer,
    gcs_client: Any,
    gcs_bucket: str,
) -> None:
    """Worker function for processing a single PUUID.

    This worker function fetches match IDs from Riot API and buffers them
    for Parquet output. It is designed to be used with threaded_queue.run_threaded()
    via a closure created by create_job_b_worker_fn().

    Args:
        item: Dict with puuid, last_read, region, platform.
        thread_id: Thread identifier for logging/debugging.
        context: JobContext with job_id, job_type, shard_id.
        buffer: ParquetBuffer for accumulating output records.
        gcs_client: Authenticated GCS client.
        gcs_bucket: GCS bucket name.

    Raises:
        RateLimitError: When API returns 429, propagated to caller for handling.

    """
    puuid = item["puuid"]
    last_read = item.get("last_read")
    region = item["region"]
    platform = item["platform"]

    # Parse last_read
    last_read_date = _parse_last_read(last_read)
    today_utc = date.today()

    # Skip: last_read == today
    if last_read_date is not None and last_read_date >= today_utc:
        return

    # Load existing match IDs for Case B deduplication
    dedup_set: set[str] = set()
    if last_read_date is not None:
        # Case B: Load existing match_ids for early-stop
        dedup_set = _load_existing_match_ids(
            gcs_client=gcs_client,
            bucket=gcs_bucket,
            region=region,
            platform=platform,
            puuid=puuid,
        )

    # Create async RiotClient for this request
    # Platform is used as the API host (e.g., vn2.api.riotgames.com)
    client = RiotClient(platform=platform)

    try:
        # Run async fetch synchronously in thread context
        loop = asyncio.get_event_loop()
        match_ids = loop.run_until_complete(
            fetch_match_ids_for_puuid(client, item, dedup_set)
        )
    finally:
        # Clean up client
        loop.run_until_complete(client.close())

    if not match_ids:
        return

    # Buffer the match IDs
    now = datetime.now(tz=UTC)

    for match_id in match_ids:
        record: dict[str, Any] = {
            "match_id": match_id,
            "puuid": puuid,
            "is_ingested": 0,
            "region": region,
            "platform": platform,
            "_ingested_at": now,
        }
        buffer.add(record)

    # Note: Buffer flushes automatically when threshold is reached (handled by caller)
    # The caller (threaded_queue) handles final flush on job completion


def create_job_b_worker_fn(
    gcs_client: Any,
    gcs_bucket: str,
) -> Callable[[Any, int, JobContext, ParquetBuffer], None]:
    """Create a job_b_worker_fn closure with GCS configuration.

    This factory function creates a worker function bound to the GCS client
    and bucket, suitable for use with threaded_queue.run_threaded().

    Args:
        gcs_client: Authenticated GCS client.
        gcs_bucket: GCS bucket name.

    Returns:
        Worker function with signature:
        (item, thread_id, context, buffer) -> None

    Example:
        ```python
        worker_fn = create_job_b_worker_fn(gcs_client, "my-bucket")
        result = run_threaded(
            work_items=work_items,
            context=context,
            thread_count=8,
            worker_fn=worker_fn,
            # ...
        )
        ```

    """

    # Use a wrapper to capture GCS parameters
    def worker_wrapper(
        item: dict[str, Any],
        thread_id: int,
        context: JobContext,
        buffer: ParquetBuffer,
    ) -> None:
        return job_b_worker_fn(
            item=item,
            thread_id=thread_id,
            context=context,
            buffer=buffer,
            gcs_client=gcs_client,
            gcs_bucket=gcs_bucket,
        )

    return worker_wrapper
