"""Job A: User (League) Ingestion.

Fetches ranked ladder entries from Riot League-Entries API and writes to GCS.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Any

import pyarrow as pa
import yaml
from google.cloud.storage import Client as GCSClient


# Add src to path for local development
src_path = Path(__file__).resolve().parents[2] / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from datarift.gcs.paths import league_path  # noqa: E402
from datarift.parquet.buffer import ParquetBuffer  # noqa: E402
from datarift.riot_client.client import (  # noqa: E402
    RateLimitError,
    RiotClient,
)
from datarift.riot_client.regions import platform_to_region  # noqa: E402


# =============================================================================
# Configuration
# =============================================================================

GCS_BUCKET = os.environ.get("GCS_BUCKET", "datarift-lakehouse")


@dataclass(frozen=True)
class JobCombination:
    """Represents a single combination to process."""

    platform: str
    region: str
    tier: str
    division: str | None  # None for apex tiers


@dataclass
class JobConfig:
    """Configuration for Job A."""

    platforms: list[str]
    platform_region: dict[str, str]
    tiers: list[str]
    apex_tiers: list[str]
    divisions: list[str]
    buffer_flush_mb: int
    max_concurrent_requests: int


# =============================================================================
# Schema
# =============================================================================

LEAGUE_SCHEMA = pa.schema(
    [
        ("puuid", pa.string()),
        ("summoner_id", pa.string()),
        ("summoner_name", pa.string()),
        ("tier", pa.string()),
        ("division", pa.string()),
        ("league_points", pa.int32()),
        ("wins", pa.int32()),
        ("losses", pa.int32()),
        ("region", pa.string()),
        ("platform", pa.string()),
        ("_ingested_at", pa.timestamp("us")),
    ]
)


# =============================================================================
# Config Loading
# =============================================================================


def _load_config() -> JobConfig:
    """Load configuration from YAML files."""
    conf_dir = Path(__file__).resolve().parents[2] / "conf"
    job_a_path = conf_dir / "job_a.yaml"

    if not job_a_path.exists():
        raise FileNotFoundError(f"Config not found: {job_a_path}")

    with open(job_a_path) as f:
        data = yaml.safe_load(f)

    return JobConfig(
        platforms=data["platforms"],
        platform_region=data["platform_region"],
        tiers=data["tiers"],
        apex_tiers=data["apex_tiers"],
        divisions=data["divisions"],
        buffer_flush_mb=data["buffer_flush_mb"],
        max_concurrent_requests=data["max_concurrent_requests"],
    )


def _build_combinations(config: JobConfig) -> list[JobCombination]:
    """Build all platform/tier/division combinations to process."""
    combinations: list[JobCombination] = []

    for platform in config.platforms:
        region = platform_to_region(platform)

        # Apex tiers (no division)
        for tier in config.apex_tiers:
            combinations.append(
                JobCombination(
                    platform=platform,
                    region=region,
                    tier=tier,
                    division=None,
                )
            )

        # Regular tiers (with divisions)
        for tier in config.tiers:
            for division in config.divisions:
                combinations.append(
                    JobCombination(
                        platform=platform,
                        region=region,
                        tier=tier,
                        division=division,
                    )
                )

    return combinations


# =============================================================================
# Data Fetching
# =============================================================================


async def _fetch_apex_entries(
    client: RiotClient,
    tier: str,
) -> list[dict[str, Any]]:
    """Fetch entries for apex tiers (Challenger, GrandMaster, Master)."""
    endpoint = f"/lol/league/v4/{tier}leagues/by-queue/RANKED_SOLO_5x5"
    try:
        data: Any = await client.get_json(endpoint)
        entries = data.get("entries", [])
        return list(entries) if entries else []
    except RateLimitError:
        raise
    except Exception:
        return []


async def _fetch_regular_entries(
    client: RiotClient,
    tier: str,
    division: str,
) -> list[dict[str, Any]]:
    """Fetch all entries for regular tiers with pagination."""
    all_entries: list[dict[str, Any]] = []
    page = 1

    while True:
        try:
            endpoint = (
                f"/lol/league/v4/entries/{tier}/{division}"
                f"?queue=RANKED_SOLO_5x5&page={page}"
            )
            entries: Any = await client.get_json(endpoint)

            if not entries:
                break

            all_entries.extend(entries)
            page += 1

        except RateLimitError:
            raise
        except Exception:
            break

    return all_entries


# =============================================================================
# Data Transformation
# =============================================================================


def _transform_entry(
    entry: dict[str, Any],
    platform: str,
    region: str,
    tier: str,
    division: str | None,
) -> dict[str, Any]:
    """Transform a Riot API entry to league schema."""
    return {
        "puuid": entry.get("puuid", ""),
        "summoner_id": entry.get("summonerId", ""),
        "summoner_name": entry.get("summonerName", ""),
        "tier": tier.lower(),
        "division": (division or "all").lower(),
        "league_points": entry.get("leaguePoints", 0),
        "wins": entry.get("wins", 0),
        "losses": entry.get("losses", 0),
        "region": region,
        "platform": platform,
        "_ingested_at": datetime.now(UTC),
    }


# =============================================================================
# GCS Writing
# =============================================================================


def _write_buffer_to_gcs(
    gcs_client: GCSClient,
    buffer: ParquetBuffer,
    job_id: str,
    thread_id: int,
    region: str,
    platform: str,
    tier: str,
    division: str | None,
) -> str | None:
    """Flush buffer to GCS and return the path."""
    if buffer.record_count == 0:
        return None

    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    unique_id = uuid.uuid4().hex[:8]

    division_value = division or "all"
    gcs_path = league_path(region, platform, tier, division_value)
    filename = f"{job_id}_{thread_id}_{timestamp}_{unique_id}.parquet"
    full_gcs_path = f"{gcs_path}{filename}"

    # Create a temporary file for writing
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        table = buffer.flush()
        if isinstance(table, str):
            return table  # Already uploaded in streaming mode
        if table.num_rows == 0:
            return None

        pa.parquet.write_table(table, tmp_path)  # type: ignore[no-untyped-call]

        bucket = gcs_client.bucket(GCS_BUCKET.removeprefix("gs://"))
        blob = bucket.blob(full_gcs_path.lstrip("/"))
        blob.upload_from_filename(tmp_path)

        return f"gs://{GCS_BUCKET.removeprefix('gs://')}/{full_gcs_path.lstrip('/')}"
    finally:
        os.unlink(tmp_path)


# =============================================================================
# Thread Worker
# =============================================================================


def _thread_worker(
    work_queue: Queue[JobCombination],
    job_id: str,
    thread_id: int,
    config: JobConfig,
    results: dict[int, dict[str, Any]],
    results_lock: threading.Lock,
) -> None:
    """Thread worker to process combinations from queue."""
    flush_threshold_bytes = config.buffer_flush_mb * 1024 * 1024

    # Create GCS client in thread
    gcs_client = GCSClient()

    # Buffer for this thread (per-combination)
    buffer: ParquetBuffer | None = None
    current_combination: JobCombination | None = None

    # Event loop for async operations
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        while True:
            # Get next combination from queue
            try:
                combination = work_queue.get_nowait()
            except Empty:
                break

            # If we had a previous combination, flush its buffer first
            if (
                buffer is not None
                and buffer.record_count > 0
                and current_combination is not None
            ):
                combo = current_combination
                _write_buffer_to_gcs(
                    gcs_client,
                    buffer,
                    job_id,
                    thread_id,
                    combo.region,
                    combo.platform,
                    combo.tier,
                    combo.division,
                )

            # Start new buffer for this combination
            current_combination = combination
            buffer = ParquetBuffer(
                flush_threshold_bytes=flush_threshold_bytes,
                streaming=True,
                gcs_client=gcs_client,
                gcs_bucket=GCS_BUCKET,
                gcs_path=league_path(
                    combination.region,
                    combination.platform,
                    combination.tier,
                    combination.division or "all",
                ),
                row_group_size=1000,
            )

            # Fetch and process
            try:
                loop.run_until_complete(
                    _fetch_and_buffer(
                        RiotClient(
                            combination.platform,
                            max_concurrent=config.max_concurrent_requests,
                        ),
                        combination,
                        buffer,
                        flush_threshold_bytes,
                    )
                )
            except RateLimitError:
                print(
                    f"[Thread {thread_id}] Rate limit hit during {combination.platform}/{combination.tier}/{combination.division}"
                )
                raise

            work_queue.task_done()

        # End of job flush - flush remaining buffer
        if (
            buffer is not None
            and buffer.record_count > 0
            and current_combination is not None
        ):
            _write_buffer_to_gcs(
                gcs_client,
                buffer,
                job_id,
                thread_id,
                current_combination.region,
                current_combination.platform,
                current_combination.tier,
                current_combination.division,
            )

    finally:
        loop.close()

        with results_lock:
            results[thread_id] = {"status": "completed"}


async def _fetch_and_buffer(
    client: RiotClient,
    combination: JobCombination,
    buffer: ParquetBuffer,
    flush_threshold_bytes: int,
) -> None:
    """Fetch entries from Riot API and add to buffer."""
    platform = combination.platform
    region = combination.region
    tier = combination.tier
    division = combination.division

    try:
        if division is None:
            entries = await _fetch_apex_entries(client, tier)
        else:
            entries = await _fetch_regular_entries(client, tier, division)

        for entry in entries:
            record = _transform_entry(entry, platform, region, tier, division)
            buffer.add(record)

            # Check if buffer needs flushing based on bytes threshold
            # In streaming mode, the writer handles row-based flushing
            # We check bytes-based flushing manually here
            if buffer.estimated_size_bytes >= flush_threshold_bytes:
                buffer.flush()

    finally:
        await client.close()


# =============================================================================
# Main Entry Point
# =============================================================================


async def main() -> None:
    """Run the Job A main workflow."""
    job_id = os.environ.get("JOB_ID", uuid.uuid4().hex[:8])
    resume_mode = os.environ.get("RESUME_MODE", "false").lower() == "true"

    print(f"[Job A] Starting - Job ID: {job_id}, Resume: {resume_mode}")

    config = _load_config()
    print(
        f"[Job A] Loaded config: {len(config.platforms)} platforms, "
        f"{len(config.tiers)} tiers, {len(config.apex_tiers)} apex tiers"
    )

    combinations = _build_combinations(config)
    print(f"[Job A] Total combinations to process: {len(combinations)}")

    work_queue: Queue[JobCombination] = Queue()
    for combo in combinations:
        work_queue.put(combo)

    results: dict[int, dict[str, Any]] = {}
    results_lock = threading.Lock()

    num_workers = min(config.max_concurrent_requests, len(combinations))
    print(f"[Job A] Starting {num_workers} worker threads")

    threads: list[threading.Thread] = []
    for i in range(num_workers):
        t = threading.Thread(
            target=_thread_worker,
            args=(work_queue, job_id, i, config, results, results_lock),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print(f"[Job A] Completed - Processed {len(combinations)} combinations")


if __name__ == "__main__":
    asyncio.run(main())
