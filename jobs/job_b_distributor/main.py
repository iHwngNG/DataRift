"""Job B-Distributor: Shard PUUIDs and trigger workers."""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml
from google.cloud import pubsub_v1
from google.cloud.storage import Client as GCSClient


# Add src to path for local development
src_path = Path(__file__).resolve().parents[2] / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from datarift.gcs.paths import puuid_shard_path  # noqa: E402
from datarift.hashing.shard import assign_shard  # noqa: E402
from datarift.hashing.string_to_int import to_int  # noqa: E402
from datarift.parquet.io import (  # noqa: E402
    overwrite_parquet,
    read_parquet_files,
)


# =============================================================================
# Configuration
# =============================================================================

GCS_BUCKET = os.environ.get("GCS_BUCKET", "datarift-lakehouse")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "job-b-worker-trigger")


@dataclass(frozen=True)
class JobBConfig:
    """Configuration for Job B."""

    shard_count: int


# =============================================================================
# Schema
# =============================================================================

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


def _load_config() -> JobBConfig:
    """Load configuration from YAML file."""
    conf_dir = Path(__file__).resolve().parents[2] / "conf"
    job_b_path = conf_dir / "job_b.yaml"

    if not job_b_path.exists():
        raise FileNotFoundError(f"Config not found: {job_b_path}")

    with open(job_b_path) as f:
        data = yaml.safe_load(f)

    return JobBConfig(
        shard_count=data["sharding"]["shard_count"],
    )


# =============================================================================
# State Loading
# =============================================================================


def _load_existing_state(
    gcs_client: GCSClient,
    shard_count: int,
    bucket: str,
) -> dict[str, str | None]:
    """Load existing PUUID state from shard files.

    Args:
        gcs_client: Authenticated GCS client.
        shard_count: Number of shards to load.
        bucket: GCS bucket name.

    Returns:
        Dictionary mapping puuid to last_read timestamp (None for NULL).

    """
    existing_state: dict[str, str | None] = {}

    for shard_id in range(shard_count):
        shard_path = puuid_shard_path(shard_id)
        full_gcs_path = f"gs://{bucket}/{shard_path}data.parquet"

        try:
            table = read_parquet_files(gcs_client, full_gcs_path)

            if table.num_rows > 0 and "puuid" in table.column_names:
                puuid_col = table.column("puuid")
                last_read_col = table.column("last_read")

                for i in range(table.num_rows):
                    puuid = puuid_col[i].as_py()
                    last_read = last_read_col[i].as_py()
                    if puuid:
                        existing_state[puuid] = last_read
        except Exception:
            # File doesn't exist or is empty - skip
            pass

    return existing_state


# =============================================================================
# League Data Loading
# =============================================================================


def _load_league_data(gcs_client: GCSClient, bucket: str) -> pa.Table:
    """Load all PUUIDs from league data.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.

    Returns:
        PyArrow Table with league data containing puuid, region, platform, tier.

    """
    league_prefix = f"gs://{bucket}/league/"
    return read_parquet_files(gcs_client, league_prefix)


# =============================================================================
# Deduplication
# =============================================================================


def _deduplicate_puuids(table: pa.Table) -> list[dict[str, Any]]:
    """Deduplicate PUUIDs, preferring highest tier.

    Tiers are ordered: challenger > grandmaster > master > diamond >
    emerald > platinum > gold > silver > bronze > iron.

    Args:
        table: PyArrow Table with league data.

    Returns:
        List of deduplicated records with puuid, region, platform.

    """
    if table.num_rows == 0:
        return []

    # Build deduplication map: puuid -> best record
    seen: dict[str, dict[str, Any]] = {}

    tier_order = {
        "challenger": 0,
        "grandmaster": 1,
        "master": 2,
        "diamond": 3,
        "emerald": 4,
        "platinum": 5,
        "gold": 6,
        "silver": 7,
        "bronze": 8,
        "iron": 9,
    }

    puuid_col = table.column("puuid")
    region_col = table.column("region")
    platform_col = table.column("platform")
    tier_col = table.column("tier")

    for i in range(table.num_rows):
        puuid = puuid_col[i].as_py()
        region = region_col[i].as_py()
        platform = platform_col[i].as_py()
        tier = (
            tier_col[i].as_py() if "tier" in table.column_names else "bronze"
        )

        if not puuid:
            continue

        if puuid not in seen:
            seen[puuid] = {
                "puuid": puuid,
                "region": region,
                "platform": platform,
                "tier_rank": tier_order.get(
                    tier.lower() if tier else "bronze", 999
                ),
            }
        else:
            current_rank = seen[puuid]["tier_rank"]
            new_rank = tier_order.get(tier.lower() if tier else "bronze", 999)
            if new_rank < current_rank:
                seen[puuid] = {
                    "puuid": puuid,
                    "region": region,
                    "platform": platform,
                    "tier_rank": new_rank,
                }

    return [
        {
            "puuid": record["puuid"],
            "region": record["region"],
            "platform": record["platform"],
        }
        for record in seen.values()
    ]


# =============================================================================
# State Merging
# =============================================================================


def _merge_with_state(
    puuids: list[dict[str, Any]],
    existing_state: dict[str, str | None],
) -> list[dict[str, Any]]:
    """Merge PUUIDs with existing state to get last_read values.

    Args:
        puuids: List of PUUID records.
        existing_state: Dictionary mapping puuid to last_read.

    Returns:
        List of PUUID records with last_read field added.

    """
    result: list[dict[str, Any]] = []

    for record in puuids:
        puuid = record["puuid"]
        last_read = existing_state.get(puuid)
        result.append(
            {
                "puuid": puuid,
                "last_read": last_read,
                "region": record["region"],
                "platform": record["platform"],
            }
        )

    return result


# =============================================================================
# Sharding
# =============================================================================


def _shard_puuids(
    puuids: list[dict[str, Any]],
    shard_count: int,
) -> dict[int, list[dict[str, Any]]]:
    """Shard PUUIDs by hash into groups.

    Args:
        puuids: List of PUUID records with metadata.
        shard_count: Number of shards.

    Returns:
        Dictionary mapping shard_id to list of PUUID records.

    """
    shards: dict[int, list[dict[str, Any]]] = {
        i: [] for i in range(shard_count)
    }

    for record in puuids:
        puuid = record["puuid"]
        int_value = to_int(puuid)
        shard_id = assign_shard(int_value, shard_count=shard_count)
        shards[shard_id].append(record)

    return shards


# =============================================================================
# Shard File Writing
# =============================================================================


def _write_shard_files(
    gcs_client: GCSClient,
    shards: dict[int, list[dict[str, Any]]],
    bucket: str,
) -> None:
    """Write shard files to GCS.

    Args:
        gcs_client: Authenticated GCS client.
        shards: Dictionary mapping shard_id to PUUID records.
        bucket: GCS bucket name.

    """
    for shard_id, records in shards.items():
        if not records:
            # Write empty table for empty shards
            table = pa.table(
                {
                    "puuid": [],
                    "last_read": [],
                    "region": [],
                    "platform": [],
                },
                schema=SHARD_SCHEMA,
            )
        else:
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
        print(
            f"[Job B-Distributor] Wrote shard {shard_id}: {table.num_rows} records"
        )


# =============================================================================
# Pub/Sub Publishing
# =============================================================================


def _publish_worker_messages(
    project_id: str,
    topic: str,
    shard_count: int,
) -> None:
    """Publish worker trigger messages to Pub/Sub.

    Args:
        project_id: GCP project ID.
        topic: Pub/Sub topic name.
        shard_count: Number of workers to trigger.

    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic)

    for shard_id in range(shard_count):
        message = json.dumps({"shard_id": shard_id})
        publisher.publish(topic_path, message.encode("utf-8"))
        print(f"[Job B-Distributor] Published trigger for shard {shard_id}")


# =============================================================================
# Main Entry Point
# =============================================================================


def main() -> None:
    """Run the Job B-Distributor workflow."""
    job_id = os.environ.get("JOB_ID", uuid.uuid4().hex[:8])
    project_id = GCP_PROJECT

    print(f"[Job B-Distributor] Starting - Job ID: {job_id}")
    print(
        f"[Job B-Distributor] GCP Project: {project_id}, Topic: {PUBSUB_TOPIC}"
    )

    config = _load_config()
    shard_count = config.shard_count
    print(f"[Job B-Distributor] Loaded config: shard_count={shard_count}")

    gcs_client = GCSClient()

    # Step 1: Load existing state
    print("[Job B-Distributor] Step 1: Loading existing state...")
    existing_state = _load_existing_state(gcs_client, shard_count, GCS_BUCKET)
    print(f"[Job B-Distributor] Loaded {len(existing_state)} existing PUUIDs")

    # Step 2: Load and deduplicate PUUIDs from league data
    print("[Job B-Distributor] Step 2: Loading league data...")
    league_table = _load_league_data(gcs_client, GCS_BUCKET)
    print(f"[Job B-Distributor] Loaded {league_table.num_rows} league records")

    print("[Job B-Distributor] Step 3: Deduplicating PUUIDs...")
    puuids = _deduplicate_puuids(league_table)
    print(f"[Job B-Distributor] Deduplicated to {len(puuids)} unique PUUIDs")

    # Step 4: Merge with state
    print("[Job B-Distributor] Step 4: Merging with state...")
    merged = _merge_with_state(puuids, existing_state)
    print(f"[Job B-Distributor] Merged: {len(merged)} PUUIDs")

    # Step 5: Shard
    print("[Job B-Distributor] Step 5: Sharding...")
    shards = _shard_puuids(merged, shard_count)
    for shard_id, records in shards.items():
        print(f"[Job B-Distributor] Shard {shard_id}: {len(records)} PUUIDs")

    # Step 6: Write shard files
    print("[Job B-Distributor] Step 6: Writing shard files...")
    _write_shard_files(gcs_client, shards, GCS_BUCKET)

    # Step 7: Trigger workers
    if project_id:
        print("[Job B-Distributor] Step 7: Publishing worker triggers...")
        _publish_worker_messages(project_id, PUBSUB_TOPIC, shard_count)
    else:
        print("[Job B-Distributor] Step 7: Skipping Pub/Sub (no GCP_PROJECT)")

    print(f"[Job B-Distributor] Completed - Job ID: {job_id}")


if __name__ == "__main__":
    main()
