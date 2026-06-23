"""Job C-Distributor: Shard match IDs and trigger workers.

Reads all match IDs from ./matchID/**, shards them by hash of match_id,
writes shard files, and triggers 4 Job C Workers via Pub/Sub.
"""

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

from datarift.gcs.paths import matchid_shard_path  # noqa: E402
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
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "job-c-worker-trigger")


@dataclass(frozen=True)
class JobCConfig:
    """Configuration for Job C."""

    shard_count: int


# =============================================================================
# Schema
# =============================================================================

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


# =============================================================================
# Config Loading
# =============================================================================


def _load_config() -> JobCConfig:
    """Load configuration from YAML file."""
    conf_dir = Path(__file__).resolve().parents[2] / "conf"
    job_c_path = conf_dir / "job_c.yaml"

    if not job_c_path.exists():
        raise FileNotFoundError(f"Config not found: {job_c_path}")

    with open(job_c_path) as f:
        data = yaml.safe_load(f)

    return JobCConfig(
        shard_count=data["sharding"]["shard_count"],
    )


# =============================================================================
# Match ID Data Loading (per-file to preserve source_file_path context)
# =============================================================================


def _load_match_id_records(
    gcs_client: GCSClient,
    bucket: str,
) -> list[dict[str, Any]]:
    """Load match ID records from all Parquet files in matchID/.

    Reads each file individually to preserve the source_file_path context
    needed for updating is_ingested back to source files later.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name.

    Returns:
        List of records with match_id, is_ingested, region, platform, puuid,
        source_file_path.

    """
    match_id_prefix = f"gs://{bucket}/matchID/"
    prefix_strip = match_id_prefix.rstrip("/")

    # List all parquet files under matchID/
    prefix = "matchID/"
    bucket_obj = gcs_client.bucket(bucket)
    blobs = list(bucket_obj.list_blobs(prefix=prefix))

    parquet_files = [blob for blob in blobs if blob.name.endswith(".parquet")]

    if not parquet_files:
        return []

    records: list[dict[str, Any]] = []

    for blob in parquet_files:
        source_file_path = f"gs://{bucket}/{blob.name}"

        try:
            # Read each file individually
            table = read_parquet_files(gcs_client, source_file_path)

            if table.num_rows == 0:
                continue

            # Extract columns if they exist
            match_id_col = table.column("match_id") if "match_id" in table.column_names else None
            is_ingested_col = table.column("is_ingested") if "is_ingested" in table.column_names else None
            region_col = table.column("region") if "region" in table.column_names else None
            platform_col = table.column("platform") if "platform" in table.column_names else None
            puuid_col = table.column("puuid") if "puuid" in table.column_names else None

            for i in range(table.num_rows):
                record: dict[str, Any] = {
                    "match_id": match_id_col[i].as_py() if match_id_col else None,
                    "is_ingested": is_ingested_col[i].as_py() if is_ingested_col else 0,
                    "region": region_col[i].as_py() if region_col else None,
                    "platform": platform_col[i].as_py() if platform_col else None,
                    "puuid": puuid_col[i].as_py() if puuid_col else None,
                    "source_file_path": source_file_path,
                }

                # Only include if we have a valid match_id
                if record["match_id"]:
                    records.append(record)

        except Exception as e:
            print(f"[Job C-Distributor] Warning: Failed to read {source_file_path}: {e}")
            continue

    return records


# =============================================================================
# Sharding
# =============================================================================


def _shard_match_ids(
    records: list[dict[str, Any]],
    shard_count: int,
) -> dict[int, list[dict[str, Any]]]:
    """Shard match ID records by hash of match_id.

    Args:
        records: List of match ID records.
        shard_count: Number of shards.

    Returns:
        Dictionary mapping shard_id to list of records.

    """
    shards: dict[int, list[dict[str, Any]]] = {
        i: [] for i in range(shard_count)
    }

    for record in records:
        match_id = record["match_id"]
        match_id_int = to_int(match_id)
        shard_id = assign_shard(match_id_int, shard_count=shard_count)
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
        shards: Dictionary mapping shard_id to match ID records.
        bucket: GCS bucket name.

    """
    for shard_id, records in shards.items():
        if not records:
            # Write empty table for empty shards
            table = pa.table(
                {
                    "match_id": [],
                    "is_ingested": [],
                    "region": [],
                    "platform": [],
                    "puuid": [],
                    "source_file_path": [],
                },
                schema=SHARD_SCHEMA,
            )
        else:
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
        print(
            f"[Job C-Distributor] Wrote shard {shard_id}: {table.num_rows} records"
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
        print(f"[Job C-Distributor] Published trigger for shard {shard_id}")


# =============================================================================
# Main Entry Point
# =============================================================================


def main() -> None:
    """Run the Job C-Distributor workflow."""
    job_id = os.environ.get("JOB_ID", uuid.uuid4().hex[:8])
    project_id = GCP_PROJECT

    print(f"[Job C-Distributor] Starting - Job ID: {job_id}")
    print(
        f"[Job C-Distributor] GCP Project: {project_id}, Topic: {PUBSUB_TOPIC}"
    )

    config = _load_config()
    shard_count = config.shard_count
    print(f"[Job C-Distributor] Loaded config: shard_count={shard_count}")

    gcs_client = GCSClient()

    # Step 1: Load match ID data per file
    print("[Job C-Distributor] Step 1: Loading match ID data from ./matchID/**")
    records = _load_match_id_records(gcs_client, GCS_BUCKET)
    print(f"[Job C-Distributor] Loaded {len(records)} match ID records")

    if not records:
        print("[Job C-Distributor] No match IDs found, skipping sharding")
        return

    # Step 2: Shard by hash of match_id
    print("[Job C-Distributor] Step 2: Sharding by match_id hash...")
    shards = _shard_match_ids(records, shard_count)
    for shard_id, shard_records in shards.items():
        print(f"[Job C-Distributor] Shard {shard_id}: {len(shard_records)} records")

    # Step 3: Write shard files
    print("[Job C-Distributor] Step 3: Writing shard files to workspace/matchid/")
    _write_shard_files(gcs_client, shards, GCS_BUCKET)

    # Step 4: Trigger workers
    if project_id:
        print("[Job C-Distributor] Step 4: Publishing worker triggers...")
        _publish_worker_messages(project_id, PUBSUB_TOPIC, shard_count)
    else:
        print("[Job C-Distributor] Step 4: Skipping Pub/Sub (no GCP_PROJECT)")

    print(f"[Job C-Distributor] Completed - Job ID: {job_id}")


if __name__ == "__main__":
    main()
