"""Path builder functions for GCS bucket structure."""

from __future__ import annotations


__all__ = [
    "build_gcs_uri",
    "checkpoint_path",
    "iceberg_table_path",
    "league_path",
    "match_id_path",
    "match_path",
    "matchid_shard_path",
    "puuid_shard_path",
    "tmp_buffer_path",
]


def league_path(region: str, platform: str, tier: str, division: str) -> str:
    """Build path for league data (Job A output)."""
    return f"league/{region}/{platform}/{tier}/{division}/"


def match_id_path(region: str, platform: str, puuid: str) -> str:
    """Build path for match ID lookup data (Job B output)."""
    return f"matchID/{region}/{platform}/{puuid}/"


def match_path(
    region: str,
    platform: str,
    year: int | str,
    month: int | str,
    date: int | str,
) -> str:
    """Build path for match data (Job C output)."""
    return f"match/{region}/{platform}/{year}/{month}/{date}/"


def puuid_shard_path(shard_id: int) -> str:
    """Build path for PUUID shard storage (Job B working state)."""
    return f"workspace/puuid/{shard_id}/"


def matchid_shard_path(shard_id: int) -> str:
    """Build path for match ID shard storage (Job C working state)."""
    return f"workspace/matchid/{shard_id}/"


def tmp_buffer_path(job_type: str, job_id: str, thread_id: int) -> str:
    """Build path for rate-limit buffer files (in-flight data)."""
    return f"workspace/tmp/{job_type}/{job_id}_{thread_id}.parquet"


def checkpoint_path(job_type: str, job_id: str, thread_id: int) -> str:
    """Build path for checkpoint JSON files (resume state)."""
    return f"state/checkpoint/{job_type}/{job_id}_{thread_id}.json"


def iceberg_table_path(table_name: str) -> str:
    """Build path for Iceberg table."""
    return f"iceberg/{table_name}/"


def build_gcs_uri(bucket: str, path: str) -> str:
    """Build a complete GCS URI from bucket and path."""
    clean_bucket = bucket.removeprefix("gs://")
    return f"gs://{clean_bucket}/{path.lstrip('/')}"
