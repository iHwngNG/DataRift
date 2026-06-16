"""Path builder functions for GCS bucket structure."""

from __future__ import annotations


def league_path(region: str, platform: str, tier: str, division: str) -> str:
    """Build path for league data.

    Args:
        region: Geographic region (e.g., 'americas', 'europe', 'asia').
        platform: Platform identifier (e.g., 'riot', 'steam').
        tier: League tier (e.g., 'challenger', 'grandmaster', 'master').
        division: Division within tier (e.g., 'I', 'II', 'III', 'IV').

    Returns:
        GCS path like 'league/{region}/{platform}/{tier}/{division}/'

    """
    return f"league/{region}/{platform}/{tier}/{division}/"


def match_id_path(region: str, platform: str, puuid: str) -> str:
    """Build path for match ID lookup data.

    Args:
        region: Geographic region (e.g., 'americas', 'europe', 'asia').
        platform: Platform identifier (e.g., 'riot', 'steam').
        puuid: Player's unique identifier.

    Returns:
        GCS path like 'matchID/{region}/{platform}/{puuid}/'

    """
    return f"matchID/{region}/{platform}/{puuid}/"


def match_path(
    region: str,
    platform: str,
    year: int | str,
    month: int | str,
    date: int | str,
) -> str:
    """Build path for match data.

    Args:
        region: Geographic region (e.g., 'americas', 'europe', 'asia').
        platform: Platform identifier (e.g., 'riot', 'steam').
        year: Year of the match (e.g., 2024).
        month: Month of the match (1-12).
        date: Day of the match (1-31).

    Returns:
        GCS path like 'match/{region}/{platform}/{year}/{month}/{date}/'

    """
    return f"match/{region}/{platform}/{year}/{month}/{date}/"


def puuid_shard_path(shard_id: int) -> str:
    """Build path for PUUID shard storage.

    Args:
        shard_id: Shard identifier (0-based).

    Returns:
        GCS path like 'workspace/puuid/{shard_id}/'

    """
    return f"workspace/puuid/{shard_id}/"


def matchid_shard_path(shard_id: int) -> str:
    """Build path for match ID shard storage.

    Args:
        shard_id: Shard identifier (0-based).

    Returns:
        GCS path like 'workspace/matchid/{shard_id}/'

    """
    return f"workspace/matchid/{shard_id}/"


def iceberg_table_path(table_name: str) -> str:
    """Build path for Iceberg table.

    Args:
        table_name: Name of the Iceberg table.

    Returns:
        GCS path like 'iceberg/{table_name}/'

    """
    return f"iceberg/{table_name}/"


def build_gcs_uri(bucket: str, path: str) -> str:
    """Build a complete GCS URI from bucket and path.

    Args:
        bucket: GCS bucket name (with or without gs:// prefix).
        path: GCS object path.

    Returns:
        Full GCS URI like 'gs://bucket-name/path/to/object/'

    """
    clean_bucket = bucket.removeprefix("gs://")
    return f"gs://{clean_bucket}/{path.lstrip('/')}"
