"""
gcs_config.py
~~~~~~~~~~~~~
Google Cloud Storage configuration for the DataRift pipeline.

Controls bucket names, GCS object path prefixes, and upload chunk size.
All settings can be overridden at runtime via environment variables.

GCS path structure (transactional data):
    {prefix}/{region}/{platform}/{year}/{month}/{date}/

GCS path structure (static data):
    {static_prefix}/{data_type}/
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Default chunk size: 8 MiB (GCS resumable upload recommended minimum)
_DEFAULT_CHUNK_BYTES: int = 8 * 1024 * 1024


@dataclass(frozen=True)
class GCSConfig:
    """Typed configuration for Google Cloud Storage.

    Attributes:
        bronze_bucket: GCS bucket for raw JSON ingestion (Bronze layer).
        silver_bucket: GCS bucket for Iceberg Parquet files (Silver layer).
        league_prefix: Object prefix for league data paths.
        match_prefix: Object prefix for match data paths.
        static_prefix: Object prefix root for static data (champion, runes, items).
        upload_chunk_bytes: Size in bytes of each resumable upload chunk.
    """

    bronze_bucket: str
    silver_bucket: str
    league_prefix: str
    match_prefix: str
    static_prefix: str
    upload_chunk_bytes: int

    @classmethod
    def from_env(cls) -> "GCSConfig":
        """Instantiate :class:`GCSConfig` from environment variables.

        Returns:
            A fully populated :class:`GCSConfig` instance.
        """
        return cls(
            bronze_bucket=os.getenv("GCS_BRONZE_BUCKET", "datarift-bronze"),
            silver_bucket=os.getenv("GCS_SILVER_BUCKET", "datarift-silver"),
            league_prefix=os.getenv("GCS_LEAGUE_PREFIX", "lol/league"),
            match_prefix=os.getenv("GCS_MATCH_PREFIX", "lol/match"),
            static_prefix=os.getenv("GCS_STATIC_PREFIX", "lol"),
            upload_chunk_bytes=int(
                os.getenv("GCS_CHUNK_BYTES", str(_DEFAULT_CHUNK_BYTES))
            ),
        )

    def build_match_prefix(
        self,
        region: str,
        platform: str,
        year: int,
        month: int,
        day: int,
    ) -> str:
        """Build a fully-qualified GCS object prefix for match data.

        The prefix is based on the **creation date** of the resource, not the
        ingestion date.

        Args:
            region: Riot regional routing value, e.g. ``"asia"``.
            platform: Riot platform value, e.g. ``"kr"``.
            year: 4-digit year of the resource creation date.
            month: Month (1–12) of the resource creation date.
            day: Day (1–31) of the resource creation date.

        Returns:
            GCS prefix string, e.g.
            ``"lol/match/asia/kr/2024/05/15/"``.
        """
        return f"{self.match_prefix}/{region}/{platform}/{year}/{month:02d}/{day:02d}/"

    def build_league_prefix(
        self, tier: str, rank: str = None, league_points: int = None
    ) -> str:
        """Build a fully-qualified GCS object prefix for league data."""
        if tier.upper() in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
            return f"{self.league_prefix}/{tier.upper()}/{league_points}/"
        return f"{self.league_prefix}/{tier.upper()}/{rank.upper()}/"

    def build_static_prefix(self, data_type: str) -> str:
        """Build a GCS object prefix for static (Data Dragon) data.

        Args:
            data_type: One of ``"champion"``, ``"runes"``, ``"items"``.

        Returns:
            GCS prefix string, e.g. ``"lol/champion/"``.
        """
        return f"{self.static_prefix}/{data_type}/"
