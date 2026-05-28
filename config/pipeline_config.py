"""
pipeline_config.py
~~~~~~~~~~~~~~~~~~
Batch ingestion pipeline execution configuration for DataRift.

Controls how many records are processed per batch, degree of parallelism,
match fetch depth, request timeouts, and which rank tiers/divisions to ingest.
All settings can be overridden at runtime via environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _parse_list(value: str) -> list[str]:
    """Split a comma-separated env var value into a stripped list of strings."""
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class PipelineConfig:
    """Typed configuration for the DataRift ingestion pipeline.

    Attributes:
        batch_size: Number of player PUUIDs to process in a single batch.
        max_workers: Number of threads in the thread pool for parallel fetching.
        match_count_per_player: Number of recent match IDs to fetch per player.
        request_timeout: Maximum seconds to wait for a single batch to complete.
        queue_id: Riot queue ID to filter match history.
            ``420`` = Ranked Solo/Duo, ``440`` = Ranked Flex.
        tiers: List of rank tiers to ingest, e.g. ``["DIAMOND", "PLATINUM"]``.
        divisions: List of rank divisions to ingest, e.g. ``["I", "II", "III", "IV"]``.
    """

    batch_size: int
    max_workers: int
    match_count_per_player: int
    request_timeout: int
    queue_id: int
    tiers: list[str]
    divisions: list[str]

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Instantiate :class:`PipelineConfig` from environment variables.

        Returns:
            A fully populated :class:`PipelineConfig` instance.
        """
        return cls(
            batch_size=int(os.getenv("PIPELINE_BATCH_SIZE", "100")),
            max_workers=int(os.getenv("PIPELINE_MAX_WORKERS", "4")),
            match_count_per_player=int(os.getenv("PIPELINE_MATCH_COUNT", "20")),
            request_timeout=int(os.getenv("PIPELINE_REQUEST_TIMEOUT", "30")),
            queue_id=int(os.getenv("PIPELINE_QUEUE_ID", "420")),
            tiers=_parse_list(os.getenv("PIPELINE_TIERS", "DIAMOND,PLATINUM")),
            divisions=_parse_list(os.getenv("PIPELINE_DIVISIONS", "I,II,III,IV")),
        )

    @property
    def all_segments(self) -> list[tuple[str, str]]:
        """Return the Cartesian product of tiers × divisions as (tier, division) tuples.

        Useful for iterating over all rank segments to ingest.

        Returns:
            List of ``(tier, division)`` tuples, e.g.
            ``[("DIAMOND", "I"), ("DIAMOND", "II"), ...]``.
        """
        return [(tier, division) for tier in self.tiers for division in self.divisions]
