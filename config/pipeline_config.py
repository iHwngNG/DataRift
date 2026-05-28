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


@dataclass(frozen=True)
class PipelineConfig:
    """Typed configuration for the DataRift ingestion pipeline.

    Attributes:
        batch_size: Number of player PUUIDs to process in a single batch.
        max_workers: Number of threads in the thread pool for parallel fetching.
        match_count_per_player: Number of recent match IDs to fetch per player.
        request_timeout: Maximum seconds to wait for a single batch to complete.
        parquet_buffer_mb: In-RAM buffer flush threshold in mebibytes.
        max_runtime_seconds: Wall-clock time budget; the job exits gracefully when this is exceeded.
        parquet_compression: Parquet codec passed to PyArrow.
    """

    batch_size: int
    max_workers: int
    match_count_per_player: int
    request_timeout: int
    parquet_buffer_mb: int
    max_runtime_seconds: int
    parquet_compression: str
    max_pages: int | None

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Instantiate :class:`PipelineConfig` from environment variables.

        Returns:
            A fully populated :class:`PipelineConfig` instance.
        """
        max_pages_env = os.getenv("PIPELINE_MAX_PAGES", "")
        max_pages = int(max_pages_env) if max_pages_env else None

        return cls(
            batch_size=int(os.getenv("PIPELINE_BATCH_SIZE", "100")),
            max_workers=int(os.getenv("PIPELINE_MAX_WORKERS", "4")),
            match_count_per_player=int(os.getenv("PIPELINE_MATCH_COUNT", "20")),
            request_timeout=int(os.getenv("PIPELINE_REQUEST_TIMEOUT", "30")),
            parquet_buffer_mb=int(os.getenv("PARQUET_BUFFER_MB", "128")),
            max_runtime_seconds=int(os.getenv("MAX_RUNTIME_SECONDS", "75600")),
            parquet_compression=os.getenv("PARQUET_COMPRESSION", "snappy"),
            max_pages=max_pages,
        )

    @property
    def parquet_buffer_bytes(self) -> int:
        """Flush threshold converted to bytes."""
        return self.parquet_buffer_mb * 1024 * 1024
