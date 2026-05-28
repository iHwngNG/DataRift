"""
config/__init__.py
~~~~~~~~~~~~~~~~~~
DataRift configuration package.

Exports all typed configuration dataclasses and the :func:`get_all_config`
convenience factory, which loads every sub-config from environment variables
in a single call.

Typical usage at application startup::

    from config import get_all_config

    cfg = get_all_config()
    cfg.logging.configure_logging()

    # Access individual sub-configs
    bucket = cfg.gcs.bronze_bucket
    api_key = cfg.riot.api_key
"""

from __future__ import annotations

from dataclasses import dataclass

from config.gcs_config import GCSConfig
from config.iceberg_config import IcebergConfig
from config.logging_config import LoggingConfig
from config.pipeline_config import PipelineConfig
from config.riot_config import RiotConfig

__all__ = [
    "GCSConfig",
    "IcebergConfig",
    "LoggingConfig",
    "PipelineConfig",
    "RiotConfig",
    "AppConfig",
    "get_all_config",
]


@dataclass(frozen=True)
class AppConfig:
    """Bundle of all DataRift configuration sub-configs.

    Attributes:
        gcs: Google Cloud Storage settings.
        riot: Riot Games API settings.
        pipeline: Batch ingestion pipeline settings.
        iceberg: Apache Iceberg catalog settings.
        logging: Structured logging settings.
    """

    gcs: GCSConfig
    riot: RiotConfig
    pipeline: PipelineConfig
    iceberg: IcebergConfig
    logging: LoggingConfig


def get_all_config() -> AppConfig:
    """Load all configuration from environment variables and return a bundle.

    This function is the recommended entry point for application code.  It
    reads each sub-config from environment variables and returns a single
    :class:`AppConfig` instance.

    Returns:
        Fully populated :class:`AppConfig` bundle.

    Raises:
        ValueError: If any required environment variable (e.g.
            ``RIOT_API_KEY``) is missing or if any setting is invalid.

    Example::

        import os
        os.environ["RIOT_API_KEY"] = "RGAPI-..."

        from config import get_all_config

        cfg = get_all_config()
        cfg.logging.configure_logging()
        print(cfg.gcs.bronze_bucket)   # "datarift-bronze"
        print(cfg.pipeline.all_segments[:2])  # [("DIAMOND", "I"), ...]
    """
    return AppConfig(
        gcs=GCSConfig.from_env(),
        riot=RiotConfig.from_env(),
        pipeline=PipelineConfig.from_env(),
        iceberg=IcebergConfig.from_env(),
        logging=LoggingConfig.from_env(),
    )
