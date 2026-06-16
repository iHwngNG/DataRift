"""Config loader module.

Loads and validates YAML configurations with Pydantic models.
Merges base.yaml with job-specific configs.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import BaseModel, Field


if TYPE_CHECKING:
    pass


class GCSConfig(BaseModel):
    """GCS configuration."""

    bucket: str
    project_id: str
    region: str


class PathsConfig(BaseModel):
    """GCS folder structure configuration."""

    league: str = "league/"
    match_id: str = "matchID/"
    match: str = "match/"
    workspace_puuid: str = "workspace/puuid/"
    workspace_matchid: str = "workspace/matchid/"
    iceberg: str = "iceberg/"


class RiotAPIConfig(BaseModel):
    """Riot API configuration."""

    base_url: str = "https://{region}.api.riotgames.com"
    match_v5_url: str = "https://{cluster}.api.riotgames.com"
    timeout_seconds: int = 30
    max_retries: int = 5


class ConcurrencyConfig(BaseModel):
    """Concurrency configuration."""

    default_thread_pool_size: int = 8
    max_concurrent_requests: int = 50


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = "INFO"
    format: str = "json"
    include_context: bool = True


class MonitoringConfig(BaseModel):
    """Monitoring configuration."""

    enabled: bool = True
    log_run_summary: bool = True


class BaseConfig(BaseModel):
    """Base configuration model (shared across all jobs)."""

    gcs: GCSConfig
    paths: PathsConfig
    riot_api: RiotAPIConfig
    concurrency: ConcurrencyConfig
    logging: LoggingConfig
    monitoring: MonitoringConfig


# =============================================================================
# Job A Config
# =============================================================================


class ShardingConfig(BaseModel):
    """Sharding configuration."""

    shard_count: int = 4


class JobAConfig(BaseModel):
    """Job A (User Ingestion) configuration."""

    sharding: ShardingConfig = Field(default_factory=ShardingConfig)
    platforms: list[str] = Field(
        default_factory=lambda: ["kr", "vn2", "euw1", "eun1"]
    )
    platform_region: dict[str, str] = Field(
        default_factory=lambda: {
            "kr": "asia",
            "vn2": "sea",
            "euw1": "europe",
            "eun1": "europe",
        }
    )
    tiers: list[str] = Field(
        default_factory=lambda: [
            "iron",
            "bronze",
            "silver",
            "gold",
            "platinum",
            "emerald",
            "diamond",
        ]
    )
    apex_tiers: list[str] = Field(
        default_factory=lambda: ["challenger", "grandmaster", "master"]
    )
    divisions: list[str] = Field(
        default_factory=lambda: ["I", "II", "III", "IV"]
    )
    buffer_flush_mb: int = 4
    max_concurrent_requests: int = 4


# =============================================================================
# Job B Config
# =============================================================================


class JobBConfig(BaseModel):
    """Job B (Match ID Ingestion) configuration."""

    sharding: ShardingConfig = Field(default_factory=ShardingConfig)
    thread_pool_size: int = 8
    buffer_flush_mb: int = 1
    pagination_count: int = 100
    max_match_ids_per_puuid_first_run: int = 1000
    max_match_ids_per_puuid_incremental: int = 1000
    platforms: list[str] = Field(
        default_factory=lambda: ["kr", "vn2", "euw1", "eun1"]
    )
    platform_region: dict[str, str] = Field(
        default_factory=lambda: {
            "kr": "asia",
            "vn2": "sea",
            "euw1": "europe",
            "eun1": "europe",
        }
    )


# =============================================================================
# Job C Config
# =============================================================================


class JobCConfig(BaseModel):
    """Job C (Match Data Ingestion) configuration."""

    sharding: ShardingConfig = Field(default_factory=ShardingConfig)
    thread_pool_size: int = 8
    buffer_flush_mb: int = 32
    platforms: list[str] = Field(
        default_factory=lambda: ["kr", "vn2", "euw1", "eun1"]
    )
    platform_region: dict[str, str] = Field(
        default_factory=lambda: {
            "kr": "asia",
            "vn2": "sea",
            "euw1": "europe",
            "eun1": "europe",
        }
    )


# =============================================================================
# Job D Config
# =============================================================================


class IcebergTableConfig(BaseModel):
    """Iceberg table configuration."""

    name: str
    gcs_path: str
    partition_by: list[str]


class IcebergCatalogConfig(BaseModel):
    """Iceberg catalog configuration."""

    type: str = "bigquery"
    warehouse: str


class JobDConfig(BaseModel):
    """Job D (Iceberg Sync) configuration."""

    catalog: IcebergCatalogConfig
    tables: list[IcebergTableConfig]


# =============================================================================
# Full Config (Base + Job specific)
# =============================================================================


class FullConfig(BaseModel):
    """Full configuration with base + job-specific settings."""

    gcs: GCSConfig
    paths: PathsConfig
    riot_api: RiotAPIConfig
    concurrency: ConcurrencyConfig
    logging: LoggingConfig
    monitoring: MonitoringConfig
    job_a: JobAConfig
    job_b: JobBConfig
    job_c: JobCConfig
    job_d: JobDConfig


# =============================================================================
# Platform Config
# =============================================================================


class PlatformInfo(BaseModel):
    """Platform information."""

    id: str
    region: str
    display_name: str
    api_platform: str
    shard_id: int


class RegionInfo(BaseModel):
    """Region information."""

    match_api_cluster: str
    api_gateway: str


class PlatformConfig(BaseModel):
    """Platform configuration model."""

    platforms: list[PlatformInfo]
    regions: dict[str, RegionInfo]
    platform_list: list[str]


# =============================================================================
# Loader Functions
# =============================================================================


def _resolve_env_vars(value: Any) -> Any:
    """Recursively resolve environment variables in config values."""
    if isinstance(value, str):
        if value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            return os.environ.get(env_var, value)
        return value
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def _deep_merge(
    base: dict[str, Any], override: dict[str, Any]
) -> dict[str, Any]:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML file and resolve environment variables."""
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return _resolve_env_vars(data)  # type: ignore[no-any-return]


def _get_conf_dir() -> Path:
    """Get the conf directory path."""
    return Path(__file__).parent.parent.parent / "conf"


@lru_cache(maxsize=1)
def load_base_config() -> BaseConfig:
    """Load base configuration from conf/base.yaml."""
    conf_dir = _get_conf_dir()
    base_path = conf_dir / "base.yaml"

    if not base_path.exists():
        raise FileNotFoundError(f"Base config not found: {base_path}")

    data = load_yaml(base_path)
    return BaseConfig(**data)


def load_job_config(job_name: str) -> dict[str, Any]:
    """Load job-specific configuration from conf/{job_name}.yaml."""
    conf_dir = _get_conf_dir()
    job_path = conf_dir / f"{job_name}.yaml"

    if not job_path.exists():
        raise FileNotFoundError(f"Job config not found: {job_path}")

    return load_yaml(job_path)


def load_full_config() -> FullConfig:
    """Load full configuration (base + all jobs merged)."""
    base = load_base_config()
    base_dict = base.model_dump()

    job_names = ["job_a", "job_b", "job_c", "job_d"]
    for job_name in job_names:
        try:
            job_data = load_job_config(job_name)
            base_dict[job_name] = job_data
        except FileNotFoundError:
            pass

    return FullConfig(**base_dict)


def load_config(job_name: str | None = None) -> BaseConfig | FullConfig:
    """Load configuration.

    Args:
        job_name: Optional job name (job_a, job_b, job_c, job_d).
                 If provided, returns FullConfig with job-specific settings merged.

    Returns:
        BaseConfig if job_name is None, FullConfig otherwise.

    """
    if job_name:
        full_config = load_full_config()
        job_key = job_name.lower()
        if not hasattr(full_config, job_key):
            raise ValueError(f"Unknown job: {job_name}")
        return full_config

    return load_base_config()


def load_platform_config() -> PlatformConfig:
    """Load platform configuration from conf/platforms.yaml."""
    conf_dir = _get_conf_dir()
    platform_path = conf_dir / "platforms.yaml"

    if not platform_path.exists():
        raise FileNotFoundError(f"Platform config not found: {platform_path}")

    data = load_yaml(platform_path)
    return PlatformConfig(**data)


def get_platform_region(platform: str, job_name: str | None = None) -> str:
    """Get region for a platform.

    Args:
        platform: Platform ID (e.g., 'kr', 'vn2')
        job_name: Optional job name to get region mapping from that job's config

    Returns:
        Region string ('asia', 'sea', 'europe')

    """
    if job_name:
        try:
            config = load_config(job_name)
            region = getattr(config, "platform_region", None)
            if region is not None:
                return region.get(platform, "")  # type: ignore[no-any-return]
        except FileNotFoundError, AttributeError:
            pass

    return _DEFAULT_PLATFORM_REGION.get(platform, "")


_DEFAULT_PLATFORM_REGION = {
    "kr": "asia",
    "vn2": "sea",
    "euw1": "europe",
    "eun1": "europe",
}


def get_all_platforms() -> list[str]:
    """Get list of all platform IDs."""
    return ["kr", "vn2", "euw1", "eun1"]


def get_region_api_gateway(region: str) -> str:
    """Get API gateway URL for a region."""
    return _REGION_GATEWAYS.get(region, "")


_REGION_GATEWAYS = {
    "asia": "https://asia.api.riotgames.com",
    "sea": "https://sea.api.riotgames.com",
    "europe": "https://europe.api.riotgames.com",
}
