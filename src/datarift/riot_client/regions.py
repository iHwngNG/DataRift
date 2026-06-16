"""Platform to region/cluster mapping for Riot Games API.

Static mappings for platform IDs to regions and clusters used in Match-V5 API.
"""

from __future__ import annotations


__all__ = [
    "get_cluster",
    "platform_to_region",
]

# Platform to region mapping
_PLATFORM_TO_REGION: dict[str, str] = {
    "vn2": "sea",
    "kr": "kr",
    "euw1": "eu",
    "eun1": "eu",
}

# Platform to cluster mapping for Match-V5 API
_PLATFORM_TO_CLUSTER: dict[str, str] = {
    "vn2": "sea",
    "kr": "kr",
    "euw1": "europe",
    "eun1": "europe",
}


def platform_to_region(platform: str) -> str:
    """Convert platform ID to region identifier.

    Args:
        platform: Platform ID (e.g., "vn2", "kr", "euw1", "eun1").

    Returns:
        Region identifier (e.g., "sea", "kr", "eu").

    Raises:
        ValueError: If the platform is not supported.

    """
    if platform not in _PLATFORM_TO_REGION:
        supported = ", ".join(sorted(_PLATFORM_TO_REGION))
        msg = f"Unsupported platform: {platform!r}. Supported platforms: {supported}"
        raise ValueError(msg)
    return _PLATFORM_TO_REGION[platform]


def get_cluster(platform: str) -> str:
    """Get the cluster for Match-V5 API requests.

    Match-V5 uses a different base URL structure:
    https://{cluster}.api.riotgames.com

    Args:
        platform: Platform ID (e.g., "vn2", "kr", "euw1", "eun1").

    Returns:
        Cluster identifier (e.g., "sea", "kr", "europe").

    Raises:
        ValueError: If the platform is not supported.

    """
    if platform not in _PLATFORM_TO_CLUSTER:
        supported = ", ".join(sorted(_PLATFORM_TO_CLUSTER))
        msg = f"Unsupported platform: {platform!r}. Supported platforms: {supported}"
        raise ValueError(msg)
    return _PLATFORM_TO_CLUSTER[platform]
