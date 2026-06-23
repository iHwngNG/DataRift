"""Match-V5 API wrappers for Riot Games API.

Provides access to match IDs by PUUID and match details.
"""

from __future__ import annotations

from typing import Any

import httpx

from datarift.riot_client.regions import get_cluster


async def get_match_ids_by_puuid(
    puuid: str,
    region: str,
    api_key: str,
    start_time: int | None = None,
    count: int = 100,
    start: int = 0,
) -> list[str]:
    """Fetch match IDs for a given PUUID.

    Args:
        puuid: Player's PUUID.
        region: Region identifier (e.g., "sea", "kr", "eu").
        api_key: Riot API key.
        start_time: Unix timestamp for earliest match (optional).
        count: Number of match IDs to retrieve (default 100, max 100).
        start: Pagination offset (default 0).

    Returns:
        List of match IDs.

    Raises:
        ValueError: If count is not between 1 and 100.

    """
    if count < 1 or count > 100:
        msg = "count must be between 1 and 100"
        raise ValueError(msg)

    cluster = get_cluster(region)

    base_url = f"https://{cluster}.api.riotgames.com"
    path = f"/lol/match/v5/matches/by-puuid/{puuid}/ids"

    params: dict[str, Any] = {"count": count, "start": start}
    if start_time is not None:
        params["startTime"] = start_time

    async with httpx.AsyncClient() as http_client:
        response = await http_client.get(
            base_url + path,
            params=params,
            headers={"X-RIOT-API-KEY": api_key},
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]


async def get_match_detail(
    match_id: str,
    region: str,
    api_key: str,
) -> dict[str, Any]:
    """Fetch match details for a given match ID.

    Args:
        match_id: Match ID (e.g., "VN1_1234567890").
        region: Region identifier (e.g., "sea", "kr", "eu").
        api_key: Riot API key.

    Returns:
        Match detail data as a dictionary.

    """
    cluster = get_cluster(region)

    base_url = f"https://{cluster}.api.riotgames.com"
    path = f"/lol/match/v5/matches/{match_id}"

    async with httpx.AsyncClient() as http_client:
        response = await http_client.get(
            base_url + path,
            headers={"X-RIOT-API-KEY": api_key},
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]
