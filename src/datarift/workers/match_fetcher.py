"""Match detail fetcher for Job C Worker.

Provides utilities to fetch match details from Riot Match-V5 API and derive
partition dates from game timestamps.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import httpx

from datarift.riot_client.regions import get_cluster


logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when Riot API returns 429 Too Many Requests."""

    def __init__(self, retry_after_seconds: float | None = None) -> None:
        """Initialize RateLimitError.

        Args:
            retry_after_seconds: Optional seconds to wait before retrying.

        """
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"Rate limited. Retry after {retry_after_seconds}s"
            if retry_after_seconds
            else "Rate limited"
        )


class MatchNotFoundError(Exception):
    """Raised when a match ID returns 404 Not Found."""

    pass


async def fetch_match_detail(
    match_id: str,
    region: str,
    api_key: str,
) -> dict[str, Any]:
    """Fetch match details for a given match ID from Riot Match-V5 API.

    Args:
        match_id: Match ID (e.g., "VN1_1234567890").
        region: Region identifier (e.g., "sea", "kr", "eu").
        api_key: Riot API key.

    Returns:
        Match detail data as a dictionary with full response JSON.

    Raises:
        RateLimitError: When API returns 429 Too Many Requests.
        MatchNotFoundError: When match ID returns 404 Not Found.
        httpx.HTTPStatusError: For other HTTP errors.

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

        if response.status_code == 429:
            retry_after = None
            retry_after_header = response.headers.get("Retry-After")
            if retry_after_header is not None:
                try:
                    retry_after = float(retry_after_header)
                except ValueError:
                    pass
            raise RateLimitError(retry_after_seconds=retry_after)

        if response.status_code == 404:
            logger.warning(f"[match_fetcher] Match not found: {match_id}")
            raise MatchNotFoundError(f"Match {match_id} not found (404)")

        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]


def derive_partition_date(game_start_timestamp: int | float) -> tuple[int, int, int]:
    """Derive year, month, and date from game start timestamp.

    Converts a Unix timestamp in milliseconds (as returned by Riot API)
    to calendar date components for partition key generation.

    Args:
        game_start_timestamp: Unix timestamp in milliseconds since epoch.
            Example: 1700000000000 -> datetime(2023, 11, 14, ...)

    Returns:
        Tuple of (year, month, date) as integers.
        Example: 1700000000000 -> (2023, 11, 14)

    Raises:
        ValueError: If timestamp is negative or results in invalid date.

    """
    if game_start_timestamp < 0:
        raise ValueError("game_start_timestamp must be non-negative")

    # Convert milliseconds to seconds
    timestamp_seconds = game_start_timestamp / 1000.0

    # Convert to datetime in UTC
    dt = datetime.fromtimestamp(timestamp_seconds, tz=UTC)

    return (dt.year, dt.month, dt.day)
