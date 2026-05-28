"""
riot_config.py
~~~~~~~~~~~~~~
Riot Games API configuration for the DataRift pipeline.

Controls the API key, platform/region routing, HTTP retry behaviour,
application-level rate-limit budgets, and endpoint base URLs.
All settings can be overridden at runtime via environment variables.

Rate-limit defaults reflect the Riot developer key tiers:
    - 20 requests / 1 second
    - 100 requests / 2 minutes
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_DATA_DRAGON_BASE_DEFAULT = "https://ddragon.leagueoflegends.com"

_APEX_TIERS: frozenset[str] = frozenset({"MASTER", "GRANDMASTER", "CHALLENGER"})
_DEFAULT_TIERS = "IRON,BRONZE,SILVER,GOLD,PLATINUM,EMERALD,DIAMOND"
_DEFAULT_DIVISIONS = "I,II,III,IV"

def _parse_csv(value: str) -> list[str]:
    """Split a comma-separated string into a stripped, non-empty list."""
    return [item.strip().upper() for item in value.split(",") if item.strip()]

@dataclass(frozen=True)
class RiotConfig:
    """Typed configuration for the Riot Games API client.

    Attributes:
        api_key: Riot developer API key (``RGAPI-...``). Required — raises
            :exc:`ValueError` if not set in the environment.
        platform: Platform routing value used for league/summoner endpoints,
            e.g. ``"kr"``, ``"euw1"``, ``"na1"``.
        region: Regional routing value used for match-v5 endpoints,
            e.g. ``"asia"``, ``"europe"``, ``"americas"``.
        max_retries: Maximum number of HTTP retry attempts on transient errors.
        backoff_factor: Multiplier for exponential back-off on 5xx errors.
            Retry *n* waits ``backoff_factor × 2ⁿ`` seconds.
        timeout: Per-request HTTP timeout in seconds.
        rate_limit_per_second: Application-level cap on requests per second.
        rate_limit_per_two_minutes: Application-level cap on requests per
            2-minute window.
        data_dragon_base: Base URL for the Riot Data Dragon CDN.
        queue_id: Riot queue ID to filter match history.
            ``420`` = Ranked Solo/Duo, ``440`` = Ranked Flex.
        tiers: List of rank tiers to ingest, e.g. ``["DIAMOND", "PLATINUM"]``.
        divisions: List of rank divisions to ingest, e.g. ``["I", "II", "III", "IV"]``.
        request_delay_seconds: Minimum sleep between consecutive API requests.
    """

    api_key: str
    platform: str
    region: str
    max_retries: int
    backoff_factor: float
    timeout: int
    rate_limit_per_second: int
    rate_limit_per_two_minutes: int
    data_dragon_base: str
    queue_id: int
    tiers: list[str]
    divisions: list[str]
    request_delay_seconds: float

    @classmethod
    def from_env(cls) -> "RiotConfig":
        """Instantiate :class:`RiotConfig` from environment variables.

        Returns:
            A fully populated :class:`RiotConfig` instance.

        Raises:
            ValueError: If ``RIOT_API_KEY`` is not set or is empty.
        """
        api_key = os.getenv("RIOT_API_KEY", "")
        if not api_key:
            raise ValueError(
                "RIOT_API_KEY environment variable is required but not set. "
                "Obtain a key from https://developer.riotgames.com/ and export it."
            )

        tiers = _parse_csv(os.getenv("RIOT_TIERS", _DEFAULT_TIERS))
        apex_found = [t for t in tiers if t in _APEX_TIERS]
        if apex_found:
            raise ValueError(
                f"Apex tiers {apex_found} cannot be fetched by pagination. "
                "Remove them from the RIOT_TIERS env var."
            )

        return cls(
            api_key=api_key,
            platform=os.getenv("RIOT_PLATFORM", "kr"),
            region=os.getenv("RIOT_REGION", "asia"),
            max_retries=int(os.getenv("RIOT_MAX_RETRIES", "5")),
            backoff_factor=float(os.getenv("RIOT_BACKOFF_FACTOR", "1.0")),
            timeout=int(os.getenv("RIOT_TIMEOUT", "10")),
            rate_limit_per_second=int(os.getenv("RIOT_RATE_LIMIT_PER_SEC", "20")),
            rate_limit_per_two_minutes=int(
                os.getenv("RIOT_RATE_LIMIT_PER_2MIN", "100")
            ),
            data_dragon_base=os.getenv(
                "RIOT_DATA_DRAGON_BASE", _DATA_DRAGON_BASE_DEFAULT
            ),
            queue_id=int(os.getenv("RIOT_QUEUE_ID", "420")),
            tiers=tiers,
            divisions=_parse_csv(os.getenv("RIOT_DIVISIONS", _DEFAULT_DIVISIONS)),
            request_delay_seconds=float(os.getenv("RIOT_REQUEST_DELAY_SECONDS", "0.05")),
        )

    @property
    def platform_base_url(self) -> str:
        """Base URL for platform-routed endpoints (league, summoner).

        Returns:
            Full base URL, e.g. ``"https://kr.api.riotgames.com"``.
        """
        return f"https://{self.platform}.api.riotgames.com"

    @property
    def regional_base_url(self) -> str:
        """Base URL for regional-routed endpoints (match-v5).

        Returns:
            Full base URL, e.g. ``"https://asia.api.riotgames.com"``.
        """
        return f"https://{self.region}.api.riotgames.com"

    @property
    def all_segments(self) -> list[tuple[str, str]]:
        """Cartesian product of tiers × divisions as (tier, division) pairs.

        Returns:
            e.g. ``[("IRON", "I"), ("IRON", "II"), …, ("DIAMOND", "IV")]``
        """
        return [
            (tier, division)
            for tier in self.tiers
            for division in self.divisions
        ]
