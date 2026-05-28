"""
riot_client.py
~~~~~~~~~~~~~~
A production-grade HTTP client for the Riot Games API and Data Dragon CDN.

Endpoints covered:
  - League / Player  : GET /lol/league/v4/entries/{queue}/{tier}/{division}
  - Match IDs        : GET /lol/match/v5/matches/by-puuid/{puuid}/ids
  - Match Detail     : GET /lol/match/v5/matches/{matchId}
  - Static Data      : Riot Data Dragon CDN (champions, runes, items)

Rate-limit handling:
  - HTTP 429 → sleep for the number of seconds specified in `Retry-After`
    header then retry, up to `max_retries` times.
  - HTTP 5xx → exponential backoff retry.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests
from requests import Response, Session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATA_DRAGON_BASE = "https://ddragon.leagueoflegends.com"

# Mapping from short region tag to the Riot regional routing host.
# Ref: https://developer.riotgames.com/docs/lol#routing-values
_REGIONAL_HOSTS: dict[str, str] = {
    "americas": "americas",
    "asia": "asia",
    "europe": "europe",
    "sea": "sea",
    # Convenience aliases
    "na": "americas",
    "br": "americas",
    "lan": "americas",
    "las": "americas",
    "kr": "asia",
    "jp": "asia",
    "euw": "europe",
    "eune": "europe",
    "tr": "europe",
    "ru": "europe",
    "oce": "sea",
    "sg": "sea",
    "ph": "sea",
    "th": "sea",
    "tw": "sea",
    "vn": "sea",
}


# ---------------------------------------------------------------------------
# RiotClient
# ---------------------------------------------------------------------------


class RiotClient:
    """Client for the Riot Games REST API and Data Dragon CDN.

    Args:
        api_key: Riot developer API key (``RGAPI-...``).
        platform: Platform routing value, e.g. ``"kr"``, ``"euw1"``,
            ``"na1"``.  Used for summoner/league endpoints.
        region: Regional routing value, e.g. ``"asia"``, ``"europe"``,
            ``"americas"``.  Used for match-v5 endpoints.  When omitted the
            class will infer the region from *platform* using the lookup table
            above, falling back to ``"asia"`` if unknown.
        max_retries: Maximum number of times to retry a failed request.
        backoff_factor: Multiplicative factor for exponential back-off on 5xx
            errors.  First retry waits ``backoff_factor`` seconds, second waits
            ``2 * backoff_factor``, etc.
        timeout: HTTP request timeout in seconds (default ``10``).
    """

    def __init__(
        self,
        api_key: str,
        platform: str = "kr",
        region: str | None = None,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        timeout: int = 10,
    ) -> None:
        self.api_key = api_key
        self.platform = platform.lower()
        self.region = (
            region.lower() if region else _REGIONAL_HOSTS.get(self.platform, "asia")
        )
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout

        # Base URLs
        self._platform_url = f"https://{self.platform}.api.riotgames.com"
        self._regional_url = f"https://{self.region}.api.riotgames.com"

        # Reusable session
        self._session: Session = requests.Session()
        self._session.headers.update({"X-Riot-Token": self.api_key})

    # -----------------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Any:
        """Send an HTTP request with automatic retry and rate-limit handling.

        Args:
            method: HTTP method string (``"GET"``, ``"POST"``, …).
            url: Full request URL.
            **kwargs: Extra keyword arguments forwarded to
                :meth:`requests.Session.request`.

        Returns:
            Parsed JSON response body.

        Raises:
            requests.HTTPError: When the request fails after all retries.
        """
        kwargs.setdefault("timeout", self.timeout)

        for attempt in range(self.max_retries + 1):
            response: Response = self._session.request(method, url, **kwargs)

            # --- Rate limited ---
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 1))
                logger.warning(
                    "Rate limited by Riot API (attempt %d/%d). "
                    "Waiting %d s before retry.",
                    attempt + 1,
                    self.max_retries,
                    retry_after,
                )
                time.sleep(retry_after)
                continue

            # --- Server error → exponential backoff ---
            if response.status_code >= 500:
                wait = self.backoff_factor * (2**attempt)
                logger.warning(
                    "Server error %d (attempt %d/%d). Retrying in %.1f s.",
                    response.status_code,
                    attempt + 1,
                    self.max_retries,
                    wait,
                )
                time.sleep(wait)
                continue

            # --- Success or non-retriable client error ---
            response.raise_for_status()
            return response.json()

        # Exhausted all retries
        response.raise_for_status()  # type: ignore[possibly-undefined]

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """Convenience wrapper around :meth:`_request` for GET calls."""
        return self._request("GET", url, params=params)

    # -----------------------------------------------------------------------
    # User / League endpoints  (Platform routing)
    # -----------------------------------------------------------------------

    def get_league_entries(
        self,
        tier: str,
        division: str,
        queue: str = "RANKED_SOLO_5x5",
        page: int = 1,
    ) -> list[dict]:
        """Fetch a page of ranked league entries for a given tier/division.

        Corresponds to ``GET /lol/league/v4/entries/{queue}/{tier}/{division}``.

        Each entry contains the player's ``puuid``, ``leaguePoints``,
        ``wins``, ``losses``, and metadata flags such as ``hotStreak``.

        Args:
            tier: Rank tier, e.g. ``"DIAMOND"``, ``"GOLD"``, ``"PLATINUM"``.
            division: Rank division, e.g. ``"I"``, ``"II"``, ``"III"``, ``"IV"``.
            queue: Queue type (default ``"RANKED_SOLO_5x5"``).
            page: 1-based page index (default ``1``).

        Returns:
            List of league entry dictionaries.

        Example::

            entries = client.get_league_entries("DIAMOND", "II")
            for entry in entries:
                print(entry["puuid"], entry["leaguePoints"])
        """
        url = f"{self._platform_url}/lol/league/v4/entries/{queue}/{tier}/{division}"
        return self._get(url, params={"page": page})

    # -----------------------------------------------------------------------
    # Match endpoints  (Regional routing)
    # -----------------------------------------------------------------------

    def get_match_ids_by_puuid(
        self,
        puuid: str,
        queue: int | None = None,
        count: int = 20,
        start: int = 0,
    ) -> list[str]:
        """Fetch a list of match IDs for a player's recent match history.

        Corresponds to
        ``GET /lol/match/v5/matches/by-puuid/{puuid}/ids``.

        Args:
            puuid: Player's PUUID (encrypted universally unique identifier).
            queue: Optional queue ID filter, e.g. ``420`` for Ranked Solo/Duo.
                When ``None`` all queues are returned.
            count: Number of match IDs to return (max ``100``).
            start: Start index for pagination (default ``0``).

        Returns:
            List of match ID strings, e.g. ``["KR_8212408629", ...]``.
        """
        url = f"{self._regional_url}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params: dict[str, Any] = {"count": count, "start": start}
        if queue is not None:
            params["queue"] = queue
        return self._get(url, params=params)

    def get_match(self, match_id: str) -> dict:
        """Fetch the full match detail object for a given match ID.

        Corresponds to ``GET /lol/match/v5/matches/{matchId}``.

        The returned payload contains two top-level keys:
        - ``metadata``: data version, match ID, and list of PUUIDs.
        - ``info``: full game data including all 10 participant records.

        Key fields in each participant record include ``championName``,
        ``kills``, ``deaths``, ``assists``, ``totalDamageDealtToChampions``,
        ``goldEarned``, ``win``, ``teamPosition``, and the nested
        ``challenges`` block.

        Args:
            match_id: Match ID string, e.g. ``"KR_8212408629"``.

        Returns:
            Full match detail dictionary.
        """
        url = f"{self._regional_url}/lol/match/v5/matches/{match_id}"
        return self._get(url)

    # -----------------------------------------------------------------------
    # Static data  (Data Dragon CDN)
    # -----------------------------------------------------------------------

    def get_latest_version(self) -> str:
        """Fetch the latest available Data Dragon patch version string.

        Returns:
            Version string, e.g. ``"14.10.1"``.
        """
        url = f"{_DATA_DRAGON_BASE}/api/versions.json"
        versions: list[str] = self._get(url)
        return versions[0]

    def get_champions(self, version: str | None = None) -> dict:
        """Fetch champion static data from the Data Dragon CDN.

        Args:
            version: Data Dragon version string.  When ``None`` the latest
                version is fetched automatically.

        Returns:
            Dictionary keyed by champion key containing champion metadata
            (name, title, blurb, stats, tags, image, etc.).
        """
        version = version or self.get_latest_version()
        url = f"{_DATA_DRAGON_BASE}/cdn/{version}/data/en_US/champion.json"
        payload = self._get(url)
        return payload.get("data", payload)

    def get_runes(self, version: str | None = None) -> list[dict]:
        """Fetch rune tree static data from the Data Dragon CDN.

        Args:
            version: Data Dragon version string.  When ``None`` the latest
                version is fetched automatically.

        Returns:
            List of rune path dictionaries, each containing ``id``, ``key``,
            ``icon``, ``name``, and nested ``slots`` → ``runes`` records.
        """
        version = version or self.get_latest_version()
        url = f"{_DATA_DRAGON_BASE}/cdn/{version}/data/en_US/runesReforged.json"
        return self._get(url)

    def get_items(self, version: str | None = None) -> dict:
        """Fetch item static data from the Data Dragon CDN.

        Args:
            version: Data Dragon version string.  When ``None`` the latest
                version is fetched automatically.

        Returns:
            Dictionary keyed by item ID string containing item metadata
            (name, description, gold cost, stats, tags, etc.).
        """
        version = version or self.get_latest_version()
        url = f"{_DATA_DRAGON_BASE}/cdn/{version}/data/en_US/item.json"
        payload = self._get(url)
        return payload.get("data", payload)
