"""League-Entries-V4 API wrappers for Riot Games API.

Provides paginated access to ranked league entries for GOLD, SILVER, BRONZE, and IRON tiers.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from typing import Any

from datarift.riot_client.client import RiotClient


class QueueType(Enum):
    """Ranked queue types."""

    RANKED_SOLO_5x5 = "RANKED_SOLO_5x5"


class Tier(Enum):
    """League tiers (excluding CHALLENGER, GRANDMASTER, MASTER)."""

    GOLD = "GOLD"
    SILVER = "SILVER"
    BRONZE = "BRONZE"
    IRON = "IRON"


class Division(Enum):
    """League divisions."""

    ONE = "I"
    TWO = "II"
    THREE = "III"
    FOUR = "IV"


@dataclass
class SummonerEntry:
    """Represents a summoner entry from League-Entries-V4 API."""

    summoner_id: str
    summoner_name: str
    puuid: str
    rank: str
    league_points: int
    wins: int
    losses: int
    tier: str
    queue_type: str
    veteran: bool
    inactive: bool
    fresh_blood: bool
    hot_streak: bool

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SummonerEntry:
        """Create a SummonerEntry from API response dictionary.

        Args:
            data: API response dictionary.

        Returns:
            SummonerEntry instance.

        """
        return cls(
            summoner_id=data["summonerId"],
            summoner_name=data["summonerName"],
            puuid=data["puuid"],
            rank=data["rank"],
            league_points=data["leaguePoints"],
            wins=data["wins"],
            losses=data["losses"],
            tier=data["tier"],
            queue_type=data["queueType"],
            veteran=data.get("veteran", False),
            inactive=data.get("inactive", False),
            fresh_blood=data.get("freshBlood", False),
            hot_streak=data.get("hotStreak", False),
        )


async def get_all_summoner_entries(
    client: RiotClient,
    queue: QueueType = QueueType.RANKED_SOLO_5x5,
    tiers: list[Tier] | None = None,
    divisions: list[Division] | None = None,
) -> AsyncIterator[SummonerEntry]:
    """Fetch all summoner entries for specified tiers and divisions.

    Iterates through all pages until an empty page is returned.

    Args:
        client: RiotClient instance.
        queue: Queue type (default: RANKED_SOLO_5x5).
        tiers: List of tiers to fetch (default: GOLD, SILVER, BRONZE, IRON).
        divisions: List of divisions to fetch (default: ONE, TWO, THREE, FOUR).

    Yields:
        SummonerEntry for each summoner found.

    """
    if tiers is None:
        tiers = list(Tier)
    if divisions is None:
        divisions = list(Division)

    for tier in tiers:
        for division in divisions:
            page = 1
            while True:
                path = f"/lol/league/v4/entries/{queue.value}/{tier.value}/{division.value}"
                params = {"page": page}

                entries = await client.get_json(path, params=params)

                if not entries:
                    break

                for entry_data in entries:
                    yield SummonerEntry.from_dict(entry_data)

                page += 1


async def get_entries_for_tier_and_division(
    client: RiotClient,
    tier: Tier,
    division: Division,
    queue: QueueType = QueueType.RANKED_SOLO_5x5,
) -> list[SummonerEntry]:
    """Fetch all summoner entries for a specific tier and division.

    Iterates through all pages until an empty page is returned.

    Args:
        client: RiotClient instance.
        tier: Tier level.
        division: Division level.
        queue: Queue type (default: RANKED_SOLO_5x5).

    Returns:
        List of SummonerEntry for the specified tier and division.

    """
    entries: list[SummonerEntry] = []
    page = 1

    while True:
        path = f"/lol/league/v4/entries/{queue.value}/{tier.value}/{division.value}"
        params = {"page": page}

        data = await client.get_json(path, params=params)

        if not data:
            break

        for entry_data in data:
            entries.append(SummonerEntry.from_dict(entry_data))

        page += 1

    return entries
