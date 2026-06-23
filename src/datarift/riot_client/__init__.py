"""Riot Games API client module.

Provides async HTTP client with rate limiting, retry logic, and wrappers
for League-Entries-V4 and Match-V5 APIs.
"""

from __future__ import annotations

from datarift.riot_client.client import RateLimitError, RiotClient
from datarift.riot_client.league_api import (
    Division,
    QueueType,
    SummonerEntry,
    Tier,
    get_all_summoner_entries,
    get_entries_for_tier_and_division,
)
from datarift.riot_client.match_api import (
    get_match_detail,
    get_match_ids_by_puuid,
)
from datarift.riot_client.regions import get_cluster, platform_to_region


__all__ = [
    "Division",
    "QueueType",
    "RateLimitError",
    "RiotClient",
    "SummonerEntry",
    "Tier",
    "get_all_summoner_entries",
    "get_cluster",
    "get_entries_for_tier_and_division",
    "get_match_detail",
    "get_match_ids_by_puuid",
    "platform_to_region",
]
