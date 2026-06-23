"""Workers package for DataRift jobs.

Provides worker functions and utilities for parallel job processing:
- threaded_queue: Thread pool execution with rate-limit handling
- puuid_fetcher: Fetches match IDs by PUUID (Job B Worker)
- match_fetcher: Fetches match details from Riot API (Job C Worker)
"""

from __future__ import annotations

from datarift.workers.match_fetcher import (
    MatchNotFoundError,
    RateLimitError,
    derive_partition_date,
    fetch_match_detail,
)
from datarift.workers.puuid_fetcher import (
    create_job_b_worker_fn,
    fetch_match_ids_for_puuid,
    job_b_worker_fn,
)
from datarift.workers.threaded_queue import (
    JobContext,
    ThreadedQueueResult,
    run_threaded,
)


__all__ = [
    # threaded_queue
    "JobContext",
    # match_fetcher
    "MatchNotFoundError",
    "RateLimitError",
    "ThreadedQueueResult",
    # puuid_fetcher
    "create_job_b_worker_fn",
    "derive_partition_date",
    "fetch_match_detail",
    "fetch_match_ids_for_puuid",
    "job_b_worker_fn",
    "run_threaded",
]
