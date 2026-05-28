"""
main.py
~~~~~~~
Entry point for the ``fetch_users`` Cloud Run Job.

Orchestration overview
----------------------
For each tier (e.g. DIAMOND), all divisions (I, II, III, IV) are fetched
**concurrently** using a :class:`~concurrent.futures.ThreadPoolExecutor` with
up to ``PIPELINE_MAX_WORKERS`` threads (default 4).

Each thread:

1. Pages through Riot's League Entries API until an empty page is returned
   or the page limit (``PIPELINE_MAX_PAGES``) is reached.
2. Publishes each page of records as individual Pub/Sub messages to the
   configured topic (``PUBSUB_TOPIC_ID``).

The downstream ``parquet_writer`` job consumes those messages, accumulates
records in a 128 MB in-RAM buffer, and flushes to GCS as Parquet files.

429 / Rate-limit handling
--------------------------
Handled transparently inside :class:`~pipeline.fetcher.LeagueEntryFetcher`.
On a 429 response the thread sleeps for ``Retry-After + 2`` seconds before
retrying.  This wait does **not** consume a retry slot.

Exit codes
----------
0   Normal completion (all segments processed or time budget exceeded).
1   Fatal configuration or unrecoverable API error.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Optional

# Allow importing from root 'config' package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pipeline.fetcher import LeagueEntryFetcher
from pipeline.publisher import PubSubPublisher

from config.pipeline_config import PipelineConfig
from config.riot_config import RiotConfig

# ---------------------------------------------------------------------------
# Logging — structured for Cloud Logging ingestion
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("fetch_users")


# ---------------------------------------------------------------------------
# Per-division worker
# ---------------------------------------------------------------------------

def _fetch_division(
    fetcher: LeagueEntryFetcher,
    publisher: PubSubPublisher,
    tier: str,
    division: str,
    max_pages: Optional[int],
    start_time: float,
    max_runtime_seconds: int,
) -> int:
    """Fetch all pages for one (tier, division) pair and publish to Pub/Sub.

    Args:
        fetcher: Initialised :class:`LeagueEntryFetcher` (one per thread
            because :class:`requests.Session` is **not** thread-safe).
        publisher: Shared :class:`PubSubPublisher` (thread-safe).
        tier: e.g. ``"DIAMOND"``.
        division: e.g. ``"I"``.
        max_pages: Optional page cap.
        start_time: ``time.monotonic()`` value from the start of the job.
        max_runtime_seconds: Kill-switch budget in seconds.

    Returns:
        Total number of records published.
    """
    total = 0
    for page_records in fetcher.iter_pages(tier, division, max_pages=max_pages):
        # Time budget guard
        if time.monotonic() - start_time >= max_runtime_seconds:
            logger.warning(
                "Time budget exceeded mid-division %s/%s — stopping this thread.",
                tier,
                division,
            )
            break

        publisher.publish_batch(page_records, tier, division)
        total += len(page_records)

    logger.info(
        "Division %s/%s complete — published %d records.",
        tier,
        division,
        total,
    )
    return total


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(riot_cfg: RiotConfig, pipeline_cfg: PipelineConfig) -> None:
    """Execute the parallel fetch-and-publish pipeline.

    Args:
        riot_cfg: Riot API configuration.
        pipeline_cfg: Execution pipeline configuration.
    """
    start_time = time.monotonic()

    # Read Pub/Sub config from env
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT", "")
    topic_id = os.environ.get("PUBSUB_TOPIC_ID", "")
    if not project_id or not topic_id:
        raise ValueError(
            "GOOGLE_CLOUD_PROJECT and PUBSUB_TOPIC_ID environment variables are required."
        )

    publisher = PubSubPublisher(
        project_id=project_id,
        topic_id=topic_id,
        platform=riot_cfg.platform,
    )

    # Group segments by tier so we submit all divisions of a tier in one batch
    tiers_order = riot_cfg.tiers
    divisions = riot_cfg.divisions
    max_workers = pipeline_cfg.max_workers
    max_pages = pipeline_cfg.max_pages

    logger.info(
        "Job started — platform=%s tiers=%s divisions=%s max_workers=%d topic=%s",
        riot_cfg.platform,
        tiers_order,
        divisions,
        max_workers,
        topic_id,
    )

    total_records = 0

    try:
        for tier in tiers_order:
            # Check time budget before starting each tier
            if time.monotonic() - start_time >= pipeline_cfg.max_runtime_seconds:
                logger.warning("Time budget exceeded before tier %s — stopping.", tier)
                break

            logger.info(
                "Fetching tier %s with %d division(s) across %d thread(s).",
                tier,
                len(divisions),
                min(len(divisions), max_workers),
            )

            # One fetcher per thread — requests.Session is NOT thread-safe
            futures: dict[Future, tuple[str, str]] = {}

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for division in divisions:
                    # Each thread gets its own LeagueEntryFetcher (its own Session)
                    thread_fetcher = LeagueEntryFetcher(
                        platform_base_url=riot_cfg.platform_base_url,
                        api_key=riot_cfg.api_key,
                        request_delay_seconds=riot_cfg.request_delay_seconds,
                        max_retries=riot_cfg.max_retries,
                    )
                    f = executor.submit(
                        _fetch_division,
                        thread_fetcher,
                        publisher,
                        tier,
                        division,
                        max_pages,
                        start_time,
                        pipeline_cfg.max_runtime_seconds,
                    )
                    futures[f] = (tier, division)

                for f in as_completed(futures):
                    seg_tier, seg_div = futures[f]
                    try:
                        count = f.result()
                        total_records += count
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "Division %s/%s failed: %s", seg_tier, seg_div, exc
                        )

            logger.info(
                "Tier %s complete. Cumulative records published: %d.",
                tier,
                total_records,
            )

        # Flush any batched-but-unconfirmed publish futures
        publisher.flush()

    finally:
        publisher.close()

    elapsed = time.monotonic() - start_time
    logger.info(
        "Job complete — platform=%s total_records=%d elapsed=%.1fs",
        riot_cfg.platform,
        total_records,
        elapsed,
    )


def main() -> None:
    """Load config from environment and run the job."""
    try:
        riot_cfg = RiotConfig.from_env()
        pipeline_cfg = PipelineConfig.from_env()
    except ValueError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)

    try:
        run(riot_cfg, pipeline_cfg)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unrecoverable error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
