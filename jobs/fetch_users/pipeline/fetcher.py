"""
fetcher.py
~~~~~~~~~~
Paginated fetcher for the Riot League Entries v4 endpoint.

URL template::

    https://{platform}.api.riotgames.com/lol/league/v4/entries/
    RANKED_SOLO_5x5/{tier}/{division}?page={n}&api_key={key}

The fetcher iterates pages starting at 1.  Riot returns an **empty list**
``[]`` when a page has no entries, which signals that the division is
exhausted.

HTTP error handling
-------------------
* **429 Too Many Requests**: honours the ``Retry-After`` response header
  (falls back to exponential back-off capped at 60 s).
* **5xx Server Errors**: exponential back-off, up to ``max_retries`` attempts.
* **Other 4xx**: raised immediately as :exc:`RuntimeError`.
"""

from __future__ import annotations

import logging
import time
from typing import Generator

import requests

logger = logging.getLogger(__name__)

# Queue constant — only RANKED_SOLO_5x5 is supported by this job.
_QUEUE = "RANKED_SOLO_5x5"


class LeagueEntryFetcher:
    """Paginates through Riot League Entries for one (tier, division) segment.

    Args:
        platform_base_url: e.g. ``"https://kr.api.riotgames.com"``
        api_key: Riot API key.
        request_delay_seconds: Minimum sleep between consecutive requests.
        max_retries: Maximum retry attempts on transient HTTP errors.
    """

    def __init__(
        self,
        platform_base_url: str,
        api_key: str,
        request_delay_seconds: float = 0.05,
        max_retries: int = 5,
    ) -> None:
        self._base_url = platform_base_url.rstrip("/")
        self._api_key = api_key
        self._delay = request_delay_seconds
        self._max_retries = max_retries
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def iter_pages(
        self, tier: str, division: str, max_pages: int | None = None
    ) -> Generator[list[dict], None, None]:
        """Yield pages of league entry records for the given tier/division.

        Stops automatically when Riot returns an empty page or when max_pages is reached.

        Args:
            tier: e.g. ``"DIAMOND"``.
            division: e.g. ``"I"``.
            max_pages: Optional maximum number of pages to fetch.

        Yields:
            A ``list[dict]`` of player entry records (may contain up to 205
            entries per page, typical is ~200).

        Raises:
            RuntimeError: On unrecoverable HTTP errors.
        """
        page = 1
        while True:
            if max_pages is not None and page > max_pages:
                logger.info(
                    "Page limit of %d reached for segment %s/%s. Stopping pagination.",
                    max_pages,
                    tier,
                    division,
                )
                return
            records = self._fetch_page(tier, division, page)
            if not records:
                logger.info(
                    "Segment %s/%s exhausted after %d page(s).",
                    tier,
                    division,
                    page - 1,
                )
                return
            yield records
            page += 1
            time.sleep(self._delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_url(self, tier: str, division: str, page: int) -> str:
        return (
            f"{self._base_url}/lol/league/v4/entries/"
            f"{_QUEUE}/{tier}/{division}"
            f"?page={page}&api_key={self._api_key}"
        )

    def _fetch_page(self, tier: str, division: str, page: int) -> list[dict]:
        """Fetch a single page with retry / back-off logic.

        429 responses are retried indefinitely (honouring ``Retry-After``) and
        do **not** consume a retry slot.  Transient 5xx errors and network
        failures are retried up to ``self._max_retries`` times with
        exponential back-off before raising :exc:`RuntimeError`.

        Returns:
            Parsed JSON list, or an empty list if the page is empty.

        Raises:
            RuntimeError: If all retries are exhausted or a non-retryable
                HTTP error is received.
        """
        url = self._build_url(tier, division, page)
        backoff = 1.0
        # Separate counter for real errors (5xx / network) — 429 does NOT increment this.
        error_attempts = 0

        while True:
            try:
                resp = self._session.get(url, timeout=10)
            except requests.RequestException as exc:
                error_attempts += 1
                if error_attempts > self._max_retries:
                    raise RuntimeError(
                        f"Network error after {self._max_retries} retries: {exc}"
                    ) from exc
                wait = backoff * (2 ** (error_attempts - 1))
                logger.warning(
                    "Network error (attempt %d/%d), retrying in %.1fs: %s",
                    error_attempts,
                    self._max_retries,
                    wait,
                    exc,
                )
                time.sleep(wait)
                continue

            # ---- Rate limited (429) — never counts as an error attempt --
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", backoff * 2))
                wait = retry_after + 2  # +2 s safety buffer as per pipeline policy
                logger.warning(
                    "429 Too Many Requests on %s/%s page %d — "
                    "waiting %.1fs (Retry-After=%.1fs + 2s buffer).",
                    tier,
                    division,
                    page,
                    wait,
                    retry_after,
                )
                time.sleep(wait)
                continue  # Does NOT increment error_attempts

            # ---- Server errors (5xx) ------------------------------------
            if resp.status_code >= 500:
                error_attempts += 1
                if error_attempts > self._max_retries:
                    raise RuntimeError(
                        f"Server error {resp.status_code} after "
                        f"{self._max_retries} retries on {tier}/{division} page {page}."
                    )
                wait = backoff * (2 ** (error_attempts - 1))
                logger.warning(
                    "HTTP %d on %s/%s page %d (attempt %d/%d), retry in %.1fs.",
                    resp.status_code,
                    tier,
                    division,
                    page,
                    error_attempts,
                    self._max_retries,
                    wait,
                )
                time.sleep(wait)
                continue

            # ---- Client errors (4xx, non-429) — fail immediately --------
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Client error {resp.status_code} for "
                    f"{tier}/{division} page {page}: {resp.text[:200]}"
                )

            # ---- Success ------------------------------------------------
            data: list[dict] = resp.json()
            logger.debug(
                "Fetched %d records — %s/%s page %d.",
                len(data),
                tier,
                division,
                page,
            )
            return data
