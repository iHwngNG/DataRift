"""
test_fetcher.py
~~~~~~~~~~~~~~~
Unit tests for :class:`~pipeline.fetcher.LeagueEntryFetcher`.

Uses ``unittest.mock`` to intercept ``requests.Session.get`` so no real
HTTP calls are made.

Tests cover:
- Normal pagination: yields pages until an empty list is returned.
- Single-page segment: yields one page then stops.
- 429 handling: retries without counting towards ``max_retries``; respects
  ``Retry-After`` header.
- 5xx handling: applies exponential back-off up to ``max_retries``; raises
  ``RuntimeError`` when exhausted.
- 4xx (non-429) handling: raises ``RuntimeError`` immediately.
- Correct URL construction (tier/division/page/api_key in URL).
- Network error (``requests.RequestException``) retried and eventually raised.
"""

from __future__ import annotations

import os
import sys

# Allow importing from root config and jobs/fetch_users/pipeline
_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "jobs", "fetch_users"))

import unittest
from unittest.mock import MagicMock, patch

import requests
from pipeline.fetcher import LeagueEntryFetcher

_BASE_URL = "https://kr.api.riotgames.com"
_API_KEY = "RGAPI-test-key"


def _make_fetcher(**kwargs) -> LeagueEntryFetcher:
    defaults = dict(
        platform_base_url=_BASE_URL,
        api_key=_API_KEY,
        request_delay_seconds=0,  # No sleep in tests
        max_retries=3,
    )
    defaults.update(kwargs)
    return LeagueEntryFetcher(**defaults)


def _mock_response(status: int, json_data=None, headers=None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_data if json_data is not None else []
    resp.headers = headers or {}
    resp.text = str(json_data)
    return resp


class TestFetcherURLConstruction(unittest.TestCase):
    @patch("pipeline.fetcher.requests.Session.get")
    def test_url_contains_tier_division_page_key(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(200, [])  # Empty → stops after 1 call
        fetcher = _make_fetcher()
        list(fetcher.iter_pages("DIAMOND", "I"))

        called_url: str = mock_get.call_args[0][0]
        self.assertIn("/lol/league/v4/entries/RANKED_SOLO_5x5/DIAMOND/I", called_url)
        self.assertIn("page=1", called_url)
        self.assertIn(f"api_key={_API_KEY}", called_url)
        self.assertIn(_BASE_URL, called_url)


class TestFetcherPagination(unittest.TestCase):
    @patch("pipeline.fetcher.requests.Session.get")
    def test_stops_on_empty_page(self, mock_get: MagicMock) -> None:
        """Should yield 2 pages then stop when page 3 is empty."""
        page1 = [{"puuid": "a"}]
        page2 = [{"puuid": "b"}, {"puuid": "c"}]
        mock_get.side_effect = [
            _mock_response(200, page1),
            _mock_response(200, page2),
            _mock_response(200, []),  # Empty → stop
        ]
        fetcher = _make_fetcher()
        pages = list(fetcher.iter_pages("GOLD", "II"))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], page1)
        self.assertEqual(pages[1], page2)

    @patch("pipeline.fetcher.requests.Session.get")
    def test_single_page_then_empty(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = [
            _mock_response(200, [{"puuid": "x"}]),
            _mock_response(200, []),
        ]
        fetcher = _make_fetcher()
        pages = list(fetcher.iter_pages("IRON", "IV"))
        self.assertEqual(len(pages), 1)

    @patch("pipeline.fetcher.requests.Session.get")
    def test_immediate_empty_yields_nothing(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(200, [])
        fetcher = _make_fetcher()
        pages = list(fetcher.iter_pages("BRONZE", "III"))
        self.assertEqual(pages, [])


class TestFetcherRateLimiting(unittest.TestCase):
    @patch("pipeline.fetcher.time.sleep")
    @patch("pipeline.fetcher.requests.Session.get")
    def test_429_retried_without_max_retries_decrement(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """429 should not consume a retry slot; the request should eventually succeed."""
        good_page = [{"puuid": "y"}]
        mock_get.side_effect = [
            _mock_response(429, headers={"Retry-After": "0.01"}),
            _mock_response(429, headers={"Retry-After": "0.01"}),
            _mock_response(200, good_page),
            _mock_response(200, []),  # Stop pagination
        ]
        fetcher = _make_fetcher(max_retries=1)  # Would fail if 429 counted
        pages = list(fetcher.iter_pages("PLATINUM", "I"))
        self.assertEqual(pages, [good_page])
        # sleep should have been called for 429 waits
        self.assertGreaterEqual(mock_sleep.call_count, 2)

    @patch("pipeline.fetcher.time.sleep")
    @patch("pipeline.fetcher.requests.Session.get")
    def test_429_uses_retry_after_header(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        mock_get.side_effect = [
            _mock_response(429, headers={"Retry-After": "5"}),
            _mock_response(200, []),
        ]
        fetcher = _make_fetcher()
        list(fetcher.iter_pages("SILVER", "II"))
        mock_sleep.assert_any_call(7.0)


class TestFetcherServerErrors(unittest.TestCase):
    @patch("pipeline.fetcher.time.sleep")
    @patch("pipeline.fetcher.requests.Session.get")
    def test_5xx_retried_up_to_max_retries(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """After max_retries 5xx responses, RuntimeError is raised."""
        mock_get.return_value = _mock_response(503)
        fetcher = _make_fetcher(max_retries=2)
        with self.assertRaises(RuntimeError):
            list(fetcher.iter_pages("EMERALD", "III"))

    @patch("pipeline.fetcher.time.sleep")
    @patch("pipeline.fetcher.requests.Session.get")
    def test_5xx_recovers_if_subsequent_call_succeeds(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        good = [{"puuid": "z"}]
        mock_get.side_effect = [
            _mock_response(503),
            _mock_response(200, good),
            _mock_response(200, []),
        ]
        fetcher = _make_fetcher(max_retries=3)
        pages = list(fetcher.iter_pages("DIAMOND", "IV"))
        self.assertEqual(pages, [good])


class TestFetcherClientErrors(unittest.TestCase):
    @patch("pipeline.fetcher.requests.Session.get")
    def test_404_raises_immediately(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(404)
        fetcher = _make_fetcher()
        with self.assertRaises(RuntimeError):
            list(fetcher.iter_pages("IRON", "I"))

    @patch("pipeline.fetcher.requests.Session.get")
    def test_403_raises_immediately(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(403)
        fetcher = _make_fetcher()
        with self.assertRaises(RuntimeError):
            list(fetcher.iter_pages("GOLD", "III"))


class TestFetcherNetworkError(unittest.TestCase):
    @patch("pipeline.fetcher.time.sleep")
    @patch("pipeline.fetcher.requests.Session.get")
    def test_network_error_retried_then_raises(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        mock_get.side_effect = requests.ConnectionError("Connection refused")
        fetcher = _make_fetcher(max_retries=2)
        with self.assertRaises(RuntimeError):
            list(fetcher.iter_pages("BRONZE", "IV"))


class TestFetcherPageLimiting(unittest.TestCase):
    @patch("pipeline.fetcher.requests.Session.get")
    def test_respects_max_pages_limit(self, mock_get: MagicMock) -> None:
        """Should yield max_pages lists of entries and stop, even if more are available."""
        page1 = [{"puuid": "p1"}]
        page2 = [{"puuid": "p2"}]
        page3 = [{"puuid": "p3"}]
        mock_get.side_effect = [
            _mock_response(200, page1),
            _mock_response(200, page2),
            _mock_response(200, page3),
        ]
        fetcher = _make_fetcher()
        pages = list(fetcher.iter_pages("DIAMOND", "III", max_pages=2))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], page1)
        self.assertEqual(pages[1], page2)
        # Ensure it did not request the 3rd page
        self.assertEqual(mock_get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
