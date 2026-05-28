import unittest
from unittest.mock import patch, MagicMock
import requests
import sys
from pathlib import Path

# Add project root to sys.path to allow execution from any context
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.clients.riot_client import RiotClient


class TestRiotClient(unittest.TestCase):
    def setUp(self):
        self.api_key = "RGAPI-mock-key"
        self.client = RiotClient(
            api_key=self.api_key, platform="kr", max_retries=2, backoff_factor=0.1
        )

    def test_initialization_and_routing(self):
        # Platform 'kr' should map to region 'asia'
        self.assertEqual(self.client.platform, "kr")
        self.assertEqual(self.client.region, "asia")
        self.assertEqual(self.client._platform_url, "https://kr.api.riotgames.com")
        self.assertEqual(self.client._regional_url, "https://asia.api.riotgames.com")
        self.assertEqual(self.client._session.headers.get("X-Riot-Token"), self.api_key)

    def test_custom_region(self):
        client = RiotClient(api_key=self.api_key, platform="br1", region="americas")
        self.assertEqual(client.platform, "br1")
        self.assertEqual(client.region, "americas")

    @patch("requests.Session.request")
    def test_successful_request(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success"}
        mock_request.return_value = mock_response

        res = self.client.get_league_entries(tier="DIAMOND", division="I")
        self.assertEqual(res, {"status": "success"})
        mock_request.assert_called_once_with(
            "GET",
            "https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/DIAMOND/I",
            params={"page": 1},
            timeout=10,
        )

    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request")
    def test_rate_limiting_retry(self, mock_request, mock_sleep):
        # First request yields 429 with Retry-After: 3
        # Second request yields 200 success
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {"Retry-After": "3"}

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"status": "recovered"}

        mock_request.side_effect = [mock_429, mock_200]

        res = self.client.get_match("KR_12345")
        self.assertEqual(res, {"status": "recovered"})
        self.assertEqual(mock_request.call_count, 2)
        mock_sleep.assert_called_once_with(3)

    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request")
    def test_server_error_backoff(self, mock_request, mock_sleep):
        # First request yields 500
        # Second request yields 503
        # Third request yields 200 success
        mock_500 = MagicMock()
        mock_500.status_code = 500

        mock_503 = MagicMock()
        mock_503.status_code = 503

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"status": "recovered_5xx"}

        mock_request.side_effect = [mock_500, mock_503, mock_200]

        res = self.client.get_match_ids_by_puuid("some-puuid")
        self.assertEqual(res, {"status": "recovered_5xx"})
        self.assertEqual(mock_request.call_count, 3)

        # Sleeps: attempt 0: backoff_factor * 2^0 = 0.1 * 1 = 0.1
        # attempt 1: backoff_factor * 2^1 = 0.1 * 2 = 0.2
        mock_sleep.assert_any_call(0.1)
        mock_sleep.assert_any_call(0.2)

    @patch("requests.Session.request")
    def test_request_exhaustion(self, mock_request):
        # Always yields 500, should raise HTTPError after max_retries (2 retries, total 3 attempts)
        mock_500 = MagicMock()
        mock_500.status_code = 500
        mock_500.raise_for_status.side_effect = requests.HTTPError("Server Error")
        mock_request.return_value = mock_500

        with self.assertRaises(requests.HTTPError):
            self.client.get_latest_version()

        self.assertEqual(mock_request.call_count, 3)


if __name__ == "__main__":
    unittest.main()
