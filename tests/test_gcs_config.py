import unittest
import sys
from pathlib import Path

# Add project root to sys.path to allow execution from any context
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.gcs_config import GCSConfig

class TestGCSConfig(unittest.TestCase):
    def setUp(self):
        # Create a basic GCSConfig instance for testing
        self.config = GCSConfig(
            bronze_bucket="test-bronze",
            silver_bucket="test-silver",
            league_prefix="lol/league",
            match_prefix="lol/match",
            static_prefix="lol",
            upload_chunk_bytes=8388608,
        )

    def test_build_match_prefix(self):
        """Test build_match_prefix generates correct prefix."""
        # Check if the method exists, if not this will raise an AttributeError in the test (which is expected if missing)
        result = self.config.build_match_prefix(
            region="asia",
            platform="kr",
            year=2024,
            month=5,
            day=15
        )
        self.assertEqual(result, "lol/match/asia/kr/2024/05/15/")
        
        # Test zero-padding
        result2 = self.config.build_match_prefix(
            region="americas",
            platform="na1",
            year=2023,
            month=1,
            day=5
        )
        self.assertEqual(result2, "lol/match/americas/na1/2023/01/05/")

    def test_build_league_prefix_standard_tier(self):
        """Test build_league_prefix for standard tiers generates correct prefix."""
        result = self.config.build_league_prefix(
            tier="DIAMOND",
            rank="I"
        )
        self.assertEqual(result, "lol/league/DIAMOND/I/")
        
        result2 = self.config.build_league_prefix(
            tier="GOLD",
            rank="IV"
        )
        self.assertEqual(result2, "lol/league/GOLD/IV/")

    def test_build_league_prefix_apex_tier(self):
        """Test build_league_prefix for apex tiers generates correct prefix."""
        result = self.config.build_league_prefix(
            tier="MASTER",
            league_points=500
        )
        self.assertEqual(result, "lol/league/MASTER/500/")
        
        result2 = self.config.build_league_prefix(
            tier="CHALLENGER",
            league_points=1200
        )
        self.assertEqual(result2, "lol/league/CHALLENGER/1200/")

if __name__ == "__main__":
    unittest.main()
