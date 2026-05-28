"""
test_buffer.py
~~~~~~~~~~~~~~
Unit tests for :class:`~pipeline.buffer.ParquetBuffer`.

Tests cover:
- Adding records injects ``platform`` field correctly.
- ``is_empty`` / ``record_count`` reflect state accurately.
- ``flush()`` returns valid Parquet bytes readable by PyArrow.
- ``flush()`` resets the buffer to an empty state.
- ``flush()`` on an empty buffer returns ``None``.
- ``should_flush`` triggers correctly when threshold is met.
- The injected ``platform`` value appears in the flushed Parquet data.
"""

from __future__ import annotations

import os
import sys

# Allow importing from root config and jobs/fetch_users/pipeline
_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "jobs", "fetch_users"))

import io
import unittest

import pyarrow.parquet as pq
from pipeline.buffer import ParquetBuffer


def _make_record(
    puuid: str = "abc123",
    tier: str = "DIAMOND",
    rank: str = "I",
    lp: int = 75,
    wins: int = 50,
    losses: int = 40,
) -> dict:
    """Return a minimal record matching the Riot API schema."""
    return {
        "leagueId": "league-id-" + puuid,
        "queueType": "RANKED_SOLO_5x5",
        "tier": tier,
        "rank": rank,
        "puuid": puuid,
        "leaguePoints": lp,
        "wins": wins,
        "losses": losses,
        "veteran": False,
        "inactive": False,
        "freshBlood": False,
        "hotStreak": False,
    }


class TestParquetBufferEmpty(unittest.TestCase):
    def setUp(self) -> None:
        self.buf = ParquetBuffer(platform="kr")

    def test_is_empty_initially(self) -> None:
        self.assertTrue(self.buf.is_empty)

    def test_record_count_initially_zero(self) -> None:
        self.assertEqual(self.buf.record_count, 0)

    def test_flush_empty_returns_none(self) -> None:
        self.assertIsNone(self.buf.flush())

    def test_should_flush_false_when_empty(self) -> None:
        self.assertFalse(self.buf.should_flush)


class TestParquetBufferAdd(unittest.TestCase):
    def setUp(self) -> None:
        self.buf = ParquetBuffer(platform="euw1")

    def test_add_increments_record_count(self) -> None:
        records = [_make_record(puuid=str(i)) for i in range(10)]
        self.buf.add(records)
        self.assertEqual(self.buf.record_count, 10)

    def test_is_not_empty_after_add(self) -> None:
        self.buf.add([_make_record()])
        self.assertFalse(self.buf.is_empty)

    def test_platform_injected_into_records(self) -> None:
        rec = _make_record(puuid="test-puuid")
        self.buf.add([rec])
        # The dict is mutated in place
        self.assertEqual(rec["platform"], "euw1")

    def test_multiple_add_calls_accumulate(self) -> None:
        self.buf.add([_make_record(puuid="a")])
        self.buf.add([_make_record(puuid="b"), _make_record(puuid="c")])
        self.assertEqual(self.buf.record_count, 3)


class TestParquetBufferFlush(unittest.TestCase):
    def setUp(self) -> None:
        self.buf = ParquetBuffer(platform="na1", compression="snappy")
        records = [_make_record(puuid=str(i)) for i in range(20)]
        self.buf.add(records)

    def test_flush_returns_bytes(self) -> None:
        result = self.buf.flush()
        self.assertIsInstance(result, bytes)
        self.assertGreater(len(result), 0)

    def test_flush_produces_valid_parquet(self) -> None:
        result = self.buf.flush()
        assert result is not None
        table = pq.read_table(io.BytesIO(result))
        self.assertEqual(table.num_rows, 20)

    def test_flush_contains_platform_column(self) -> None:
        result = self.buf.flush()
        assert result is not None
        table = pq.read_table(io.BytesIO(result))
        self.assertIn("platform", table.schema.names)
        platforms = table.column("platform").to_pylist()
        self.assertTrue(all(p == "na1" for p in platforms))

    def test_flush_resets_buffer(self) -> None:
        self.buf.flush()
        self.assertTrue(self.buf.is_empty)
        self.assertEqual(self.buf.record_count, 0)

    def test_flush_second_time_returns_none(self) -> None:
        self.buf.flush()
        self.assertIsNone(self.buf.flush())


class TestParquetBufferThreshold(unittest.TestCase):
    def test_should_flush_when_threshold_met(self) -> None:
        # Set a very low threshold (1 byte) so any data triggers it
        buf = ParquetBuffer(platform="kr", flush_threshold_bytes=1)

        # Add enough records to exceed _SIZE_CHECK_INTERVAL and trigger recompute
        records = [_make_record(puuid=str(i)) for i in range(600)]
        buf.add(records)

        self.assertTrue(buf.should_flush)

    def test_should_not_flush_below_threshold(self) -> None:
        # Very large threshold — will never be hit with a small dataset
        buf = ParquetBuffer(
            platform="kr", flush_threshold_bytes=512 * 1024 * 1024
        )
        buf.add([_make_record(puuid="x")])
        self.assertFalse(buf.should_flush)


if __name__ == "__main__":
    unittest.main()
