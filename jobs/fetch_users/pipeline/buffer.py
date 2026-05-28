"""
buffer.py
~~~~~~~~~
In-RAM Parquet accumulation buffer for the ``fetch_users`` job.

Design
------
Records are appended as Python dicts and stored as a list of
:class:`pyarrow.RecordBatch` objects.  The estimated serialised size is
checked via a lightweight in-memory serialisation call.  When the caller
decides to flush, the buffer is converted to a single :class:`pyarrow.Table`,
serialised to Parquet bytes, and returned — the internal state is then
cleared.

Thread safety
-------------
This buffer is **not** thread-safe.  The ``fetch_users`` job runs
single-threaded, so no locking is required.

Schema
------
Inferred from the first batch added.  Subsequent batches must be
schema-compatible (Riot's API response is stable for this endpoint).

Parquet schema for ``/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{div}``::

    leagueId       : string  (nullable)
    queueType      : string
    tier           : string
    rank           : string
    puuid          : string
    leaguePoints   : int64
    wins           : int64
    losses         : int64
    veteran        : bool
    inactive       : bool
    freshBlood     : bool
    hotStreak      : bool
    platform       : string  (added by this job — not in the raw API payload)
"""

from __future__ import annotations

import io
import logging
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Explicit schema — avoids inference surprises across Riot API versions
# ---------------------------------------------------------------------------
_SCHEMA = pa.schema(
    [
        pa.field("leagueId", pa.string(), nullable=True),
        pa.field("queueType", pa.string()),
        pa.field("tier", pa.string()),
        pa.field("rank", pa.string()),
        pa.field("puuid", pa.string()),
        pa.field("leaguePoints", pa.int64()),
        pa.field("wins", pa.int64()),
        pa.field("losses", pa.int64()),
        pa.field("veteran", pa.bool_()),
        pa.field("inactive", pa.bool_()),
        pa.field("freshBlood", pa.bool_()),
        pa.field("hotStreak", pa.bool_()),
        pa.field("platform", pa.string()),
    ]
)

# How often to recompute the estimated size (every N records added)
_SIZE_CHECK_INTERVAL = 500


class ParquetBuffer:
    """Accumulates Riot league-entry records and flushes them as Parquet bytes.

    Args:
        platform: Platform slug injected into every record as the ``platform``
            column (e.g. ``"kr"``).
        compression: Parquet compression codec (``"snappy"``, ``"zstd"``, …).
        flush_threshold_bytes: Byte size at which the caller should flush.
            Checked lazily every ``_SIZE_CHECK_INTERVAL`` records.
    """

    def __init__(
        self,
        platform: str,
        compression: str = "snappy",
        flush_threshold_bytes: int = 128 * 1024 * 1024,
    ) -> None:
        self._platform = platform
        self._compression = compression
        self._threshold = flush_threshold_bytes

        self._records: list[dict] = []
        self._record_count_since_size_check = 0
        self._estimated_bytes: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, records: list[dict]) -> None:
        """Append a page of records to the buffer.

        The ``platform`` field is injected into each record.

        Args:
            records: Raw dicts from the Riot API response.
        """
        for rec in records:
            rec["platform"] = self._platform
            self._records.append(rec)

        self._record_count_since_size_check += len(records)

        # Recompute size estimate periodically to avoid per-record overhead
        if self._record_count_since_size_check >= _SIZE_CHECK_INTERVAL:
            self._estimated_bytes = self._compute_size()
            self._record_count_since_size_check = 0
            logger.debug(
                "Buffer size estimate: %.2f MB (%d records).",
                self._estimated_bytes / 1024 / 1024,
                len(self._records),
            )

    @property
    def size_bytes(self) -> int:
        """Current estimated serialised Parquet size in bytes.

        If fewer than ``_SIZE_CHECK_INTERVAL`` records have been added since
        the last check, the cached estimate is returned.
        """
        return self._estimated_bytes

    @property
    def record_count(self) -> int:
        """Number of records currently held in the buffer."""
        return len(self._records)

    @property
    def is_empty(self) -> bool:
        """``True`` if the buffer holds no records."""
        return len(self._records) == 0

    @property
    def should_flush(self) -> bool:
        """``True`` when the estimated size meets or exceeds the threshold."""
        return self._estimated_bytes >= self._threshold

    def flush(self) -> Optional[bytes]:
        """Serialise all buffered records to Parquet bytes and clear the buffer.

        Returns:
            Parquet file as ``bytes``, or ``None`` if the buffer is empty.
        """
        if self.is_empty:
            return None

        table = pa.Table.from_pylist(self._records, schema=_SCHEMA)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=self._compression)
        result = buf.getvalue()

        logger.info(
            "Flushed %d records → %.2f MB Parquet (%s).",
            len(self._records),
            len(result) / 1024 / 1024,
            self._compression,
        )

        # Reset state
        self._records = []
        self._estimated_bytes = 0
        self._record_count_since_size_check = 0

        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_size(self) -> int:
        """Serialise the current records and return byte length.

        This is the only expensive operation; it is called at most once per
        ``_SIZE_CHECK_INTERVAL`` records.
        """
        if not self._records:
            return 0
        table = pa.Table.from_pylist(self._records, schema=_SCHEMA)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=self._compression)
        return len(buf.getvalue())
