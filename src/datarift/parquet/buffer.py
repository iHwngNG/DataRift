"""Parquet buffer module for efficient in-memory data aggregation."""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.parquet as papq


class StreamingParquetWriter:
    """Memory-efficient Parquet writer using streaming RecordBatch writes.

    Converts each record to a RecordBatch immediately and writes to disk,
    avoiding holding all records as Python dicts in memory. This reduces
    peak memory usage from 2-3x the data size to approximately 1x.

    The writer buffers records in memory until a threshold is reached,
    then writes a row group to the temp file. On finalize(), the temp
    file is uploaded to GCS and deleted.
    """

    def __init__(
        self,
        gcs_client: Any,
        gcs_bucket: str,
        gcs_path: str,
        row_group_size: int = 1000,
        flush_threshold_bytes: int = 0,
        schema: pa.Schema | None = None,
    ) -> None:
        """Initialize streaming writer.

        Args:
            gcs_client: Authenticated GCS client.
            gcs_bucket: GCS bucket name (with or without gs:// prefix).
            gcs_path: GCS object path (e.g., 'data/output.parquet').
            row_group_size: Number of rows per row group before flushing
                to disk. Default 1000.
            flush_threshold_bytes: Bytes threshold for flushing. When buffer
                size reaches this value, flush is triggered. Default 0 means
                no bytes-based flush (only row-based).
            schema: Optional PyArrow schema for type inference. If not
                provided, inferred from first record.

        """
        import uuid
        from datetime import datetime

        self._gcs_client = gcs_client
        self._gcs_bucket = gcs_bucket.removeprefix("gs://")
        self._gcs_path = gcs_path.lstrip("/")
        self._row_group_size = row_group_size
        self._flush_threshold_bytes = flush_threshold_bytes
        self._schema = schema

        self._temp_file = tempfile.NamedTemporaryFile(
            suffix=".parquet", delete=False
        )
        self._temp_path = self._temp_file.name
        self._temp_file.close()

        self._writer: papq.ParquetWriter | None = None
        self._buffer: list[dict[str, Any]] = []
        self._buffer_size_bytes = 0
        self._buffer_parquet_size_bytes = 0
        self._buffer_json_count = 0
        self._total_rows = 0
        self._is_finalized = False

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_suffix = uuid.uuid4().hex[:8]
        base_path = (
            self._gcs_path.rsplit("/", 1)[0] if "/" in self._gcs_path else ""
        )
        filename = self._gcs_path.split("/")[-1]
        self._final_path = (
            f"{base_path}/{timestamp}_{unique_suffix}_{filename}".lstrip("/")
        )

    def _ensure_writer(self, schema: pa.Schema) -> None:
        """Ensure ParquetWriter is initialized with the given schema."""
        if self._writer is None:
            self._writer = papq.ParquetWriter(self._temp_path, schema)  # type: ignore[no-untyped-call]

    def _measure_parquet_size(self, batch: pa.RecordBatch) -> int:
        """Measure parquet compressed size for a record batch.

        Writes the batch to a temporary buffer to measure compressed size.

        Args:
            batch: RecordBatch to measure.

        Returns:
            Compressed size in bytes.

        """
        import io

        buffer = io.BytesIO()
        temp_writer = papq.ParquetWriter(  # type: ignore[no-untyped-call]
            buffer, batch.schema, compression="snappy"
        )
        temp_writer.write_batch(batch)  # type: ignore[no-untyped-call]
        temp_writer.close()  # type: ignore[no-untyped-call]
        size = buffer.tell()
        buffer.close()
        return size

    def _flush_buffer(self) -> None:
        """Flush buffered records to disk as a row group."""
        if not self._buffer:
            return

        records = self._buffer
        self._buffer = []
        self._buffer_size_bytes = 0
        self._buffer_parquet_size_bytes = 0
        self._buffer_json_count = 0

        batch = pa.RecordBatch.from_pylist(records, schema=self._schema)
        self._ensure_writer(batch.schema)

        if self._schema is None:
            self._schema = batch.schema

        if self._writer is not None:
            self._writer.write_batch(batch)  # type: ignore[no-untyped-call]
        self._total_rows += batch.num_rows

        del records
        del batch

    def add(self, record: dict[str, Any], estimated_size: int = 100) -> None:
        """Add a single record to the writer.

        Record is converted to RecordBatch and written to disk immediately
        if buffer threshold is reached (by row count or bytes).

        Args:
            record: Dictionary representing a single row.
            estimated_size: Estimated memory size of the record in bytes.
                NOTE: This is NOT used for flush threshold tracking.

        """
        if self._is_finalized:
            raise RuntimeError("Cannot add records after finalization")

        # Measure parquet compressed size for this record
        record_batch = pa.RecordBatch.from_pylist(
            [record], schema=self._schema
        )
        parquet_size = self._measure_parquet_size(record_batch)

        self._buffer.append(record)
        self._buffer_json_count += 1
        self._buffer_parquet_size_bytes += parquet_size

        del record_batch

        # Flush if row threshold reached OR bytes threshold reached
        should_flush = len(self._buffer) >= self._row_group_size
        if self._flush_threshold_bytes > 0:
            should_flush = (
                should_flush
                or self._buffer_parquet_size_bytes
                >= self._flush_threshold_bytes
            )

        if should_flush:
            self._flush_buffer()

    def add_batch(self, records: list[dict[str, Any]]) -> None:
        """Add multiple records as a single batch.

        More efficient than adding one at a time when records are already
        available as a list.

        Args:
            records: List of dictionaries representing rows.

        """
        if self._is_finalized:
            raise RuntimeError("Cannot add records after finalization")

        if not records:
            return

        batch = pa.RecordBatch.from_pylist(records, schema=self._schema)
        self._ensure_writer(batch.schema)

        if self._schema is None:
            self._schema = batch.schema

        if self._writer is not None:
            self._writer.write_batch(batch)  # type: ignore[no-untyped-call]
        self._total_rows += batch.num_rows

        del records
        del batch

    def should_flush(self) -> bool:
        """Check if buffer has records that could be flushed."""
        return len(self._buffer) > 0

    def flush(self) -> None:
        """Flush any buffered records to disk."""
        if self._is_finalized:
            raise RuntimeError("Cannot flush after finalization")
        self._flush_buffer()

    def finalize(self) -> str:
        """Finalize and upload to GCS.

        Flushes any remaining buffered records, closes the ParquetWriter,
        uploads the temp file to GCS, and deletes the temp file.

        Returns:
            Full GCS path (gs://bucket/path) of the uploaded file.

        Raises:
            RuntimeError: If called multiple times.
            Exception: Re-raises any GCS upload errors after cleanup.

        """
        if self._is_finalized:
            raise RuntimeError("Already finalized")

        self._is_finalized = True

        try:
            self._flush_buffer()

            if self._writer is not None:
                self._writer.close()  # type: ignore[no-untyped-call]
                self._writer = None

            if self._total_rows == 0:
                self._cleanup_temp_file()
                return f"gs://{self._gcs_bucket}/{self._final_path}"

            self._upload_to_gcs()
            self._cleanup_temp_file()

            return f"gs://{self._gcs_bucket}/{self._final_path}"

        except Exception:
            self._cleanup_temp_file()
            raise

    def _upload_to_gcs(self) -> None:
        """Upload temp file to GCS."""
        bucket = self._gcs_client.bucket(self._gcs_bucket)
        blob = bucket.blob(self._final_path)
        blob.upload_from_filename(self._temp_path)

    def _cleanup_temp_file(self) -> None:
        """Delete temp file if it exists."""
        if self._temp_path and os.path.exists(self._temp_path):
            try:
                os.unlink(self._temp_path)
            except OSError:
                pass
            self._temp_path = ""

    def __enter__(self) -> StreamingParquetWriter:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit context manager, finalizing or cleaning up on exception."""
        if exc_type is not None:
            self._cleanup_temp_file()
        else:
            self.finalize()

    @property
    def total_rows(self) -> int:
        """Total number of rows written (flushed to disk)."""
        return self._total_rows

    @property
    def buffered_rows(self) -> int:
        """Number of rows currently buffered in memory."""
        return len(self._buffer)

    @property
    def total_record_count(self) -> int:
        """Total rows including both flushed and buffered."""
        return self._total_rows + len(self._buffer)


class ParquetBuffer:
    """Buffer for accumulating records before flushing to Parquet.

    Supports two modes:
    - Default (streaming=False): Accumulates records in memory, converts to
      PyArrow Table on flush().
    - Streaming (streaming=True): Converts each record to RecordBatch
      immediately and writes to temp file via StreamingParquetWriter.

    Use streaming=True for large records (e.g., JSON ~25MB) to reduce peak
    memory usage from 2-3x to approximately 1x the data size.
    """

    def __init__(
        self,
        flush_threshold_bytes: int,
        streaming: bool = False,
        gcs_client: Any = None,
        gcs_bucket: str = "",
        gcs_path: str = "",
        row_group_size: int = 1000,
    ) -> None:
        """Initialize buffer with size threshold.

        Args:
            flush_threshold_bytes: Size threshold in bytes. When buffer
                reaches or exceeds this value, should_flush() returns True.
                In streaming mode, this is passed to StreamingParquetWriter
                as flush_threshold_bytes for bytes-based flushing.
            streaming: If True, use StreamingParquetWriter for memory-efficient
                writes. Requires gcs_client, gcs_bucket, and gcs_path.
            gcs_client: Authenticated GCS client. Required if streaming=True.
            gcs_bucket: GCS bucket name. Required if streaming=True.
            gcs_path: GCS object path. Required if streaming=True.
            row_group_size: Rows per row group in streaming mode. Default 1000.

        """
        self._flush_threshold_bytes = flush_threshold_bytes
        self._streaming = streaming
        self._records: list[dict[str, Any]] = []
        self._estimated_size_bytes: int = 0
        self._estimated_row_size: int = 100

        self._streaming_writer: StreamingParquetWriter | None = None
        if self._streaming:
            if gcs_client is None:
                raise ValueError("gcs_client required when streaming=True")
            self._streaming_writer = StreamingParquetWriter(
                gcs_client=gcs_client,
                gcs_bucket=gcs_bucket,
                gcs_path=gcs_path,
                row_group_size=row_group_size,
                flush_threshold_bytes=flush_threshold_bytes,
            )

    def add(
        self, record: dict[str, Any], estimated_size: int | None = None
    ) -> None:
        """Append a record to the buffer.

        Args:
            record: Dictionary representing a single row of data.
            estimated_size: Estimated memory size in bytes. If not provided,
                uses default _estimated_row_size.
                NOTE: In streaming mode, this is NOT used for flush threshold
                tracking. The parquet compressed size is tracked instead.

        """
        if estimated_size is None:
            estimated_size = self._estimated_row_size

        if self._streaming:
            # In streaming mode, pass to writer directly - don't store in _records
            # NOTE: estimated_size is NOT used for threshold tracking in streaming mode
            if self._streaming_writer is not None:
                self._streaming_writer.add(
                    record, estimated_size=estimated_size
                )
        else:
            self._records.append(record)
            self._estimated_size_bytes += estimated_size

    def should_flush(self) -> bool:
        """Check if buffer has reached the flush threshold.

        Returns:
            True if estimated size >= threshold bytes, False otherwise.

        """
        if self._streaming and self._streaming_writer is not None:
            return self._streaming_writer.should_flush()
        return self._estimated_size_bytes >= self._flush_threshold_bytes

    def flush(self) -> pa.Table | str:
        """Convert buffer contents to PyArrow Table and reset.

        In streaming mode, finalizes the temp file and uploads to GCS,
        returning the GCS path as a string.

        Returns:
            PyArrow Table (non-streaming) or GCS path string (streaming).

        """
        if self._streaming and self._streaming_writer is not None:
            return self._streaming_writer.finalize()

        if not self._records:
            return pa.table({})

        table = pa.Table.from_pylist(self._records)
        self._reset()
        return table

    def _reset(self) -> None:
        """Clear buffer state after flush."""
        self._records = []
        self._estimated_size_bytes = 0

    @property
    def record_count(self) -> int:
        """Number of records currently in buffer."""
        if self._streaming and self._streaming_writer is not None:
            return self._streaming_writer.total_record_count
        return len(self._records)

    @property
    def estimated_size_bytes(self) -> int:
        """Current estimated buffer size in bytes."""
        if self._streaming and self._streaming_writer is not None:
            return self._streaming_writer.total_rows * self._estimated_row_size
        return self._estimated_size_bytes

    @property
    def buffered_bytes(self) -> int:
        """Current buffer size in bytes (streaming mode only)."""
        if self._streaming and self._streaming_writer is not None:
            return self._streaming_writer._buffer_size_bytes
        return self._estimated_size_bytes


class ParquetPartitionBuffer:
    """Manages multiple ParquetBuffer instances keyed by partition.

    Each unique partition key (tuple) maintains its own buffer,
    allowing partitioned data to be accumulated and flushed independently.

    Supports streaming mode for memory-efficient writes. When streaming=True,
    each partition uses its own StreamingParquetWriter for immediate disk
    writes instead of holding all records in memory.
    """

    def __init__(
        self,
        flush_threshold_bytes: int,
        streaming: bool = False,
        gcs_client: Any = None,
        gcs_bucket: str = "",
        row_group_size: int = 1000,
    ) -> None:
        """Initialize partition buffer manager.

        Args:
            flush_threshold_bytes: Default size threshold in bytes for each
                partition buffer. Applied to all partitions unless overridden.
            streaming: If True, use StreamingParquetWriter for each partition.
            gcs_client: Authenticated GCS client. Required if streaming=True.
            gcs_bucket: GCS bucket name. Required if streaming=True.
            row_group_size: Rows per row group in streaming mode. Default 1000.

        """
        self._flush_threshold_bytes = flush_threshold_bytes
        self._streaming = streaming
        self._gcs_client = gcs_client
        self._gcs_bucket = gcs_bucket
        self._row_group_size = row_group_size
        self._buffers: dict[tuple[str, ...], ParquetBuffer] = {}

    def _make_gcs_path(
        self, partition_key: tuple[str, ...], suffix: str = "data.parquet"
    ) -> str:
        """Construct GCS path from partition key."""
        key_parts = [str(k) for k in partition_key]
        return "/".join([*key_parts, suffix])

    def add(
        self, record: dict[str, Any], partition_key: tuple[str, ...]
    ) -> None:
        """Add record to the appropriate partition buffer.

        Args:
            record: Dictionary representing a single row of data.
            partition_key: Tuple key identifying the partition (e.g., (region, platform)).

        """
        if partition_key not in self._buffers:
            self._buffers[partition_key] = ParquetBuffer(
                flush_threshold_bytes=self._flush_threshold_bytes,
                streaming=self._streaming,
                gcs_client=self._gcs_client,
                gcs_bucket=self._gcs_bucket,
                gcs_path=self._make_gcs_path(partition_key),
                row_group_size=self._row_group_size,
            )
        self._buffers[partition_key].add(record)

    def should_flush(self, partition_key: tuple[str, ...]) -> bool:
        """Check if a specific partition buffer should flush.

        Args:
            partition_key: Tuple key identifying the partition.

        Returns:
            True if partition exists and its buffer should flush, False otherwise.

        """
        if partition_key not in self._buffers:
            return False
        return self._buffers[partition_key].should_flush()

    def flush_partition(
        self, partition_key: tuple[str, ...]
    ) -> pa.Table | str | None:
        """Flush a single partition buffer and return its contents.

        In streaming mode, returns the GCS path string. Otherwise,
        returns PyArrow Table or None.

        Args:
            partition_key: Tuple key identifying the partition to flush.

        Returns:
            GCS path (streaming), PyArrow Table (non-streaming), or None if
            partition doesn't exist or has no records.

        """
        if partition_key not in self._buffers:
            return None

        result = self._buffers[partition_key].flush()
        if self._streaming:
            return result
        table = result
        if isinstance(table, str) or table.num_rows == 0:
            return None
        return table

    def flush_all(
        self,
    ) -> dict[tuple[str, ...], pa.Table] | dict[tuple[str, ...], str]:
        """Flush all partition buffers.

        In streaming mode, returns dict of partition keys to GCS paths.
        Otherwise, returns dict of partition keys to PyArrow Tables.

        Returns:
            Dictionary mapping partition keys to their respective data.
            Only partitions with non-empty content are included.

        """
        result: dict[tuple[str, ...], pa.Table | str] = {}
        for partition_key in list(self._buffers.keys()):
            flushed = self._buffers[partition_key].flush()
            if self._streaming:
                if flushed:
                    result[partition_key] = flushed
            else:
                table = flushed
                if isinstance(table, str) or table.num_rows > 0:
                    result[partition_key] = table
        return result

    @property
    def partition_keys(self) -> list[tuple[str, ...]]:
        """List of active partition keys."""
        return list(self._buffers.keys())

    @property
    def partition_counts(self) -> dict[tuple[str, ...], int]:
        """Record counts per partition."""
        return {key: buf.record_count for key, buf in self._buffers.items()}

    def clear_partition(self, partition_key: tuple[str, ...]) -> None:
        """Remove a specific partition buffer.

        Args:
            partition_key: Tuple key identifying the partition to remove.

        """
        self._buffers.pop(partition_key, None)

    def clear_all(self) -> None:
        """Remove all partition buffers."""
        self._buffers.clear()
