"""Threaded queue worker for parallel job processing with rate-limit handling.

This module provides a thread-pool based queue processor that handles work items
in parallel, manages per-thread buffers, and implements a graceful shutdown
sequence when all threads encounter rate limiting from external APIs.

The design supports:
- Parallel processing of work items across multiple threads
- Per-thread ParquetBuffer instances for in-memory accumulation
- Rate-limit detection: when ALL active threads hit RateLimitError, trigger
  a checkpoint-and-resume sequence
- Resume mode: restore state from checkpoints to continue from where left off
- Mock-friendly design: all external dependencies passed as function parameters
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from datarift.riot_client.client import RateLimitError


if TYPE_CHECKING:
    from datarift.parquet.buffer import ParquetBuffer


__all__ = [
    "JobContext",
    "ThreadedQueueResult",
    "run_threaded",
]


POISON_PILL = object()


@dataclass
class JobContext:
    """Context passed to every worker thread.

    Attributes:
        job_id: Unique identifier for this job execution.
        job_type: Type of job (e.g., "b" or "c").
        shard_id: Shard index this worker is processing.

    """

    job_id: str
    job_type: str
    shard_id: int


class ThreadedQueueResult(Enum):
    """Result of the threaded queue execution.

    Attributes:
        COMPLETED: All items processed normally.
        RATE_LIMITED: All threads hit rate limit, shutdown sequence triggered.

    """

    COMPLETED = "COMPLETED"
    RATE_LIMITED = "RATE_LIMITED"


@dataclass
class _ThreadState:
    """Internal state for tracking thread progress and errors."""

    thread_id: int
    rate_limited: bool = False
    in_flight_item: Any = None
    rate_limit_error: RateLimitError | None = None


def run_threaded(
    work_items: list[Any],
    context: JobContext,
    thread_count: int,
    worker_fn: Callable[[Any, int, JobContext, ParquetBuffer], None],
    buffer_factory_fn: Callable[[int, JobContext], ParquetBuffer],
    checkpoint_save_fn: Callable[..., str],
    scheduler_retry_fn: Callable[..., Any],
    gcs_client: Any,
    gcs_bucket: str,
    tmp_buffer_flush_fn: Callable[..., str],
    tmp_buffer_restore_fn: Callable[..., pa.Table],
    checkpoint_load_fn: Callable[..., Any],
    checkpoint_exists_fn: Callable[..., bool],
    resume_mode: bool = False,
    parent_job_id: str | None = None,
    max_retry_after: float = 120.0,
) -> ThreadedQueueResult:
    """Run work items through a thread pool with rate-limit handling.

    Distributes work items across multiple threads, each with its own ParquetBuffer.
    When all active threads encounter RateLimitError, triggers a graceful shutdown
    sequence that saves checkpoints and schedules a retry.

    Args:
        work_items: Items to process (e.g., PUUID records, match IDs).
        context: Job context with job_id, job_type, and shard_id.
        thread_count: Number of worker threads to spawn.
        worker_fn: Function to call for each work item.
            Signature: (item, thread_id, context, buffer) -> None
            The buffer is a ParquetBuffer instance that the worker should use
            to accumulate records. It may contain restored data from a previous
            run in resume mode.
        buffer_factory_fn: Creates a new ParquetBuffer for each thread.
            Signature: (thread_id, context) -> ParquetBuffer
        checkpoint_save_fn: Saves checkpoint data to GCS.
            Signature: (gcs_client, bucket, job_type, job_id, thread_id, data) -> str
        scheduler_retry_fn: Schedules a Cloud Tasks retry.
            Signature: (...) -> RetryTaskScheduled
        gcs_client: Authenticated GCS client.
        gcs_bucket: GCS bucket name (with or without gs:// prefix).
        tmp_buffer_flush_fn: Flushes ParquetBuffer data to GCS tmp.
            Signature: (gcs_client, bucket, job_type, job_id, thread_id, table) -> str
        tmp_buffer_restore_fn: Restores PyArrow Table from GCS tmp.
            Signature: (gcs_client, bucket, job_type, job_id, thread_id) -> pa.Table
        checkpoint_load_fn: Loads checkpoint data from GCS.
            Signature: (gcs_client, bucket, job_type, job_id, thread_id) -> CheckpointData
        checkpoint_exists_fn: Checks if checkpoint exists in GCS.
            Signature: (gcs_client, bucket, job_type, job_id, thread_id) -> bool
        resume_mode: If True, restore state from parent_job_id checkpoints.
        parent_job_id: Job ID to restore checkpoints from (in resume mode).
        max_retry_after: Maximum retry-after seconds to use (default 120.0).

    Returns:
        ThreadedQueueResult indicating completion status.

    Raises:
        Exception: Any non-RateLimitError exception from worker_fn bubbles up.
        RateLimitError: Never raised; always converted to RATE_LIMITED return.

    """
    work_queue: queue.Queue[Any] = queue.Queue()

    thread_states: dict[int, _ThreadState] = {}
    shutdown_event = threading.Event()
    state_lock = threading.Lock()
    thread_exception: BaseException | None = None
    rate_limited_count = 0
    processed_count = 0
    total_items = len(work_items)

    def get_thread_state(thread_id: int) -> _ThreadState:
        """Get or create thread state."""
        with state_lock:
            if thread_id not in thread_states:
                thread_states[thread_id] = _ThreadState(thread_id=thread_id)
            return thread_states[thread_id]

    restored_tables: dict[int, pa.Table | None] = {}
    thread_buffers: dict[int, ParquetBuffer] = {}
    items_to_process: list[Any] = work_items

    if resume_mode and parent_job_id:
        restored_items: list[Any] = []
        for thread_id in range(thread_count):
            if checkpoint_exists_fn(
                gcs_client,
                gcs_bucket,
                context.job_type,
                parent_job_id,
                thread_id,
            ):
                checkpoint = checkpoint_load_fn(
                    gcs_client,
                    gcs_bucket,
                    context.job_type,
                    parent_job_id,
                    thread_id,
                )
                remaining = checkpoint.data.get("remaining_items", [])
                restored_items.extend(remaining)

                restored_table = tmp_buffer_restore_fn(
                    gcs_client,
                    gcs_bucket,
                    context.job_type,
                    parent_job_id,
                    thread_id,
                )
                restored_tables[thread_id] = restored_table

        # Fallback to original work_items if no checkpoints exist
        items_to_process = restored_items if restored_items else work_items
        total_items = len(items_to_process)

    result = ThreadedQueueResult.COMPLETED

    def worker(thread_id: int) -> None:
        """Worker thread function."""
        nonlocal result, processed_count, rate_limited_count, thread_exception
        state = get_thread_state(thread_id)

        if thread_id in restored_tables and restored_tables[thread_id] is not None:
            restored_tables[thread_id] = None

        buffer = buffer_factory_fn(thread_id, context)
        thread_buffers[thread_id] = buffer

        try:
            while True:
                if shutdown_event.is_set():
                    break

                try:
                    item = work_queue.get(timeout=0.01)
                except queue.Empty:
                    continue

                if item is POISON_PILL:
                    work_queue.task_done()
                    break

                state.in_flight_item = item

                try:
                    worker_fn(item, thread_id, context, buffer)
                except RateLimitError as exc:
                    with state_lock:
                        state.rate_limited = True
                        state.rate_limit_error = exc
                        rate_limited_count += 1

                    work_queue.task_done()

                    with state_lock:
                        if rate_limited_count >= thread_count:
                            shutdown_event.set()
                    continue

                state.in_flight_item = None
                work_queue.task_done()

                with state_lock:
                    processed_count += 1
                    if processed_count >= total_items:
                        shutdown_event.set()

        except BaseException as exc:
            with state_lock:
                thread_exception = exc
            shutdown_event.set()

    threads: list[threading.Thread] = []
    for thread_id in range(thread_count):
        t = threading.Thread(target=worker, args=(thread_id,), daemon=True)
        threads.append(t)
        t.start()

    for item in items_to_process:
        work_queue.put(item)

    for _ in range(thread_count):
        work_queue.put(POISON_PILL)

    for t in threads:
        t.join()

    if thread_exception is not None:
        raise thread_exception

    if shutdown_event.is_set() and rate_limited_count > 0:
        result = ThreadedQueueResult.RATE_LIMITED

        remaining_items: list[Any] = []
        while True:
            try:
                item = work_queue.get_nowait()
                if item is not POISON_PILL:
                    remaining_items.append(item)
                work_queue.task_done()
            except queue.Empty:
                break

        total_remaining = len(remaining_items)
        per_thread_share = total_remaining // thread_count if thread_count > 0 else 0
        extra = total_remaining % thread_count

        for thread_id in range(thread_count):
            state = thread_states.get(thread_id, _ThreadState(thread_id=thread_id))

            start_idx = thread_id * per_thread_share + min(thread_id, extra)
            end_idx = start_idx + per_thread_share + (1 if thread_id < extra else 0)

            thread_remaining = remaining_items[start_idx:end_idx]

            if state.in_flight_item is not None:
                thread_remaining.insert(0, state.in_flight_item)

            tmp_parquet_path = ""

            if thread_id in thread_buffers:
                buffer = thread_buffers[thread_id]
                table = buffer.flush()
                if isinstance(table, pa.Table) and table.num_rows > 0:
                    tmp_parquet_path = tmp_buffer_flush_fn(
                        gcs_client,
                        gcs_bucket,
                        context.job_type,
                        context.job_id,
                        thread_id,
                        table,
                    )
            elif (
                thread_id in restored_tables and restored_tables[thread_id] is not None
            ):
                table_to_flush = restored_tables[thread_id]
                if table_to_flush is not None and table_to_flush.num_rows > 0:
                    tmp_parquet_path = tmp_buffer_flush_fn(
                        gcs_client,
                        gcs_bucket,
                        context.job_type,
                        context.job_id,
                        thread_id,
                        table_to_flush,
                    )

            checkpoint_data = {
                "remaining_items": thread_remaining,
                "tmp_parquet_path": tmp_parquet_path,
                "retry_after_seconds": max_retry_after,
                "shard_id": context.shard_id,
            }
            checkpoint_save_fn(
                gcs_client,
                gcs_bucket,
                context.job_type,
                context.job_id,
                thread_id,
                checkpoint_data=checkpoint_data,
            )

        scheduler_retry_fn(
            project_id="",
            location="",
            job_name=context.job_id,
            retry_after=int(max_retry_after),
        )
    else:
        result = ThreadedQueueResult.COMPLETED
        for _thread_id, buffer in thread_buffers.items():
            buffer.flush()

    return result
