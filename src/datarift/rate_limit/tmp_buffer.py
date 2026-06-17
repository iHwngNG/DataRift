"""Temporary buffer module for rate-limit scenarios.

Handles flushing in-flight PyArrow Table data to GCS when rate limits
are hit, and restoring that data when the job resumes.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.parquet as papq

from datarift.gcs.paths import tmp_buffer_path


__all__ = [
    "TmpBufferNotFoundError",
    "delete",
    "flush",
    "restore",
]


class TmpBufferNotFoundError(Exception):
    """Raised when a temporary buffer file does not exist in GCS."""

    def __init__(
        self,
        job_type: str,
        job_id: str,
        thread_id: int,
    ) -> None:
        """Initialize the error with buffer identifiers.

        Args:
            job_type: Type of job.
            job_id: Unique job identifier.
            thread_id: Thread ID.

        """
        self.job_type = job_type
        self.job_id = job_id
        self.thread_id = thread_id
        super().__init__(
            f"Temporary buffer not found for job_type={job_type}, "
            f"job_id={job_id}, thread_id={thread_id}"
        )


def flush(
    gcs_client: Any,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
    table: pa.Table,
) -> str:
    """Write a PyArrow Table to GCS as a temporary buffer file.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name (with or without gs:// prefix).
        job_type: Type of job (e.g., "job_b", "job_c").
        job_id: Unique job identifier.
        thread_id: Thread ID that was buffering data.
        table: PyArrow Table to flush to GCS.

    Returns:
        GCS path of the saved buffer file.

    """
    gcs_path = tmp_buffer_path(job_type, job_id, thread_id)
    clean_bucket = bucket.removeprefix("gs://")

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        papq.write_table(table, tmp_path)  # type: ignore[no-untyped-call]

        bucket_obj = gcs_client.bucket(clean_bucket)
        blob = bucket_obj.blob(gcs_path)
        blob.upload_from_filename(
            tmp_path, content_type="application/octet-stream"
        )

        return gcs_path
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def restore(
    gcs_client: Any,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
) -> pa.Table:
    """Read a temporary buffer file from GCS back into a PyArrow Table.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name (with or without gs:// prefix).
        job_type: Type of job (e.g., "job_b", "job_c").
        job_id: Unique job identifier.
        thread_id: Thread ID that was buffering data.

    Returns:
        PyArrow Table restored from the buffer file.

    Raises:
        TmpBufferNotFoundError: If buffer file does not exist in GCS.

    """
    gcs_path = tmp_buffer_path(job_type, job_id, thread_id)
    clean_bucket = bucket.removeprefix("gs://")

    bucket_obj = gcs_client.bucket(clean_bucket)
    blob = bucket_obj.blob(gcs_path)

    if not blob.exists():
        raise TmpBufferNotFoundError(job_type, job_id, thread_id)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        blob.download_to_filename(tmp_path)
        table = papq.read_table(tmp_path)  # type: ignore[no-untyped-call]
        return table
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def delete(
    gcs_client: Any,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
) -> bool:
    """Delete a temporary buffer file from GCS.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name (with or without gs:// prefix).
        job_type: Type of job (e.g., "job_b", "job_c").
        job_id: Unique job identifier.
        thread_id: Thread ID that was buffering data.

    Returns:
        True if deleted, False if didn't exist.

    """
    gcs_path = tmp_buffer_path(job_type, job_id, thread_id)
    clean_bucket = bucket.removeprefix("gs://")

    bucket_obj = gcs_client.bucket(clean_bucket)
    blob = bucket_obj.blob(gcs_path)

    if blob.exists():
        blob.delete()
        return True
    return False
