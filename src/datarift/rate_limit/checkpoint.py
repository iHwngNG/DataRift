"""Rate-limit checkpoint module for saving/resuming job state."""

from __future__ import annotations


__all__ = [
    "CheckpointData",
    "CheckpointNotFoundError",
    "checkpoint_path",
    "delete",
    "exists",
    "load",
    "save",
]

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from datarift.gcs.paths import checkpoint_path


@dataclass
class CheckpointData:
    """Structured data for rate-limit checkpoint.

    Attributes:
        job_type: Type of job (e.g., "job_b", "job_c").
        job_id: Unique job identifier.
        thread_id: Thread ID that was checkpointed.
        checkpoint_timestamp: ISO format timestamp when checkpoint was created.
        data: Arbitrary checkpoint data (e.g., API cursor, last processed item).

    """

    job_type: str
    job_id: str
    thread_id: int
    checkpoint_timestamp: str
    data: dict[str, Any]


class CheckpointNotFoundError(Exception):
    """Raised when a checkpoint does not exist in GCS."""

    def __init__(
        self,
        job_type: str,
        job_id: str,
        thread_id: int,
    ) -> None:
        """Initialize the error with checkpoint identifiers.

        Args:
            job_type: Type of job.
            job_id: Unique job identifier.
            thread_id: Thread ID.

        """
        self.job_type = job_type
        self.job_id = job_id
        self.thread_id = thread_id
        super().__init__(
            f"Checkpoint not found for job_type={job_type}, "
            f"job_id={job_id}, thread_id={thread_id}"
        )


def save(
    gcs_client: Any,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
    data: dict[str, Any],
) -> str:
    """Save checkpoint data to GCS.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name (with or without gs:// prefix).
        job_type: Type of job (e.g., "job_b", "job_c").
        job_id: Unique job identifier.
        thread_id: Thread ID that was checkpointed.
        data: Arbitrary checkpoint data (e.g., API cursor, last processed item).

    Returns:
        GCS path of the saved checkpoint.

    """
    checkpoint = CheckpointData(
        job_type=job_type,
        job_id=job_id,
        thread_id=thread_id,
        checkpoint_timestamp=datetime.now(UTC).isoformat(),
        data=data,
    )

    gcs_path = checkpoint_path(job_type, job_id, thread_id)
    clean_bucket = bucket.removeprefix("gs://")

    bucket_obj = gcs_client.bucket(clean_bucket)
    blob = bucket_obj.blob(gcs_path)
    blob.upload_from_string(
        json.dumps(asdict(checkpoint), indent=2),
        content_type="application/json",
    )

    return gcs_path


def load(
    gcs_client: Any,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
) -> CheckpointData:
    """Load checkpoint data from GCS.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name (with or without gs:// prefix).
        job_type: Type of job (e.g., "job_b", "job_c").
        job_id: Unique job identifier.
        thread_id: Thread ID that was checkpointed.

    Returns:
        CheckpointData with restored state.

    Raises:
        CheckpointNotFoundError: If checkpoint does not exist in GCS.

    """
    gcs_path = checkpoint_path(job_type, job_id, thread_id)
    clean_bucket = bucket.removeprefix("gs://")

    bucket_obj = gcs_client.bucket(clean_bucket)
    blob = bucket_obj.blob(gcs_path)

    if not blob.exists():
        raise CheckpointNotFoundError(job_type, job_id, thread_id)

    content = blob.download_as_text()
    raw = json.loads(content)

    return CheckpointData(
        job_type=raw["job_type"],
        job_id=raw["job_id"],
        thread_id=raw["thread_id"],
        checkpoint_timestamp=raw["checkpoint_timestamp"],
        data=raw["data"],
    )


def delete(
    gcs_client: Any,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
) -> bool:
    """Delete checkpoint from GCS.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name (with or without gs:// prefix).
        job_type: Type of job (e.g., "job_b", "job_c").
        job_id: Unique job identifier.
        thread_id: Thread ID that was checkpointed.

    Returns:
        True if deleted, False if didn't exist.

    """
    gcs_path = checkpoint_path(job_type, job_id, thread_id)
    clean_bucket = bucket.removeprefix("gs://")

    bucket_obj = gcs_client.bucket(clean_bucket)
    blob = bucket_obj.blob(gcs_path)

    if blob.exists():
        blob.delete()
        return True
    return False


def exists(
    gcs_client: Any,
    bucket: str,
    job_type: str,
    job_id: str,
    thread_id: int,
) -> bool:
    """Check if checkpoint exists in GCS.

    Args:
        gcs_client: Authenticated GCS client.
        bucket: GCS bucket name (with or without gs:// prefix).
        job_type: Type of job (e.g., "job_b", "job_c").
        job_id: Unique job identifier.
        thread_id: Thread ID that was checkpointed.

    Returns:
        True if checkpoint exists, False otherwise.

    """
    gcs_path = checkpoint_path(job_type, job_id, thread_id)
    clean_bucket = bucket.removeprefix("gs://")

    bucket_obj = gcs_client.bucket(clean_bucket)
    blob = bucket_obj.blob(gcs_path)

    exists_result: bool = blob.exists()
    return exists_result
