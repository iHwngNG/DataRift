"""Rate-limit scheduler module for scheduling Cloud Tasks retries.

Schedules delayed tasks to resume jobs after rate limit cool-down periods,
adding a 2-second buffer to the Retry-After duration.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import tasks_v2


__all__ = [
    "RetryTaskError",
    "RetryTaskScheduled",
    "schedule_retry",
]


# Buffer time added to retry_after to ensure rate limit has lifted
_SCHEDULE_BUFFER_SECONDS = 2

# Default Cloud Tasks queue name
_DEFAULT_QUEUE = "datarift-rate-limit-retry"

# Default resume function endpoint pattern
_DEFAULT_ENDPOINT_PATTERN = (
    "https://{location}-{project_id}.cloudfunctions.net/datarift-resume"
)


@dataclass
class RetryTaskScheduled:
    """Result of a successful retry task scheduling.

    Attributes:
        task_name: Full name of the created Cloud Tasks task.
        schedule_time: When the task is scheduled to run (ISO format).
        job_name: The parent job ID passed to the scheduled task.
        delay_seconds: Actual delay used (Retry-After + 2 seconds).

    """

    task_name: str
    schedule_time: str
    job_name: str
    delay_seconds: int


class RetryTaskError(Exception):
    """Raised when scheduling a retry task fails."""


def schedule_retry(
    project_id: str,
    location: str,
    job_name: str,
    retry_after: int,
    queue: str = _DEFAULT_QUEUE,
    tasks_client: tasks_v2.CloudTasksClient | None = None,
    endpoint_url: str | None = None,
) -> RetryTaskScheduled:
    """Schedule a Cloud Task to resume a rate-limited job after cool-down.

    Creates an HTTP task that will be executed after ``retry_after + 2`` seconds.
    The task is configured with headers that allow the worker to resume from
    its checkpoint:

    - ``RESUME_MODE=true``
    - ``PARENT_JOB_ID=<job_name>``

    Args:
        project_id: GCP project ID hosting the Cloud Tasks queue.
        location: GCP region (e.g., "us-central1").
        job_name: Unique name/identifier of the job to resume. Passed as
            ``PARENT_JOB_ID`` to the resumed task.
        retry_after: Seconds to wait before resuming (from Retry-After header).
            The actual delay will be this value plus 2 seconds as a safety buffer.
        queue: Cloud Tasks queue name. Defaults to "datarift-rate-limit-retry".
        tasks_client: Optional CloudTasksClient instance (for testing).
        endpoint_url: Full URL of the job entrypoint. If not provided, defaults
            to the pattern ``https://{location}-{project_id}.cloudfunctions.net/datarift-resume``.
            Can also be set via the ``JOB_ENTRYPOINT_URL`` environment variable,
            which takes precedence if both are set.

    Returns:
        RetryTaskScheduled with details of the created task.

    Raises:
        RetryTaskError: If the task cannot be created.
        ValueError: If ``retry_after`` is negative or no endpoint URL can be determined.

    """
    if retry_after < 0:
        raise ValueError(
            f"retry_after must be non-negative, got {retry_after}"
        )

    # Resolve endpoint URL: explicit arg > env var > default pattern
    url = endpoint_url or os.environ.get("JOB_ENTRYPOINT_URL")
    if not url:
        url = _DEFAULT_ENDPOINT_PATTERN.format(
            location=location, project_id=project_id
        )

    delay_seconds = retry_after + _SCHEDULE_BUFFER_SECONDS

    client = tasks_client or tasks_v2.CloudTasksClient()

    parent = client.queue_path(
        project=project_id, location=location, queue=queue
    )

    task_name = f"{parent}/tasks/retry-{job_name}"

    http_request = tasks_v2.HttpRequest(
        http_method=tasks_v2.HttpMethod.POST,
        url=url,
        headers={
            "Content-Type": "application/json",
            "RESUME_MODE": "true",
            "PARENT_JOB_ID": job_name,
        },
        body=b"",
    )

    task = tasks_v2.Task(
        name=task_name,
        http_request=http_request,
        schedule_time=datetime.now(UTC) + timedelta(seconds=delay_seconds),
    )

    try:
        response = client.create_task(request={"parent": parent, "task": task})
    except GoogleAPICallError as exc:
        raise RetryTaskError(
            f"Failed to schedule retry task for job {job_name!r} "
            f"in queue {queue!r}: {exc}"
        ) from exc

    schedule_time_iso = (
        response.schedule_time.rfc3339() if response.schedule_time else ""
    )

    return RetryTaskScheduled(
        task_name=response.name or task_name,
        schedule_time=schedule_time_iso,
        job_name=job_name,
        delay_seconds=delay_seconds,
    )
