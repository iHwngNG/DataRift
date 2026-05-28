"""
uploader.py
~~~~~~~~~~~
GCS writer for the ``fetch_users`` job.

Partition path schema
---------------------
::

    {prefix}/platform={platform}/tier={tier}/rank={division}/
        run_id={ISO_DATETIME}/part-{uuid4}.parquet

This follows Hive-style partitioning so that Spark, BigQuery, and Iceberg
readers can perform partition pruning automatically.

``run_id`` uses the ISO 8601 datetime (UTC) at the moment the first upload
of this job run is initiated.  This prevents silent overwrites when the same
Cloud Run Job is re-triggered on the same day.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from google.cloud import storage  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class GCSUploader:
    """Uploads Parquet bytes to GCS with Hive-style partition paths.

    Args:
        bucket_name: GCS bucket name (without ``gs://`` prefix).
        prefix: Object path prefix, e.g. ``"bronze/users"``.
        platform: Platform slug embedded in the partition path.
        run_id: ISO datetime string shared across all uploads in one job run.
            If ``None``, generated automatically at construction time (UTC now).
        client: Optional pre-built :class:`google.cloud.storage.Client`.
            If ``None``, the default Application Default Credentials client is
            used (suitable for Cloud Run).
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        platform: str,
        run_id: Optional[str] = None,
        client: Optional[storage.Client] = None,
    ) -> None:
        self._bucket_name = bucket_name
        self._prefix = prefix.strip("/")
        self._platform = platform
        self._run_id = run_id or datetime.now(tz=timezone.utc).strftime(
            "%Y-%m-%dT%H-%M-%SZ"
        )
        self._client = client or storage.Client()
        self._bucket = self._client.bucket(bucket_name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upload(
        self,
        data: bytes,
        tier: str,
        division: str,
    ) -> str:
        """Upload Parquet bytes to the appropriate GCS partition path.

        Args:
            data: Raw Parquet file bytes.
            tier: Rank tier, e.g. ``"DIAMOND"``.
            division: Rank division, e.g. ``"I"``.

        Returns:
            The full GCS URI of the uploaded object,
            e.g. ``"gs://my-bucket/bronze/users/platform=kr/tier=DIAMOND/…"``.

        Raises:
            google.cloud.exceptions.GoogleCloudError: On upload failure.
        """
        object_name = self._build_object_name(tier, division)
        blob = self._bucket.blob(object_name)
        blob.upload_from_string(data, content_type="application/octet-stream")

        uri = f"gs://{self._bucket_name}/{object_name}"
        logger.info(
            "Uploaded %.2f MB → %s",
            len(data) / 1024 / 1024,
            uri,
        )
        return uri

    @property
    def run_id(self) -> str:
        """The run identifier shared across all uploads in this job execution."""
        return self._run_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_object_name(self, tier: str, division: str) -> str:
        """Build the Hive-partitioned GCS object name.

        Path structure::

            {prefix}/platform={platform}/tier={tier}/rank={division}/
                part-{uuid4}.parquet

        Args:
            tier: e.g. ``"DIAMOND"``.
            division: e.g. ``"I"``.

        Returns:
            Full object name relative to bucket root.
        """
        part_id = uuid.uuid4().hex
        return (
            f"{self._prefix}"
            f"/platform={self._platform}"
            f"/tier={tier}"
            f"/rank={division}"
            f"/part-{part_id}.parquet"
        )
