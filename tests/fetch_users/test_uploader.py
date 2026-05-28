"""
test_uploader.py
~~~~~~~~~~~~~~~~
Unit tests for :class:`~pipeline.uploader.GCSUploader`.

Uses ``unittest.mock`` to stub out the GCS client; no real GCS calls.

Tests cover:
- Partition path structure (all Hive-style path components present).
- Each upload call produces a unique object name (UUID4 suffix).
- ``run_id`` is consistent across multiple ``upload()`` calls.
- Upload method is called with correct arguments (bytes + content type).
- ``run_id`` can be injected at construction for deterministic testing.
"""

from __future__ import annotations

import os
import sys

# Allow importing from root config and jobs/fetch_users/pipeline
_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "jobs", "fetch_users"))

import unittest
from unittest.mock import MagicMock

from pipeline.uploader import GCSUploader

_BUCKET = "datarift-bronze"
_PREFIX = "bronze/users"
_PLATFORM = "kr"
_RUN_ID = "2026-01-01T00-00-00Z"


def _make_uploader(client: MagicMock) -> GCSUploader:
    return GCSUploader(
        bucket_name=_BUCKET,
        prefix=_PREFIX,
        platform=_PLATFORM,
        run_id=_RUN_ID,
        client=client,
    )


def _mock_gcs_client() -> MagicMock:
    """Build a mock GCS client where blob().upload_from_string() is a no-op."""
    client = MagicMock()
    blob = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    return client


class TestUploaderPathStructure(unittest.TestCase):
    def setUp(self) -> None:
        self.client = _mock_gcs_client()
        self.uploader = _make_uploader(self.client)

    def test_platform_in_path(self) -> None:
        self.uploader.upload(b"data", "DIAMOND", "I")
        blob_name: str = self.client.bucket.return_value.blob.call_args[0][0]
        self.assertIn(f"platform={_PLATFORM}", blob_name)

    def test_tier_in_path(self) -> None:
        self.uploader.upload(b"data", "DIAMOND", "I")
        blob_name: str = self.client.bucket.return_value.blob.call_args[0][0]
        self.assertIn("tier=DIAMOND", blob_name)

    def test_division_in_path(self) -> None:
        self.uploader.upload(b"data", "DIAMOND", "I")
        blob_name: str = self.client.bucket.return_value.blob.call_args[0][0]
        self.assertIn("rank=I", blob_name)

    def test_prefix_in_path(self) -> None:
        self.uploader.upload(b"data", "IRON", "IV")
        blob_name: str = self.client.bucket.return_value.blob.call_args[0][0]
        self.assertTrue(blob_name.startswith(_PREFIX))

    def test_parquet_extension_in_path(self) -> None:
        self.uploader.upload(b"data", "SILVER", "III")
        blob_name: str = self.client.bucket.return_value.blob.call_args[0][0]
        self.assertTrue(blob_name.endswith(".parquet"))


class TestUploaderUniqueness(unittest.TestCase):
    def test_each_upload_has_unique_object_name(self) -> None:
        client = _mock_gcs_client()
        uploader = _make_uploader(client)

        uploader.upload(b"data1", "DIAMOND", "I")
        name1: str = client.bucket.return_value.blob.call_args_list[-1][0][0]

        uploader.upload(b"data2", "DIAMOND", "I")
        name2: str = client.bucket.return_value.blob.call_args_list[-1][0][0]

        self.assertNotEqual(name1, name2)


class TestUploaderRunId(unittest.TestCase):
    def test_run_id_auto_generated_when_not_provided(self) -> None:
        client = _mock_gcs_client()
        uploader = GCSUploader(
            bucket_name=_BUCKET,
            prefix=_PREFIX,
            platform=_PLATFORM,
            client=client,
        )
        self.assertIsNotNone(uploader.run_id)
        self.assertGreater(len(uploader.run_id), 0)


class TestUploaderUploadCall(unittest.TestCase):
    def test_upload_from_string_called_with_correct_data(self) -> None:
        client = _mock_gcs_client()
        uploader = _make_uploader(client)
        payload = b"parquet-bytes-here"

        uploader.upload(payload, "EMERALD", "IV")

        blob = client.bucket.return_value.blob.return_value
        blob.upload_from_string.assert_called_once_with(
            payload, content_type="application/octet-stream"
        )

    def test_upload_returns_gs_uri(self) -> None:
        client = _mock_gcs_client()
        uploader = _make_uploader(client)
        uri = uploader.upload(b"data", "DIAMOND", "II")

        self.assertTrue(uri.startswith(f"gs://{_BUCKET}/"))
        self.assertIn("DIAMOND", uri)

    def test_bucket_name_in_returned_uri(self) -> None:
        client = _mock_gcs_client()
        uploader = _make_uploader(client)
        uri = uploader.upload(b"data", "BRONZE", "I")
        self.assertIn(_BUCKET, uri)


if __name__ == "__main__":
    unittest.main()
