"""Parquet I/O module for reading and writing Parquet files to GCS."""

from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as papq


if TYPE_CHECKING:
    from google.cloud.storage import Client as GCSClient


def read_parquet_files(gcs_client: GCSClient, gcs_prefix: str) -> pa.Table:
    """Read and concatenate all Parquet files under a GCS prefix.

    Lists all objects with .parquet extension under the prefix and
    reads them into a single PyArrow Table.

    Args:
        gcs_client: Authenticated GCS client.
        gcs_prefix: GCS path prefix (e.g., 'league/americas/riot/').
                    Can be with or without gs:// prefix.

    Returns:
        PyArrow Table containing all records from matching Parquet files.
        Returns empty table if no files found.

    """
    prefix = gcs_prefix.lstrip("/")
    if prefix.startswith("gs://"):
        parts = prefix[5:].split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
    else:
        raise ValueError(
            "gcs_prefix must be in gs://bucket/path format for GCS operations"
        )

    bucket = gcs_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    parquet_files = [blob for blob in blobs if blob.name.endswith(".parquet")]

    if not parquet_files:
        return pa.table({})

    tables: list[pa.Table] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for blob in parquet_files:
            local_path = f"{tmpdir}/{blob.name.split('/')[-1]}"
            blob.download_to_filename(local_path)
            table = papq.read_table(local_path)  # type: ignore[no-untyped-call]
            tables.append(table)

    if not tables:
        return pa.table({})

    return pa.concat_tables(tables)


def write_parquet(
    table: pa.Table,
    gcs_client: GCSClient,
    gcs_path: str,
    bucket: str,
) -> str:
    """Write PyArrow Table to Parquet file in GCS.

    Writes to a unique path with a timestamp suffix to avoid overwrites.

    Args:
        table: PyArrow Table to write.
        gcs_client: Authenticated GCS client.
        gcs_path: GCS object path (e.g., 'league/americas/riot/data.parquet').
        bucket: GCS bucket name (with or without gs:// prefix).

    Returns:
        Full GCS path of the written file.

    """
    import uuid
    from datetime import datetime

    clean_bucket = bucket.removeprefix("gs://")
    path_parts = gcs_path.rsplit("/", 1)

    if len(path_parts) == 2:
        dir_path, filename = path_parts
        base_path = f"{dir_path}/{filename}"
    else:
        base_path = gcs_path
        filename = gcs_path.split("/")[-1]

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    unique_suffix = uuid.uuid4().hex[:8]
    unique_filename = f"{timestamp}_{unique_suffix}_{filename}"
    final_path = f"{base_path.rstrip('/')}/{unique_filename}"

    gcs_full_path = f"gs://{clean_bucket}/{final_path.lstrip('/')}"

    _write_table_to_gcs(table, gcs_client, clean_bucket, final_path)

    return gcs_full_path


def overwrite_parquet(
    table: pa.Table,
    gcs_client: GCSClient,
    gcs_path: str,
    bucket: str,
) -> str:
    """Write PyArrow Table to Parquet file in GCS with explicit overwrite.

    Deletes existing file if present, then writes new data. Use for workspace
    files where deterministic naming is required.

    Args:
        table: PyArrow Table to write.
        gcs_client: Authenticated GCS client.
        gcs_path: GCS object path (e.g., 'workspace/puuid/0/data.parquet').
        bucket: GCS bucket name (with or without gs:// prefix).

    Returns:
        Full GCS path of the written file.

    """
    clean_bucket = bucket.removeprefix("gs://")
    clean_path = gcs_path.lstrip("/")

    bucket_obj = gcs_client.bucket(clean_bucket)
    blob = bucket_obj.blob(clean_path)

    if blob.exists():
        blob.delete()

    _write_table_to_gcs(table, gcs_client, clean_bucket, clean_path)

    return f"gs://{clean_bucket}/{clean_path}"


def _write_table_to_gcs(
    table: pa.Table,
    gcs_client: GCSClient,
    bucket_name: str,
    object_path: str,
) -> None:
    """Write PyArrow Table to a temporary file and upload to GCS.

    Args:
        table: PyArrow Table to write.
        gcs_client: Authenticated GCS client.
        bucket_name: GCS bucket name (without gs:// prefix).
        object_path: GCS object path within the bucket.

    """
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        papq.write_table(table, tmp_path)  # type: ignore[no-untyped-call]
        blob.upload_from_filename(tmp_path)
    finally:
        import os

        os.unlink(tmp_path)
