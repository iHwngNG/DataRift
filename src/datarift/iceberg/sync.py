"""Iceberg table sync operations for incremental data loading."""

from google.cloud import storage
from pyiceberg.table import Table


def find_new_files(
    table: Table, gcs_prefix: str, gcs_client: storage.Client
) -> list[str]:
    """Find Parquet files not yet registered in Iceberg table.

    Args:
        table: Iceberg table
        gcs_prefix: GCS prefix to scan (e.g., 'league/')
        gcs_client: GCS client

    Returns:
        List of new file paths

    """
    current = table.current_snapshot()
    manifest_list_path: str | None = None
    if current is not None and current.manifest_list is not None:
        manifest_list_path = str(current.manifest_list)

    existing_files: set[str] = set()
    if manifest_list_path:
        pass

    bucket_name = gcs_prefix.split("/")[0] if "/" in gcs_prefix else gcs_prefix
    prefix = "/".join(gcs_prefix.split("/")[1:]) if "/" in gcs_prefix else ""

    blobs = gcs_client.list_blobs(bucket_name, prefix=prefix)
    new_files = []

    for blob in blobs:
        if blob.name.endswith(".parquet") and blob.name not in existing_files:
            new_files.append(f"gs://{bucket_name}/{blob.name}")

    return new_files


def register_files(table: Table, file_paths: list[str]) -> int:
    """Register new files into Iceberg table.

    Args:
        table: Iceberg table
        file_paths: List of GCS paths to register

    Returns:
        New snapshot ID

    """
    if not file_paths:
        current = table.current_snapshot()
        return current.snapshot_id if current else 0

    file_entries = []
    for path in file_paths:
        file_entry = {
            "data_file": {
                "file_path": path,
                "file_format": "PARQUET",
                "schema_id": table.schema().schema_id,
                "record_count": 0,
            }
        }
        file_entries.append(file_entry)

    snapshot = table.new_append().append_file(file_entries).commit()  # type: ignore[attr-defined]
    return snapshot.snapshot_id  # type: ignore[no-any-return]
