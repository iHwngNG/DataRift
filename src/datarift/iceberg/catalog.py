"""Iceberg catalog management for BigQuery-backed tables."""

import os

from google.oauth2 import service_account
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partition import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table


def get_catalog(
    warehouse_uri: str, project_id: str, credentials_path: str | None = None
) -> Catalog:
    """Create Iceberg REST Catalog connected to BigQuery.

    Args:
        warehouse_uri: GCS warehouse path like gs://bucket/iceberg
        project_id: GCP project ID for BigQuery
        credentials_path: Optional path to service account JSON

    Returns:
        Configured Iceberg Catalog instance

    """
    catalog_props = {
        "type": "rest",
        "uri": "https://api.iceberg.dev/v1",
        "warehouse": warehouse_uri,
        "gcs-project-id": project_id,
    }

    if credentials_path and os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(  # type: ignore[no-untyped-call]
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        catalog_props["gcs-credentials-json"] = credentials.to_json_data()

    return RestCatalog(**catalog_props)


def get_or_create_table(
    catalog: Catalog,
    table_name: str,
    schema: Schema,
    partition_spec: PartitionSpec,
    location: str | None = None,
) -> Table:
    """Get existing table or create new one.

    Args:
        catalog: Iceberg catalog
        table_name: Table name (e.g., 'datarift.league')
        schema: PyIceberg schema
        partition_spec: Partition specification
        location: Optional GCS location override

    Returns:
        Iceberg Table instance

    """
    try:
        return catalog.load_table(table_name)
    except Exception:
        return catalog.create_table(
            table_name,
            schema=schema,
            partition_spec=partition_spec,
            location=location,
        )
