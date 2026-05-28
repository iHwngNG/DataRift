"""
iceberg_config.py
~~~~~~~~~~~~~~~~~
Apache Iceberg catalog and warehouse configuration for DataRift.

Controls the catalog backend type, REST catalog URI, warehouse storage path
on GCS, the default namespace, table naming prefix, and the GCP project ID
used by the PyIceberg GCS FileIO adapter.

All settings can be overridden at runtime via environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Supported catalog types recognised by PyIceberg
_SUPPORTED_CATALOG_TYPES = frozenset({"rest", "glue", "hive", "sql", "dynamodb"})


@dataclass(frozen=True)
class IcebergConfig:
    """Typed configuration for the Apache Iceberg catalog.

    Attributes:
        catalog_type: PyIceberg catalog backend, e.g. ``"rest"``, ``"glue"``.
        catalog_uri: URI of the REST catalog server (only used when
            ``catalog_type == "rest"``).
        warehouse_uri: Root warehouse path on GCS, e.g.
            ``"gs://datarift-silver/warehouse"``.
        namespace: Default Iceberg namespace (database) for DataRift tables.
        table_prefix: Optional prefix applied to every managed table name.
        gcs_project: GCP project ID used by the PyIceberg GCS FileIO adapter.
    """

    catalog_type: str
    catalog_uri: str
    warehouse_uri: str
    namespace: str
    table_prefix: str
    gcs_project: str

    @classmethod
    def from_env(cls) -> "IcebergConfig":
        """Instantiate :class:`IcebergConfig` from environment variables.

        Returns:
            A fully populated :class:`IcebergConfig` instance.

        Raises:
            ValueError: If ``ICEBERG_CATALOG_TYPE`` is set to an unsupported value.
        """
        catalog_type = os.getenv("ICEBERG_CATALOG_TYPE", "rest").lower()
        if catalog_type not in _SUPPORTED_CATALOG_TYPES:
            raise ValueError(
                f"Unsupported ICEBERG_CATALOG_TYPE {catalog_type!r}. "
                f"Must be one of: {sorted(_SUPPORTED_CATALOG_TYPES)}"
            )

        return cls(
            catalog_type=catalog_type,
            catalog_uri=os.getenv("ICEBERG_CATALOG_URI", ""),
            warehouse_uri=os.getenv(
                "ICEBERG_WAREHOUSE_URI", "gs://datarift-silver/warehouse"
            ),
            namespace=os.getenv("ICEBERG_NAMESPACE", "lol"),
            table_prefix=os.getenv("ICEBERG_TABLE_PREFIX", "raw_"),
            gcs_project=os.getenv("GCP_PROJECT_ID", ""),
        )

    def qualified_table_name(self, table: str) -> str:
        """Return the fully-qualified Iceberg table identifier.

        Combines the namespace, prefix, and base table name into the
        dot-separated format expected by PyIceberg.

        Args:
            table: Base table name, e.g. ``"matches"``.

        Returns:
            Fully-qualified name, e.g. ``"lol.raw_matches"``.
        """
        return f"{self.namespace}.{self.table_prefix}{table}"

    def to_catalog_properties(self) -> dict[str, str]:
        """Build the ``properties`` dict expected by :func:`pyiceberg.catalog.load_catalog`.

        Returns:
            Dictionary ready to be passed as keyword arguments to
            ``load_catalog(name, **properties)``.
        """
        props: dict[str, str] = {
            "type": self.catalog_type,
            "warehouse": self.warehouse_uri,
        }
        if self.catalog_uri:
            props["uri"] = self.catalog_uri
        if self.gcs_project:
            props["gcs.project-id"] = self.gcs_project
        return props
