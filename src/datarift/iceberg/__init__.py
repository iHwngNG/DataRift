"""Iceberg table management for DataRift project."""

from datarift.iceberg.catalog import get_catalog, get_or_create_table
from datarift.iceberg.sync import find_new_files, register_files


__all__ = [
    "find_new_files",
    "get_catalog",
    "get_or_create_table",
    "register_files",
]
