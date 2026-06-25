"""Iceberg catalog + namespace management.

Uses a local SQL-backed catalog (SQLite) so the entire pipeline runs on
a laptop with no external services. The warehouse is a directory on the
local filesystem; Iceberg tables are written there as Parquet + metadata.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Sequence, Tuple

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

NAMESPACE: Tuple[str, ...] = ("logging",)
BRONZE_TABLE = "raw_logs"
SILVER_TABLE = "structured_logs"


def get_catalog(
    uri: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    """Return a configured SQL catalog.

    Args:
        uri: SQLite URI for the catalog DB (default ./catalog.db).
        warehouse: filesystem warehouse dir (default ./warehouse).
    """
    warehouse = warehouse or os.path.abspath("warehouse")
    Path(warehouse).mkdir(parents=True, exist_ok=True)
    uri = uri or f"sqlite:///{os.path.abspath('catalog.db')}"
    return load_catalog(
        "default",
        **{"type": "sql", "uri": uri, "warehouse": f"file://{warehouse}"},
    )


def ensure_namespace(catalog) -> None:
    """Create the logging namespace if it does not already exist."""
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    except Exception:
        # Some catalog backends raise a generic error on duplicate
        # namespace creation; treat that as success.
        if not _namespace_exists(catalog):
            raise


def _namespace_exists(catalog) -> bool:
    try:
        namespaces = catalog.list_namespaces()
        return NAMESPACE in [tuple(ns) for ns in namespaces]
    except Exception:
        return False


def table_identifier(name: str) -> Tuple[str, ...]:
    return NAMESPACE + (name,)
