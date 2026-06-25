"""Medallion pipeline: Bronze (raw) → Silver (parsed).

Zero-ETL here means the raw bytes land in an Iceberg table untouched
(Bronze) and the structured interpretation is materialised in a second
Iceberg table (Silver) that any engine can query in place — no
intermediate copy. The bronze→silver transform is the AI step: each raw
record is parsed into structured fields by a pluggable
:class:`src.parsers.LogParser`.

Tables live in a local SQL-catalogued Iceberg warehouse so the whole
flow runs on one machine with no external services.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Optional

import pyarrow as pa

from src.catalog import (
    BRONZE_TABLE,
    SILVER_TABLE,
    ensure_namespace,
    table_identifier,
)
from src.parsers import LogParser, get_parser
from src.schemas import BRONZE_SCHEMA, SILVER_SCHEMA


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_tables(catalog) -> None:
    """Create namespace + bronze/silver Iceberg tables if absent."""
    ensure_namespace(catalog)

    for name, schema in ((BRONZE_TABLE, BRONZE_SCHEMA), (SILVER_TABLE, SILVER_SCHEMA)):
        identifier = table_identifier(name)
        try:
            catalog.load_table(identifier)
        except Exception:
            catalog.create_table(identifier, schema=schema)


def _next_ids(catalog, table_name: str, count: int, start_seed: int = 1) -> List[int]:
    """Return ``count`` monotonically increasing ids for a table.

    Scans the existing max id and continues from there; tables start at
    ``start_seed``. Kept simple — a real deployment would use a
    sequence/identity column.
    """
    try:
        table = catalog.load_table(table_identifier(table_name))
        arrow = table.scan().to_arrow()
    except Exception:
        return list(range(start_seed, start_seed + count))

    if arrow.num_rows == 0:
        base = start_seed - 1
    else:
        base = int(pa.compute.max(arrow.column("id")).as_py())
    return [base + i for i in range(1, count + 1)]


def ingest_raw(
    catalog,
    records: Iterable[str],
    source: str = "manual",
) -> int:
    """Append raw log text to the Bronze table. Returns the count added."""
    records = list(records)
    if not records:
        return 0

    ids = _next_ids(catalog, BRONZE_TABLE, len(records))
    now = _now()
    table = pa.Table.from_arrays(
        [
            pa.array(ids, type=pa.int64()),
            pa.array(records, type=pa.string()),
            pa.array([source] * len(records), type=pa.string()),
            pa.array([now] * len(records), type=pa.timestamp("us")),
        ],
        schema=BRONZE_SCHEMA,
    )
    catalog.load_table(table_identifier(BRONZE_TABLE)).append(table)
    return len(records)


def run_pipeline(
    catalog,
    parser: Optional[LogParser] = None,
    *,
    parser_name: Optional[str] = None,
    model: str = "gemma",
) -> int:
    """Parse every Bronze row into the Silver table. Returns count parsed.

    Existing silver rows are not re-parsed: the transform only processes
    Bronze ids not already present in Silver. This makes the pipeline
    idempotent and safe to re-run as new data lands.
    """
    parser = parser or get_parser(parser_name, model=model)

    bronze = catalog.load_table(table_identifier(BRONZE_TABLE)).scan().to_arrow()
    if bronze.num_rows == 0:
        return 0

    try:
        silver = catalog.load_table(table_identifier(SILVER_TABLE)).scan().to_arrow()
        done = set(silver.column("id").to_pylist())
    except Exception:
        done = set()

    pending_idx = [i for i, rid in enumerate(bronze.column("id").to_pylist()) if rid not in done]
    if not pending_idx:
        return 0

    raws = [bronze.column("raw_text")[i].as_py() for i in pending_idx]
    bronze_ids = [bronze.column("id")[i].as_py() for i in pending_idx]
    parsed = parser.parse_batch(raws)
    now = _now()

    silver_table = pa.Table.from_arrays(
        [
            pa.array(bronze_ids, type=pa.int64()),
            pa.array([p["timestamp"] for p in parsed], type=pa.string()),
            pa.array([p["level"] for p in parsed], type=pa.string()),
            pa.array([p["service"] for p in parsed], type=pa.string()),
            pa.array([p["message"] for p in parsed], type=pa.string()),
            pa.array([parser.name] * len(parsed), type=pa.string()),
            pa.array([now] * len(parsed), type=pa.timestamp("us")),
        ],
        schema=SILVER_SCHEMA,
    )
    catalog.load_table(table_identifier(SILVER_TABLE)).append(silver_table)
    return len(parsed)


def read_silver(catalog, limit: Optional[int] = None) -> pa.Table:
    table = catalog.load_table(table_identifier(SILVER_TABLE)).scan().to_arrow()
    if limit is not None:
        table = table.slice(0, limit)
    return table


def read_bronze(catalog, limit: Optional[int] = None) -> pa.Table:
    table = catalog.load_table(table_identifier(BRONZE_TABLE)).scan().to_arrow()
    if limit is not None:
        table = table.slice(0, limit)
    return table
