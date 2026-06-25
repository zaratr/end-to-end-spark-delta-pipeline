"""End-to-end pipeline tests against a temp SQLite/Iceberg warehouse.

No Ollama required — exercises the regex parser, the Iceberg medallion
write/read path, and idempotency of the bronze→silver transform.
"""

import os
import sys
from pathlib import Path

import pyarrow as pa
import pytest

# Ensure the repo root is importable when running pytest from anywhere.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.catalog import BRONZE_TABLE, SILVER_TABLE, get_catalog, table_identifier  # noqa: E402
from src.parsers import RegexParser, get_parser  # noqa: E402
from src.pipeline import ensure_tables, ingest_raw, read_bronze, read_silver, run_pipeline  # noqa: E402


@pytest.fixture()
def catalog(tmp_path):
    uri = f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}"
    warehouse = (tmp_path / "warehouse").as_posix()
    cat = get_catalog(uri=uri, warehouse=warehouse)
    ensure_tables(cat)
    return cat


RAW_LOGS = [
    "[2026-05-09 15:30:45] WARN (auth-service): login failed after 5000ms",
    "[2026-05-09 15:31:02] ERROR (payments-api): card charge declined for txn 88412",
    "not a structured log line",
]


def test_ensure_tables_creates_both(catalog):
    bronze = catalog.load_table(table_identifier(BRONZE_TABLE)).scan().to_arrow()
    silver = catalog.load_table(table_identifier(SILVER_TABLE)).scan().to_arrow()
    assert bronze.num_rows == 0
    assert silver.num_rows == 0
    assert "raw_text" in bronze.schema.names
    assert "message" in silver.schema.names


def test_ingest_then_run_flows_rows(catalog):
    added = ingest_raw(catalog, RAW_LOGS, source="test")
    assert added == 3

    parsed = run_pipeline(catalog)  # default regex parser
    assert parsed == 3

    silver = read_silver(catalog)
    assert silver.num_rows == 3
    assert set(silver.column("service").to_pylist()) == {"auth-service", "payments-api", "unknown"}
    levels = set(silver.column("level").to_pylist())
    assert "WARN" in levels and "ERROR" in levels and "UNKNOWN" in levels


def test_run_is_idempotent(catalog):
    ingest_raw(catalog, RAW_LOGS, source="test")
    assert run_pipeline(catalog) == 3
    # Re-running must not duplicate silver rows.
    assert run_pipeline(catalog) == 0
    assert read_silver(catalog).num_rows == 3


def test_incremental_ingest_only_parses_new(catalog):
    ingest_raw(catalog, RAW_LOGS[:1])
    assert run_pipeline(catalog) == 1
    ingest_raw(catalog, RAW_LOGS[1:])
    assert run_pipeline(catalog) == 2  # only the two new rows
    assert read_silver(catalog).num_rows == 3


def test_regex_parser_unmatched_keeps_message():
    parser = RegexParser()
    out = parser.parse("garbage without structure")
    assert out["level"] == "UNKNOWN"
    assert out["service"] == "unknown"
    assert out["message"] == "garbage without structure"


def test_get_parser_default_is_regex():
    assert isinstance(get_parser(), RegexParser)
    assert isinstance(get_parser("regex"), RegexParser)
