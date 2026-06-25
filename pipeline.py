"""Iceberg Zero-ETL pipeline CLI.

Subcommands:
    init                 create the catalog, namespace, and bronze/silver tables
    ingest <file>        append raw log lines from a .jsonl/.txt file to bronze
    run [--parser NAME]  parse new bronze rows into silver
    show {bronze|silver} [--limit N]   print a table

Everything runs against a local SQLite-catalogued Iceberg warehouse so
no external services are required. Use --parser dspy (requires Ollama)
to exercise the AI path; the default regex parser needs no model.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Allow `python pipeline.py` from the repo root by putting it on sys.path.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from src.catalog import get_catalog  # noqa: E402
from src.pipeline import (  # noqa: E402
    ensure_tables,
    ingest_raw,
    read_bronze,
    read_silver,
    run_pipeline,
)


def _iter_records(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            if line.startswith("{"):
                try:
                    obj = json.loads(line)
                    yield obj.get("raw") or obj.get("raw_text") or obj.get("message") or line
                    continue
                except json.JSONDecodeError:
                    pass
            yield line


def cmd_init(args) -> int:
    catalog = get_catalog(uri=args.catalog, warehouse=args.warehouse)
    ensure_tables(catalog)
    print("catalog initialised; bronze + silver tables ready")
    return 0


def cmd_ingest(args) -> int:
    catalog = get_catalog(uri=args.catalog, warehouse=args.warehouse)
    ensure_tables(catalog)
    records = list(_iter_records(args.file))
    added = ingest_raw(catalog, records, source=args.source or os.path.basename(args.file))
    print(f"ingested {added} raw record(s) into bronze")
    return 0


def cmd_run(args) -> int:
    catalog = get_catalog(uri=args.catalog, warehouse=args.warehouse)
    ensure_tables(catalog)
    parsed = run_pipeline(catalog, parser_name=args.parser, model=args.model)
    print(f"parsed {parsed} record(s) into silver via '{args.parser}' parser")
    return 0


def cmd_show(args) -> int:
    catalog = get_catalog(uri=args.catalog, warehouse=args.warehouse)
    if args.layer == "bronze":
        table = read_bronze(catalog, limit=args.limit)
    else:
        table = read_silver(catalog, limit=args.limit)
    _print_table(table)
    return 0


def _print_table(table) -> None:
    """Render an Arrow table as a simple fixed-width grid (no pandas)."""
    if table.num_rows == 0:
        print(f"(no rows; columns: {table.schema.names})")
        return
    cols = [c.to_pylist() for c in table.columns]
    headers = table.schema.names
    str_rows = [[_fmt(v) for v in row] for row in zip(*cols)]
    widths = [max(len(h), *(len(r[i]) for r in str_rows)) for i, h in enumerate(headers)]
    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("  ".join("-" * widths[i] for i in range(len(headers))))
    for row in str_rows:
        print("  ".join(row[i].ljust(widths[i]) for i in range(len(row))))


def _fmt(v) -> str:
    if v is None:
        return ""
    s = str(v)
    return s if len(s) <= 60 else s[:57] + "..."


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="iceberg-zero-etl", description=__doc__)
    p.add_argument("--catalog", default=None, help="SQLite catalog URI (default ./catalog.db)")
    p.add_argument("--warehouse", default=None, help="warehouse dir (default ./warehouse)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="create catalog + bronze/silver tables").set_defaults(func=cmd_init)

    ing = sub.add_parser("ingest", help="append raw log lines from a file to bronze")
    ing.add_argument("file", help=".jsonl or .txt of raw log lines")
    ing.add_argument("--source", default=None, help="source label for the ingested batch")
    ing.set_defaults(func=cmd_ingest)

    run = sub.add_parser("run", help="parse new bronze rows into silver")
    run.add_argument(
        "--parser",
        default="regex",
        choices=["regex", "dspy", "gemma", "llm"],
        help="parser backend (dspy requires Ollama)",
    )
    run.add_argument("--model", default="gemma", help="Ollama model for the dspy parser")
    run.set_defaults(func=cmd_run)

    show = sub.add_parser("show", help="print a table")
    show.add_argument("layer", choices=["bronze", "silver"])
    show.add_argument("--limit", type=int, default=20)
    show.set_defaults(func=cmd_show)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
