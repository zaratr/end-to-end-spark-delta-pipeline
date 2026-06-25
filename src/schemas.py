"""Arrow schemas for the medallion layers.

Bronze holds raw, untouched log text exactly as ingested. Silver holds
records parsed into structured fields by the DSPy/regex parser. Keeping
the schemas as Arrow means the same definition drives both the Iceberg
table creation and the in-memory batching.
"""

from __future__ import annotations

import pyarrow as pa

# Bronze: raw ingestion layer — nothing is interpreted here.
BRONZE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("raw_text", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
    ]
)

# Silver: parsed, queryable. timestamp is kept as the parsed string the
# source emitted; downstream consumers can cast as needed.
SILVER_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("timestamp", pa.string()),
        pa.field("level", pa.string()),
        pa.field("service", pa.string()),
        pa.field("message", pa.string()),
        pa.field("parsed_by", pa.string(), nullable=False),
        pa.field("parsed_at", pa.timestamp("us"), nullable=False),
    ]
)
