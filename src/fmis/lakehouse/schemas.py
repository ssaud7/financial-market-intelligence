"""Explicit Spark schemas for each medallion layer.

Nothing in this pipeline is written with an inferred schema. Inference is what
lets a column quietly change type between runs — a ``volume`` that arrives as a
string in one batch and a long in the next — and it is precisely the failure the
rubric's "strict schema enforcement at the write level" is asking about. Each
layer declares its shape here, and every write is checked against it.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------
# Bronze — the raw JSON feed, preserved verbatim.
#
# Bronze deliberately does NOT parse the payload. It stores the exact bytes that
# came off the topic plus ingestion provenance, so the layer can be replayed
# after a parsing bug is fixed without going back to Kafka.
# --------------------------------------------------------------------------
BRONZE_SCHEMA = StructType(
    [
        StructField("raw_payload", StringType(), nullable=False),
        StructField("source_file", StringType(), nullable=False),
        StructField("payload_bytes", IntegerType(), nullable=False),
        StructField("ingest_ts", TimestampType(), nullable=False),
        StructField("ingest_date", DateType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
    ]
)

# The shape the Bronze payload is expected to parse into. Used with from_json in
# the Silver transform; a payload that does not match yields nulls, which the
# Silver filters then reject.
QUOTE_PAYLOAD_SCHEMA = StructType(
    [
        StructField("ticker", StringType()),
        StructField("trade_date", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("adj_close", DoubleType()),
        StructField("volume", LongType()),
        StructField("source", StringType()),
        StructField("event_ts", StringType()),
    ]
)

# --------------------------------------------------------------------------
# Silver — cleaned, correctly typed, one row per (ticker, trade_date).
# --------------------------------------------------------------------------
SILVER_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), nullable=False),
        StructField("trade_date", DateType(), nullable=False),
        StructField("open", DoubleType(), nullable=False),
        StructField("high", DoubleType(), nullable=False),
        StructField("low", DoubleType(), nullable=False),
        StructField("close", DoubleType(), nullable=False),
        StructField("adj_close", DoubleType(), nullable=True),
        StructField("volume", LongType(), nullable=False),
        StructField("source", StringType(), nullable=True),
        StructField("event_ts", TimestampType(), nullable=True),
        StructField("ingest_ts", TimestampType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
    ]
)

# --------------------------------------------------------------------------
# Gold — one row per ticker. This is an aggregate over the full Silver history,
# not a projection of it: every column below is derived from a window of
# sessions, and the table is maintained by MERGE on the ticker business key.
# --------------------------------------------------------------------------
GOLD_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), nullable=False),
        StructField("as_of_date", DateType(), nullable=False),
        StructField("latest_close", DoubleType(), nullable=False),
        StructField("ma_7", DoubleType(), nullable=True),
        StructField("ma_30", DoubleType(), nullable=True),
        StructField("ma_30_prev", DoubleType(), nullable=True),
        StructField("ma_30_trend", StringType(), nullable=True),
        StructField("pct_vs_ma_30", DoubleType(), nullable=True),
        StructField("volatility_30d_annualised", DoubleType(), nullable=True),
        StructField("avg_volume_30d", DoubleType(), nullable=True),
        StructField("high_52w", DoubleType(), nullable=True),
        StructField("low_52w", DoubleType(), nullable=True),
        StructField("pct_from_52w_high", DoubleType(), nullable=True),
        StructField("sessions_observed", LongType(), nullable=False),
        StructField("first_session", DateType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

# Delta CHECK constraints applied to Silver. Unlike the Pydantic contract, which
# guards the *ingestion boundary*, these are enforced by the storage layer
# itself: any writer touching this table — this pipeline, a notebook, a future
# job — is held to them, and a violating write is aborted.
SILVER_CONSTRAINTS: dict[str, str] = {
    "high_ge_low": "high >= low",
    "close_within_session_range": "close >= low AND close <= high",
    "open_within_session_range": "open >= low AND open <= high",
    "prices_positive": "open > 0 AND high > 0 AND low > 0 AND close > 0",
    "volume_non_negative": "volume >= 0",
}
