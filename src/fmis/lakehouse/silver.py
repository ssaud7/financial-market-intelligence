"""Silver layer — parsed, correctly typed, deduplicated time series.

Silver is where the raw Bronze payload becomes a queryable table:

* ``from_json`` against an explicit payload schema — never inference,
* every column cast to its declared type (dates as ``DATE``, volume as
  ``BIGINT``, prices as ``DOUBLE``),
* one row per ``(ticker, trade_date)``, the latest event winning, and
* a MERGE rather than an append, so replaying a Kafka batch after a crash
  updates rows instead of duplicating them.

The table also carries Delta ``CHECK`` constraints (see
:data:`~fmis.lakehouse.schemas.SILVER_CONSTRAINTS`). Those are enforced by the
storage engine on every write from any client, which is a stronger guarantee
than validating in application code.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from fmis.config import settings
from fmis.lakehouse.bronze import conform_to_schema
from fmis.lakehouse.schemas import QUOTE_PAYLOAD_SCHEMA, SILVER_CONSTRAINTS, SILVER_SCHEMA
from fmis.lakehouse.session import delta_history, get_spark, table_exists
from fmis.lineage import dataset, lineage_run
from fmis.logging_setup import configure_logging

log = configure_logging("silver")

REQUIRED_NOT_NULL = ("ticker", "trade_date", "open", "high", "low", "close", "volume")


def _parse_bronze(spark: SparkSession) -> DataFrame:
    bronze = spark.read.format("delta").load(str(settings.bronze_path))

    parsed = bronze.select(
        F.from_json(F.col("raw_payload"), QUOTE_PAYLOAD_SCHEMA).alias("q"),
        F.col("ingest_ts"),
        F.col("batch_id"),
    )

    return parsed.select(
        F.upper(F.trim(F.col("q.ticker"))).alias("ticker"),
        F.to_date(F.col("q.trade_date")).alias("trade_date"),
        F.col("q.open").cast("double").alias("open"),
        F.col("q.high").cast("double").alias("high"),
        F.col("q.low").cast("double").alias("low"),
        F.col("q.close").cast("double").alias("close"),
        F.col("q.adj_close").cast("double").alias("adj_close"),
        F.col("q.volume").cast("long").alias("volume"),
        F.col("q.source").alias("source"),
        F.to_timestamp(F.col("q.event_ts")).alias("event_ts"),
        F.col("ingest_ts"),
        F.col("batch_id"),
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per (ticker, trade_date): the most recently emitted one.

    Kafka delivery is at-least-once and the consumer commits offsets only after
    a durable write, so a crash mid-batch replays messages. Ordering by event
    time and then ingest time makes the winner deterministic.
    """
    window = Window.partitionBy("ticker", "trade_date").orderBy(
        F.col("event_ts").desc_nulls_last(), F.col("ingest_ts").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _ensure_table(spark: SparkSession) -> None:
    """Create the Silver table with its declared schema and CHECK constraints."""
    from delta.tables import DeltaTable

    if not table_exists(settings.silver_path):
        (
            DeltaTable.createIfNotExists(spark)
            .addColumns(SILVER_SCHEMA)
            .partitionedBy("ticker")
            .location(str(settings.silver_path))
            .execute()
        )
        log.info("silver.table_created", path=str(settings.silver_path))

    for name, expression in SILVER_CONSTRAINTS.items():
        try:
            spark.sql(
                f"ALTER TABLE delta.`{settings.silver_path}` "
                f"ADD CONSTRAINT {name} CHECK ({expression})"
            )
            log.info("silver.constraint_added", constraint=name, expression=expression)
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                continue
            raise


def build_silver() -> dict[str, Any]:
    """Transform Bronze into Silver and upsert on (ticker, trade_date)."""
    from delta.tables import DeltaTable

    spark = get_spark("fmis-silver")

    if not table_exists(settings.bronze_path):
        raise FileNotFoundError(
            f"No Bronze table at {settings.bronze_path}. Run the Bronze loader first."
        )

    with lineage_run(
        "silver_transform",
        inputs=[dataset(f"delta://{settings.bronze_path}")],
        outputs=[
            dataset(
                f"delta://{settings.silver_path}",
                fields=[(f.name, f.dataType.simpleString()) for f in SILVER_SCHEMA.fields],
            )
        ],
    ) as run:
        parsed = _parse_bronze(spark)
        parsed_count = parsed.count()

        # Any null here means a payload that satisfied the Pydantic contract
        # still failed to parse — a genuine defect worth surfacing loudly rather
        # than dropping quietly.
        null_condition = F.lit(False)
        for column in REQUIRED_NOT_NULL:
            null_condition = null_condition | F.col(column).isNull()
        unparseable = parsed.filter(null_condition)
        unparseable_count = unparseable.count()
        if unparseable_count:
            log.error(
                "silver.unparseable_rows",
                count=unparseable_count,
                sample=[r.asDict() for r in unparseable.limit(3).collect()],
            )

        clean = _deduplicate(parsed.filter(~null_condition))
        clean = conform_to_schema(clean, SILVER_SCHEMA, layer="silver")
        clean.cache()
        source_count = clean.count()
        duplicates_collapsed = parsed_count - unparseable_count - source_count

        _ensure_table(spark)

        (
            DeltaTable.forPath(spark, str(settings.silver_path))
            .alias("target")
            .merge(
                clean.alias("source"),
                "target.ticker = source.ticker AND target.trade_date = source.trade_date",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        history = delta_history(settings.silver_path, limit=1)
        metrics = history[0]["operationMetrics"] if history else {}
        total_rows = spark.read.format("delta").load(str(settings.silver_path)).count()
        clean.unpersist()

        result = {
            "rows_parsed": parsed_count,
            "rows_unparseable": unparseable_count,
            "duplicates_collapsed": duplicates_collapsed,
            "rows_merged_from": source_count,
            "rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
            "rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
            "rows_total": total_rows,
            "constraints": SILVER_CONSTRAINTS,
            "path": str(settings.silver_path),
            "history": history,
        }
        run.record(**{k: v for k, v in result.items() if isinstance(v, (int, str))})

    log.info("silver.built", **{k: v for k, v in result.items() if isinstance(v, (int, str))})
    return result


if __name__ == "__main__":  # pragma: no cover
    build_silver()
