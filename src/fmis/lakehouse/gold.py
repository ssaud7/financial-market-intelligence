"""Gold layer — a per-ticker analytical aggregate, maintained by MERGE.

This is not a filtered copy of Silver. Every column is derived from a *window of
sessions*: 7- and 30-session moving averages, annualised realised volatility
from daily log returns, average traded volume, and the rolling 52-week range.
Silver holds one row per ticker per day; Gold holds one row per ticker, and that
row summarises the ticker's whole history up to ``as_of_date``.

The table is maintained with a genuine ``MERGE`` keyed on ``ticker``, the
business key. Re-running the pipeline after new sessions arrive **updates** the
existing row for each known ticker and **inserts** only for ticker symbols never
seen before — the insert/update split is read back out of the Delta transaction
log afterwards as proof the operation was a real upsert.

The matched branch is additionally guarded by
``source.as_of_date >= target.as_of_date`` so a late-arriving replay of older
data cannot roll a ticker's metrics backwards.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from fmis.config import settings
from fmis.lakehouse.bronze import conform_to_schema
from fmis.lakehouse.schemas import GOLD_SCHEMA
from fmis.lakehouse.session import delta_history, get_spark, table_exists
from fmis.lineage import dataset, lineage_run
from fmis.logging_setup import configure_logging

log = configure_logging("gold")

MA_SHORT_SESSIONS = 7
MA_LONG_SESSIONS = 30
YEAR_SESSIONS = 252  # trading sessions in a calendar year, the standard convention
TREND_EPSILON = 1e-9


def compute_ticker_metrics(silver: DataFrame) -> DataFrame:
    """Reduce the Silver time series to one aggregate row per ticker."""
    ordered = Window.partitionBy("ticker").orderBy("trade_date")
    whole_history = Window.partitionBy("ticker")

    ma_long = ordered.rowsBetween(-(MA_LONG_SESSIONS - 1), 0)
    ma_short = ordered.rowsBetween(-(MA_SHORT_SESSIONS - 1), 0)
    year_window = ordered.rowsBetween(-(YEAR_SESSIONS - 1), 0)

    enriched = (
        silver.withColumn("prev_close", F.lag("close").over(ordered))
        # Log returns, so the volatility figure is the standard annualised
        # realised-volatility measure rather than a naive percentage spread.
        .withColumn(
            "log_return",
            F.when(
                (F.col("prev_close").isNotNull()) & (F.col("prev_close") > 0),
                F.log(F.col("close") / F.col("prev_close")),
            ),
        )
        .withColumn("ma_7", F.avg("close").over(ma_short))
        .withColumn("ma_30", F.avg("close").over(ma_long))
        .withColumn("avg_volume_30d", F.avg("volume").over(ma_long))
        .withColumn("stddev_log_return_30d", F.stddev_samp("log_return").over(ma_long))
        .withColumn("high_52w", F.max("high").over(year_window))
        .withColumn("low_52w", F.min("low").over(year_window))
        .withColumn("sessions_observed", F.count(F.lit(1)).over(whole_history))
        .withColumn("first_session", F.min("trade_date").over(whole_history))
    )

    # ma_30_prev is the previous session's long moving average; comparing the two
    # is what turns a level into a trend.
    enriched = enriched.withColumn("ma_30_prev", F.lag("ma_30").over(ordered))

    latest = Window.partitionBy("ticker").orderBy(F.col("trade_date").desc())
    snapshot = (
        enriched.withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return snapshot.select(
        F.col("ticker"),
        F.col("trade_date").alias("as_of_date"),
        F.col("close").alias("latest_close"),
        F.round("ma_7", 4).alias("ma_7"),
        F.round("ma_30", 4).alias("ma_30"),
        F.round("ma_30_prev", 4).alias("ma_30_prev"),
        F.when(F.col("ma_30_prev").isNull(), F.lit(None).cast("string"))
        .when(F.col("ma_30") - F.col("ma_30_prev") > TREND_EPSILON, F.lit("RISING"))
        .when(F.col("ma_30_prev") - F.col("ma_30") > TREND_EPSILON, F.lit("FALLING"))
        .otherwise(F.lit("FLAT"))
        .alias("ma_30_trend"),
        F.round(
            F.when(
                F.col("ma_30") > 0,
                (F.col("close") - F.col("ma_30")) / F.col("ma_30") * 100,
            ),
            4,
        ).alias("pct_vs_ma_30"),
        F.round(
            F.col("stddev_log_return_30d") * F.sqrt(F.lit(float(YEAR_SESSIONS))) * 100, 4
        ).alias("volatility_30d_annualised"),
        F.round("avg_volume_30d", 2).alias("avg_volume_30d"),
        F.round("high_52w", 4).alias("high_52w"),
        F.round("low_52w", 4).alias("low_52w"),
        F.round(
            F.when(
                F.col("high_52w") > 0,
                (F.col("close") - F.col("high_52w")) / F.col("high_52w") * 100,
            ),
            4,
        ).alias("pct_from_52w_high"),
        F.col("sessions_observed").cast("long").alias("sessions_observed"),
        F.col("first_session"),
        F.current_timestamp().alias("updated_at"),
    )


def _ensure_table(spark: SparkSession) -> None:
    from delta.tables import DeltaTable

    if table_exists(settings.gold_path):
        return
    (
        DeltaTable.createIfNotExists(spark)
        .addColumns(GOLD_SCHEMA)
        .location(str(settings.gold_path))
        .execute()
    )
    log.info("gold.table_created", path=str(settings.gold_path))


def merge_gold() -> dict[str, Any]:
    """Recompute the per-ticker aggregate and upsert it on the ticker key."""
    from delta.tables import DeltaTable

    spark = get_spark("fmis-gold")

    if not table_exists(settings.silver_path):
        raise FileNotFoundError(
            f"No Silver table at {settings.silver_path}. Run the Silver transform first."
        )

    with lineage_run(
        "gold_merge",
        inputs=[dataset(f"delta://{settings.silver_path}")],
        outputs=[
            dataset(
                f"delta://{settings.gold_path}",
                fields=[(f.name, f.dataType.simpleString()) for f in GOLD_SCHEMA.fields],
            )
        ],
    ) as run:
        silver = spark.read.format("delta").load(str(settings.silver_path))
        silver_rows = silver.count()

        metrics_df = compute_ticker_metrics(silver)
        metrics_df = conform_to_schema(metrics_df, GOLD_SCHEMA, layer="gold")
        metrics_df.cache()
        source_rows = metrics_df.count()

        _ensure_table(spark)
        rows_before = spark.read.format("delta").load(str(settings.gold_path)).count()

        # The MERGE. Keyed on the ticker business key; the matched branch is
        # guarded so stale replays cannot regress a ticker's snapshot.
        (
            DeltaTable.forPath(spark, str(settings.gold_path))
            .alias("target")
            .merge(metrics_df.alias("source"), "target.ticker = source.ticker")
            .whenMatchedUpdateAll(condition="source.as_of_date >= target.as_of_date")
            .whenNotMatchedInsertAll()
            .execute()
        )

        history = delta_history(settings.gold_path, limit=1)
        op_metrics = history[0]["operationMetrics"] if history else {}
        rows_after = spark.read.format("delta").load(str(settings.gold_path)).count()

        result = {
            "silver_rows_scanned": silver_rows,
            "aggregate_rows_computed": source_rows,
            "gold_rows_before": rows_before,
            "gold_rows_after": rows_after,
            "rows_inserted": int(op_metrics.get("numTargetRowsInserted", 0)),
            "rows_updated": int(op_metrics.get("numTargetRowsUpdated", 0)),
            "rows_deleted": int(op_metrics.get("numTargetRowsDeleted", 0)),
            "operation": history[0]["operation"] if history else None,
            "path": str(settings.gold_path),
            "aggregation": {
                "grain": "one row per ticker",
                "business_key": "ticker",
                "moving_average_sessions": MA_LONG_SESSIONS,
                "volatility_annualisation_sessions": YEAR_SESSIONS,
            },
            "history": history,
        }
        run.record(
            rows_inserted=result["rows_inserted"],
            rows_updated=result["rows_updated"],
            gold_rows_after=rows_after,
        )
        metrics_df.unpersist()

    log.info(
        "gold.merged",
        operation=result["operation"],
        inserted=result["rows_inserted"],
        updated=result["rows_updated"],
        rows_after=rows_after,
    )
    return result


def read_gold(limit: int = 20) -> list[dict[str, Any]]:
    """Convenience reader for notebooks and the run report."""
    spark = get_spark("fmis-gold-read")
    rows = (
        spark.read.format("delta")
        .load(str(settings.gold_path))
        .orderBy("ticker")
        .limit(limit)
        .collect()
    )
    return [r.asDict(recursive=True) for r in rows]


if __name__ == "__main__":  # pragma: no cover
    merge_gold()
