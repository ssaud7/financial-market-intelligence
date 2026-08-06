"""Bronze layer — the raw JSON feed landed verbatim into Delta.

Bronze does not parse, clean or reshape. It captures the exact message bodies
that survived the ingestion contract, together with provenance (which file, when
ingested, under which batch), and appends them to a Delta table partitioned by
ingest date.

Keeping the payload unparsed is the point of the layer: when a parsing rule
turns out to be wrong, Silver can be rebuilt from Bronze without replaying
Kafka.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fmis.config import settings
from fmis.evidence import write_run_evidence
from fmis.lakehouse.schemas import BRONZE_SCHEMA
from fmis.lakehouse.session import delta_history, get_spark, table_exists
from fmis.lineage import dataset, lineage_run
from fmis.logging_setup import configure_logging

log = configure_logging("bronze")


def conform_to_schema(df: DataFrame, schema, *, layer: str) -> DataFrame:
    """Project ``df`` onto ``schema`` exactly: same columns, same order, same types.

    Any column the schema does not declare is dropped and any it declares but
    the frame lacks is an error. Doing this before the write means a schema
    mismatch surfaces here, with a readable message, instead of as a Delta
    ``AnalysisException`` several frames deeper.
    """
    missing = [f.name for f in schema.fields if f.name not in df.columns]
    if missing:
        raise ValueError(
            f"{layer} write is missing required column(s) {missing}. "
            f"Frame has: {sorted(df.columns)}"
        )
    extra = [c for c in df.columns if c not in schema.fieldNames()]
    if extra:
        log.info(f"{layer}.dropping_undeclared_columns", columns=extra)

    return df.select(
        *[F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields]
    )


def load_bronze(*, landing_dir: Path | None = None, batch_id: str | None = None) -> dict[str, Any]:
    """Append every landed JSON line to the Bronze Delta table."""
    spark: SparkSession = get_spark("fmis-bronze")
    landing_dir = landing_dir or settings.landing_valid_path
    batch_id = batch_id or f"bronze-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"

    files = sorted(landing_dir.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError(
            f"No landed files in {landing_dir}. Run the Kafka consumer first — "
            "Bronze only ever reads records the data contract already admitted."
        )

    with lineage_run(
        "bronze_load",
        inputs=[dataset(f"file://{landing_dir}")],
        outputs=[
            dataset(
                f"delta://{settings.bronze_path}",
                fields=[(f.name, f.dataType.simpleString()) for f in BRONZE_SCHEMA.fields],
            )
        ],
    ) as run:
        # Read as text, not JSON: Bronze must not impose structure yet.
        raw = (
            spark.read.text(str(landing_dir / "*.jsonl"))
            .withColumn("source_file", F.input_file_name())
            .withColumnRenamed("value", "raw_payload")
        )

        bronze = (
            raw.filter(F.length(F.trim(F.col("raw_payload"))) > 0)
            .withColumn("payload_bytes", F.length(F.col("raw_payload")))
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("ingest_date", F.to_date(F.current_timestamp()))
            .withColumn("batch_id", F.lit(batch_id))
        )
        bronze = conform_to_schema(bronze, BRONZE_SCHEMA, layer="bronze")

        row_count = bronze.count()
        is_new_table = not table_exists(settings.bronze_path)

        (
            bronze.write.format("delta")
            .mode("append")
            .partitionBy("ingest_date")
            # No mergeSchema: an incoming frame that does not match the table
            # must be refused, not absorbed.
            .save(str(settings.bronze_path))
        )

        total_rows = spark.read.format("delta").load(str(settings.bronze_path)).count()

        result = {
            "batch_id": batch_id,
            "files_read": [f.name for f in files],
            "rows_appended": row_count,
            "rows_total": total_rows,
            "table_created": is_new_table,
            "path": str(settings.bronze_path),
            "history": delta_history(settings.bronze_path, limit=3),
        }
        run.record(rows_appended=row_count, rows_total=total_rows, batch_id=batch_id)
        write_run_evidence("bronze_load", result)

    log.info(
        "bronze.loaded",
        rows_appended=row_count,
        rows_total=total_rows,
        files=len(files),
        batch_id=batch_id,
    )
    return result


if __name__ == "__main__":  # pragma: no cover
    load_bronze()
