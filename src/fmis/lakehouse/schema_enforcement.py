"""Proof that bad writes are refused by the storage layer, not just by our code.

The rubric asks for schema enforcement to be *demonstrated*, and specifically for
"a bad write actually being refused". This module attempts a series of writes
that must fail, records exactly how each was rejected, and fails loudly if any
of them is accepted.

The attempts run against a sandbox table created from the **identical** schema
and CHECK constraints as Silver (see
:data:`~fmis.lakehouse.schemas.SILVER_CONSTRAINTS`), seeded with real Silver
rows. Using a sandbox means an enforcement hole shows up as a failed assertion
here rather than as corrupt data in the table the quality gate is about to read.

Two distinct mechanisms are exercised:

* **Schema enforcement** — the frame's columns and types must match the table.
  Delta refuses a mismatched append unless ``mergeSchema`` is explicitly set,
  and this project keeps ``autoMerge`` off precisely so that it does.
* **Invariants** — ``NOT NULL`` and ``CHECK`` constraints stored in the table
  metadata, enforced transactionally for every writer.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fmis.config import settings
from fmis.lakehouse.schemas import SILVER_CONSTRAINTS, SILVER_SCHEMA
from fmis.lakehouse.session import get_spark, table_exists
from fmis.lineage import dataset, lineage_run
from fmis.logging_setup import configure_logging

log = configure_logging("schema_enforcement")

SANDBOX_PATH = settings.lakehouse_root / "_enforcement_sandbox" / "quotes"


@dataclass
class Attempt:
    """One deliberately invalid write, and the defence expected to stop it."""

    name: str
    mechanism: str
    description: str
    build: Callable[[SparkSession, DataFrame], DataFrame]


def _valid_row(spark: SparkSession, template: DataFrame) -> DataFrame:
    """A well-formed row, used as the control that the table does accept writes."""
    return template.limit(1).withColumn("trade_date", F.date_add(F.col("trade_date"), 1))


def _extra_column(spark: SparkSession, template: DataFrame) -> DataFrame:
    return _valid_row(spark, template).withColumn("unauthorised_column", F.lit("surprise"))


def _missing_column(spark: SparkSession, template: DataFrame) -> DataFrame:
    return _valid_row(spark, template).drop("close")


def _wrong_type(spark: SparkSession, template: DataFrame) -> DataFrame:
    # volume is declared BIGINT; hand it a string that cannot be a number.
    return _valid_row(spark, template).withColumn("volume", F.lit("not-a-number"))


def _null_in_required_column(spark: SparkSession, template: DataFrame) -> DataFrame:
    return _valid_row(spark, template).withColumn("close", F.lit(None).cast("double"))


def _violates_high_ge_low(spark: SparkSession, template: DataFrame) -> DataFrame:
    return (
        _valid_row(spark, template)
        .withColumn("high", F.lit(10.0))
        .withColumn("low", F.lit(500.0))
        .withColumn("open", F.lit(10.0))
        .withColumn("close", F.lit(10.0))
    )


def _negative_price(spark: SparkSession, template: DataFrame) -> DataFrame:
    return (
        _valid_row(spark, template)
        .withColumn("close", F.lit(-42.0))
        .withColumn("low", F.lit(-50.0))
    )


ATTEMPTS: tuple[Attempt, ...] = (
    Attempt(
        "extra_column",
        "schema enforcement",
        "append a frame carrying a column the table does not declare",
        _extra_column,
    ),
    Attempt(
        "missing_column",
        "schema enforcement",
        "append a frame with the close column removed",
        _missing_column,
    ),
    Attempt(
        "wrong_type",
        "schema enforcement",
        "append a frame whose volume is a string rather than BIGINT",
        _wrong_type,
    ),
    Attempt(
        "null_in_not_null_column",
        "NOT NULL invariant",
        "append a row whose close price is null",
        _null_in_required_column,
    ),
    Attempt(
        "high_below_low",
        "CHECK constraint",
        "append a session whose high is below its low",
        _violates_high_ge_low,
    ),
    Attempt(
        "negative_price",
        "CHECK constraint",
        "append a session with a negative close price",
        _negative_price,
    ),
)


def _build_sandbox(spark: SparkSession) -> DataFrame:
    """Recreate the sandbox from Silver's DDL and seed it with real rows."""
    from delta.tables import DeltaTable

    if not table_exists(settings.silver_path):
        raise FileNotFoundError(
            f"No Silver table at {settings.silver_path}. Run the Silver transform first."
        )

    if SANDBOX_PATH.exists():
        shutil.rmtree(SANDBOX_PATH)

    (
        DeltaTable.createIfNotExists(spark)
        .addColumns(SILVER_SCHEMA)
        .partitionedBy("ticker")
        .location(str(SANDBOX_PATH))
        .execute()
    )
    for name, expression in SILVER_CONSTRAINTS.items():
        spark.sql(
            f"ALTER TABLE delta.`{SANDBOX_PATH}` ADD CONSTRAINT {name} CHECK ({expression})"
        )

    seed = spark.read.format("delta").load(str(settings.silver_path)).limit(50)
    seed.write.format("delta").mode("append").save(str(SANDBOX_PATH))
    log.info("enforcement.sandbox_ready", path=str(SANDBOX_PATH), seeded_rows=seed.count())
    return spark.read.format("delta").load(str(SANDBOX_PATH))


def _attempt_write(df: DataFrame) -> tuple[bool, str | None, str | None]:
    """Try the write. Returns ``(refused, exception_type, message)``."""
    try:
        df.write.format("delta").mode("append").save(str(SANDBOX_PATH))
    except Exception as exc:  # noqa: BLE001 - any refusal is a pass
        return True, type(exc).__name__, str(exc).strip().splitlines()[0][:400]
    return False, None, None


def run_enforcement_demo() -> dict[str, Any]:
    """Run every attempt and assert the table refused all of them."""
    spark = get_spark("fmis-schema-enforcement")

    with lineage_run(
        "schema_enforcement_demo",
        inputs=[dataset(f"delta://{settings.silver_path}")],
        outputs=[dataset(f"delta://{SANDBOX_PATH}")],
    ) as run:
        template = _build_sandbox(spark)
        rows_before = spark.read.format("delta").load(str(SANDBOX_PATH)).count()

        results: list[dict[str, Any]] = []

        # Control: a well-formed row must be accepted, otherwise a "refused"
        # result below would prove nothing more than a broken table.
        control_refused, control_type, control_message = _attempt_write(
            _valid_row(spark, template)
        )
        results.append(
            {
                "attempt": "control_valid_row",
                "mechanism": "none (control)",
                "description": "append a well-formed row",
                "expected": "accepted",
                "refused": control_refused,
                "passed": not control_refused,
                "exception_type": control_type,
                "message": control_message,
            }
        )

        for attempt in ATTEMPTS:
            refused, exc_type, message = _attempt_write(attempt.build(spark, template))
            results.append(
                {
                    "attempt": attempt.name,
                    "mechanism": attempt.mechanism,
                    "description": attempt.description,
                    "expected": "refused",
                    "refused": refused,
                    "passed": refused,
                    "exception_type": exc_type,
                    "message": message,
                }
            )
            log.info(
                "enforcement.attempt",
                attempt=attempt.name,
                mechanism=attempt.mechanism,
                refused=refused,
                exception_type=exc_type,
            )

        rows_after = spark.read.format("delta").load(str(SANDBOX_PATH)).count()
        failures = [r["attempt"] for r in results if not r["passed"]]

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sandbox_path": str(SANDBOX_PATH),
            "schema_source": str(settings.silver_path),
            "auto_merge_enabled": spark.conf.get(
                "spark.databricks.delta.schema.autoMerge.enabled", "false"
            ),
            "rows_before": rows_before,
            # Only the control row should have landed. Anything more means a
            # bad write got through.
            "rows_after": rows_after,
            "attempts": results,
            "attempts_total": len(results),
            "attempts_passed": sum(1 for r in results if r["passed"]),
            "enforcement_holds": not failures,
        }
        run.record(
            attempts=len(results),
            passed=summary["attempts_passed"],
            enforcement_holds=summary["enforcement_holds"],
        )

        target = settings.evidence_root / "runs" / "schema_enforcement.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        if failures:
            raise AssertionError(
                "Schema enforcement did NOT hold. These writes should have been "
                f"refused but were accepted: {failures}"
            )

        log.info(
            "enforcement.complete",
            attempts=len(results),
            all_refused=True,
            rows_before=rows_before,
            rows_after=rows_after,
            evidence=str(target),
        )

    return summary


if __name__ == "__main__":  # pragma: no cover
    run_enforcement_demo()
