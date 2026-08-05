"""The Great Expectations quality gate that stands between Silver and Gold.

This is a gate, not a report. If any expectation fails, :func:`run_quality_gate`
raises :class:`QualityGateFailure`, the Airflow task fails, and because the Gold
merge is declared downstream with the default ``all_success`` trigger rule it is
never scheduled. Bad data cannot reach the aggregate.

Failure is also recorded in lineage: the gate runs inside a ``lineage_run``
block, so a tripped gate emits an OpenLineage ``FAIL`` event carrying the
error before the exception propagates.

To demonstrate the halt without corrupting the lakehouse, pass ``taint=True``.
That mutates a handful of rows **in the in-memory frame only** — inverting a
session's high and low, nulling a close, duplicating a key — so the gate fails
on data that was never written anywhere.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fmis.config import settings
from fmis.lakehouse.session import get_spark, table_exists
from fmis.lineage import dataset, lineage_run
from fmis.logging_setup import configure_logging
from fmis.quality.suite import SUITE_NAME, build_specs

log = configure_logging("quality_gate")


class QualityGateFailure(RuntimeError):
    """Raised when Silver fails validation. Halts the pipeline."""

    def __init__(self, message: str, result: dict[str, Any]) -> None:
        super().__init__(message)
        self.result = result


def _taint(df: DataFrame) -> DataFrame:
    """Inject violations into the in-memory frame to prove the gate bites.

    Nothing here is written to Delta — the tainted frame exists only for the
    duration of the validation.
    """
    # Each of these trips a different expectation, so the failure report shows
    # the gate catching several distinct classes of defect at once.
    inverted = (
        df.orderBy(F.col("trade_date").desc())
        .limit(3)
        .withColumn("high", F.lit(1.0))
        .withColumn("low", F.lit(9_999.0))
    )
    nulled = (
        df.orderBy(F.col("trade_date").asc())
        .limit(1)
        .withColumn("close", F.lit(None).cast("double"))
    )
    duplicated = df.orderBy(F.col("trade_date").desc()).limit(1)

    log.warning(
        "gate.taint_injected",
        violations=["high_below_low", "null_close", "duplicate_ticker_trade_date"],
        note="in-memory only; nothing is persisted to Delta",
    )
    return df.unionByName(inverted).unionByName(nulled).unionByName(duplicated)


def _build_context_and_checkpoint(df: DataFrame, specs):
    """Wire up a GE context, suite, validation definition and checkpoint."""
    import great_expectations as gx
    from great_expectations import expectations as gxe

    # An ephemeral context keeps GE's own state out of the repo; the artefacts
    # that matter (the validation result) are written to evidence/ ourselves.
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_or_update_spark(name="fmis_spark")
    asset = data_source.add_dataframe_asset(name="silver_quotes")
    batch_definition = asset.add_batch_definition_whole_dataframe("silver_whole_table")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name=SUITE_NAME))
    for spec in specs:
        expectation_cls = getattr(gxe, spec.expectation, None)
        if expectation_cls is None:
            raise AttributeError(
                f"great_expectations.expectations has no {spec.expectation!r}. "
                "The installed Great Expectations version may differ from the one "
                "this suite was written against."
            )
        suite.add_expectation(expectation_cls(**spec.kwargs))

    validation_definition = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            data=batch_definition,
            suite=suite,
            name="silver_quotes_validation",
        )
    )
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name="silver_quality_gate",
            validation_definitions=[validation_definition],
        )
    )
    return context, checkpoint


def _summarise(checkpoint_result) -> dict[str, Any]:
    """Flatten GE's nested result into something loggable and committable."""
    summary: dict[str, Any] = {
        "success": bool(checkpoint_result.success),
        "evaluated": 0,
        "successful": 0,
        "failed_expectations": [],
        "results": [],
    }

    for validation_result in checkpoint_result.run_results.values():
        stats = validation_result.get("statistics", {}) or {}
        summary["evaluated"] += int(stats.get("evaluated_expectations", 0))
        summary["successful"] += int(stats.get("successful_expectations", 0))

        for item in validation_result.get("results", []):
            config = item.get("expectation_config", {}) or {}
            kwargs = dict(config.get("kwargs", {}) or {})
            kwargs.pop("batch_id", None)
            entry = {
                "expectation": config.get("type") or config.get("expectation_type"),
                "kwargs": kwargs,
                "success": bool(item.get("success")),
                "unexpected_count": (item.get("result", {}) or {}).get("unexpected_count"),
                "unexpected_percent": (item.get("result", {}) or {}).get("unexpected_percent"),
                "partial_unexpected_list": (item.get("result", {}) or {}).get(
                    "partial_unexpected_list"
                ),
            }
            summary["results"].append(entry)
            if not entry["success"]:
                summary["failed_expectations"].append(entry)

    return summary


def run_quality_gate(*, taint: bool = False, fail_on_error: bool = True) -> dict[str, Any]:
    """Validate Silver. Raise :class:`QualityGateFailure` if it does not pass."""
    spark = get_spark("fmis-quality-gate")

    if not table_exists(settings.silver_path):
        raise FileNotFoundError(
            f"No Silver table at {settings.silver_path}. The gate runs after the "
            "Silver transform and before the Gold merge."
        )

    job_name = "quality_gate_silver" + ("_failure_demo" if taint else "")

    with lineage_run(
        job_name,
        inputs=[dataset(f"delta://{settings.silver_path}")],
    ) as run:
        df = spark.read.format("delta").load(str(settings.silver_path))
        if taint:
            df = _taint(df)
        df.cache()
        row_count = df.count()

        specs = build_specs(known_tickers=settings.tickers)
        context, checkpoint = _build_context_and_checkpoint(df, specs)

        checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})
        summary = _summarise(checkpoint_result)
        summary.update(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "validated_table": str(settings.silver_path),
                "rows_validated": row_count,
                "tainted": taint,
                "suite": SUITE_NAME,
            }
        )
        df.unpersist()

        _write_evidence(summary, taint=taint)
        run.record(
            success=summary["success"],
            evaluated=summary["evaluated"],
            successful=summary["successful"],
            rows_validated=row_count,
        )

        if not summary["success"]:
            failed = [
                f"{f['expectation']}({', '.join(f'{k}={v}' for k, v in f['kwargs'].items())})"
                for f in summary["failed_expectations"]
            ]
            log.error(
                "gate.failed",
                evaluated=summary["evaluated"],
                successful=summary["successful"],
                failed=failed,
            )
            if fail_on_error:
                # Raised inside the lineage block so a FAIL event is emitted
                # before this propagates and halts the DAG.
                raise QualityGateFailure(
                    f"Quality gate failed: {len(summary['failed_expectations'])} of "
                    f"{summary['evaluated']} expectations did not pass. "
                    f"Gold merge will not run. Failures: {failed}",
                    summary,
                )

        log.info(
            "gate.passed",
            evaluated=summary["evaluated"],
            successful=summary["successful"],
            rows_validated=row_count,
        )

    return summary


def _write_evidence(summary: dict[str, Any], *, taint: bool) -> None:
    name = "quality_gate_failure_demo" if taint else "quality_gate"
    target = settings.evidence_root / "runs" / f"{name}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log.info("gate.evidence_written", path=str(target))


if __name__ == "__main__":  # pragma: no cover
    run_quality_gate()
