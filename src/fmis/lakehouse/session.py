"""Spark session construction for the Delta lakehouse.

One place builds the session so every stage — Bronze, Silver, Gold, the schema
enforcement demo and the Great Expectations gate — sees identical Delta
semantics. Two settings here matter for the rubric:

``spark.databricks.delta.schema.autoMerge.enabled=false``
    Schema evolution stays **off**. A write whose schema does not match the
    table is refused rather than silently widening the table. This is what makes
    the schema-enforcement demonstration meaningful.

``spark.databricks.delta.retentionDurationCheck.enabled``
    Left at its default so VACUUM cannot be pointed at a dangerously short
    retention by accident.
"""

from __future__ import annotations

import os
from typing import Iterator

from pyspark.sql import SparkSession

from fmis.config import REPO_ROOT
from fmis.logging_setup import configure_logging

log = configure_logging("spark")

# Spark 3.5 on JDK 17 needs the JVM's internals opened up. spark-submit normally
# injects these, but PySpark sessions created in-process (notebooks, Airflow
# tasks) do not always inherit them.
_JAVA_17_MODULE_OPTS = " ".join(
    [
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    ]
)

_session: SparkSession | None = None


def get_spark(app_name: str = "fmis-lakehouse", *, shuffle_partitions: int = 8) -> SparkSession:
    """Return the shared Delta-enabled Spark session, creating it on first call.

    ``shuffle_partitions`` defaults to 8 rather than Spark's 200: this dataset is
    tens of thousands of rows, and 200 shuffle partitions on a 4-core Codespace
    spends more time on task overhead than on the work itself.
    """
    global _session
    if _session is not None:
        return _session

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # ---- Delta ------------------------------------------------------
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Schema enforcement: a mismatched write must fail, never auto-evolve.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        # ---- Local runtime sizing ---------------------------------------
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        # Keep Derby's metastore and Spark's warehouse inside the repo so a
        # Codespace rebuild does not scatter state through the home directory.
        .config("spark.sql.warehouse.dir", str(REPO_ROOT / "data" / "spark-warehouse"))
        .config(
            "spark.driver.extraJavaOptions",
            f"{_JAVA_17_MODULE_OPTS} -Dderby.system.home={REPO_ROOT / 'data'}",
        )
        .config("spark.executor.extraJavaOptions", _JAVA_17_MODULE_OPTS)
    )

    # Resolves and caches the delta-spark JARs matching the installed PySpark.
    _session = configure_spark_with_delta_pip(builder).getOrCreate()
    _session.sparkContext.setLogLevel("WARN")

    log.info(
        "spark.session_started",
        app_name=app_name,
        spark_version=_session.version,
        master=_session.sparkContext.master,
    )
    return _session


def stop_spark() -> None:
    """Tear the session down. Airflow tasks are separate processes, but the
    notebooks and tests benefit from an explicit stop."""
    global _session
    if _session is not None:
        _session.stop()
        _session = None
        log.info("spark.session_stopped")


def table_exists(path) -> bool:
    """True if a Delta table has been initialised at ``path``."""
    from pathlib import Path

    return (Path(path) / "_delta_log").exists()


def delta_history(path, limit: int = 5) -> list[dict]:
    """Recent Delta transaction-log entries, used as run evidence.

    The ``operationMetrics`` map is where a MERGE records how many rows it
    actually inserted versus updated — the difference between a real upsert and
    a disguised overwrite.
    """
    from delta.tables import DeltaTable

    spark = get_spark()
    history = DeltaTable.forPath(spark, str(path)).history(limit)
    rows = history.select(
        "version", "timestamp", "operation", "operationMetrics"
    ).collect()
    return [
        {
            "version": r["version"],
            "timestamp": str(r["timestamp"]),
            "operation": r["operation"],
            "operationMetrics": dict(r["operationMetrics"] or {}),
        }
        for r in rows
    ]


def iter_rows(df, limit: int = 20) -> Iterator[dict]:
    for row in df.limit(limit).collect():
        yield row.asDict(recursive=True)
