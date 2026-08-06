"""Airflow DAG wiring all five capstone stages into one orchestrated pipeline.

Two DAGs are defined here:

``financial_market_intelligence``
    The production pipeline: ingest → Bronze → Silver → **quality gate** → Gold,
    with the RAG index refresh running in parallel and joining at the end.

``financial_market_intelligence_gate_failure_demo``
    The same shape, but the gate runs against deliberately tainted data. Its
    purpose is to *prove* the halt: the gate task fails, and every downstream
    task — the Gold merge included — is skipped rather than executed.

**Why BashOperator rather than PythonOperator.** Airflow resolves its
dependencies against an official constraints file that cannot co-exist with
PySpark, Great Expectations and ChromaDB in one environment. The pipeline
therefore lives in its own virtualenv and Airflow shells out to its CLI. This is
the standard production separation, and it has a useful property here: each
stage is a real process whose exit code is the task's verdict, so a failing
quality gate cannot be swallowed by an exception handler somewhere up the stack.

**How the halt works.** ``fmis quality-gate`` exits non-zero when any expectation
fails. Airflow's default ``all_success`` trigger rule means ``gold_merge``, which
is declared downstream of it, is never scheduled. Bad data cannot reach the
aggregate.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Resolved from the environment so the DAG works in a Codespace, a container, or
# a local checkout without editing the file.
REPO_ROOT = os.environ.get("FMIS_REPO_ROOT", "/workspaces/Finance")
FMIS = os.environ.get("FMIS_CLI", f"{REPO_ROOT}/.venv-app/bin/fmis")

DEFAULT_ARGS = {
    "owner": "sdaia-capstone",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=45),
    "depends_on_past": False,
}

# Every stage in one DAG run shares this id, so the OpenLineage events emitted
# by the pipeline code group into a single parent run in Marquez. The raw
# Airflow run id is passed through as-is; fmis.lineage hashes it into a
# deterministic UUID, which is what the OpenLineage spec requires.
STAGE_ENV = {
    "FMIS_PIPELINE_RUN_ID": "{{ run_id }}",
    "FMIS_REPO_ROOT": REPO_ROOT,
}


def stage(dag: DAG, task_id: str, command: str, *, doc: str, **kwargs) -> BashOperator:
    """A pipeline stage: one CLI invocation whose exit code is the verdict."""
    operator = BashOperator(
        task_id=task_id,
        bash_command=f"cd {REPO_ROOT} && {FMIS} {command}",
        env={**os.environ, **STAGE_ENV},
        append_env=False,
        dag=dag,
        **kwargs,
    )
    operator.doc_md = doc
    return operator


# ==========================================================================
# Production pipeline
# ==========================================================================

with DAG(
    dag_id="financial_market_intelligence",
    description="Kafka ingestion → Delta medallion → quality gate → Gold merge, with RAG refresh",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # triggered manually; the source is a historical export
    catchup=False,
    max_active_runs=1,
    tags=["sdaia", "capstone", "kafka", "delta", "rag"],
) as pipeline:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    # ---- Stage 1: ingestion ------------------------------------------
    kafka_setup = stage(
        pipeline,
        "kafka_setup",
        "kafka-setup",
        doc="Wait for the broker and create the quotes and dead-letter topics.",
    )
    produce_quotes = stage(
        pipeline,
        "produce_quotes",
        "produce",
        doc="Stream daily S&P 500 quotes to Kafka, corrupting a configurable slice.",
    )
    consume_and_validate = stage(
        pipeline,
        "consume_and_validate",
        "consume",
        doc=(
            "Enforce the Pydantic data contract at the ingestion boundary. "
            "Valid records land in the landing zone; rejects go to the "
            "dead-letter topic **and** the quarantine zone with a reason code."
        ),
    )
    contract_report = stage(
        pipeline,
        "contract_scorecard",
        "contract-report",
        doc="Score the contract against the injected faults. Fails if any fault escaped.",
    )

    # ---- Stage 2: lakehouse ------------------------------------------
    bronze_load = stage(
        pipeline,
        "bronze_load",
        "bronze",
        doc="Land the raw JSON feed verbatim into the Bronze Delta table.",
    )
    silver_transform = stage(
        pipeline,
        "silver_transform",
        "silver",
        doc="Parse, cast, deduplicate and MERGE into Silver on (ticker, trade_date).",
    )
    schema_enforcement = stage(
        pipeline,
        "schema_enforcement_demo",
        "enforce-schema",
        doc="Prove mismatched writes and constraint violations are refused at write time.",
    )

    # ---- Stage 4: the gate -------------------------------------------
    # Everything downstream of this task depends on it succeeding.
    quality_gate = stage(
        pipeline,
        "quality_gate",
        "quality-gate",
        doc=(
            "**The gate.** Great Expectations validates Silver — `high >= low` "
            "among them. A non-zero exit fails this task, and the default "
            "`all_success` trigger rule means `gold_merge` is never scheduled."
        ),
        retries=0,  # a data-quality failure is not a transient error; do not retry
    )

    gold_merge = stage(
        pipeline,
        "gold_merge",
        "gold",
        doc=(
            "Compute the per-ticker aggregate (30-session moving average, "
            "annualised volatility, 52-week range) and MERGE it into Gold on "
            "the ticker business key."
        ),
    )

    # ---- Stage 3: RAG (parallel branch) ------------------------------
    rag_index = stage(
        pipeline,
        "rag_index_refresh",
        "rag-index",
        doc="Chunk the 10-K filings, embed them into Chroma, and rebuild the BM25 corpus.",
    )
    rag_query = stage(
        pipeline,
        "rag_answer_demo",
        "rag-demo",
        doc="Answer the investor questions with hybrid retrieval, RRF, reranking and citations.",
    )

    lineage_summary = stage(
        pipeline,
        "lineage_summary",
        "lineage-summary",
        doc="Summarise the START/COMPLETE/FAIL events emitted across the run.",
    )

    # ---- Dependencies ------------------------------------------------
    start >> kafka_setup >> produce_quotes >> consume_and_validate
    consume_and_validate >> [contract_report, bronze_load]
    bronze_load >> silver_transform >> [schema_enforcement, quality_gate]

    # The gate stands between Silver and Gold. This edge is the whole point.
    quality_gate >> gold_merge

    start >> rag_index >> rag_query

    [contract_report, schema_enforcement, gold_merge, rag_query] >> lineage_summary
    lineage_summary >> finish


# ==========================================================================
# Failure-path demonstration
# ==========================================================================

with DAG(
    dag_id="financial_market_intelligence_gate_failure_demo",
    description="Proves a failed quality gate halts the pipeline before Gold is merged",
    default_args={**DEFAULT_ARGS, "retries": 0},
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sdaia", "capstone", "failure-path"],
) as failure_demo:

    demo_start = EmptyOperator(task_id="start")

    failing_gate = stage(
        failure_demo,
        "quality_gate_tainted",
        "quality-gate --taint",
        doc=(
            "Runs the same Great Expectations suite against Silver with "
            "violations injected **in memory only** — an inverted high/low, a "
            "null close, a duplicated business key. Nothing is written to Delta.\n\n"
            "This task is **expected to fail.** That failure is the deliverable: "
            "it demonstrates the gate halting the pipeline."
        ),
    )

    would_be_gold = stage(
        failure_demo,
        "gold_merge_must_not_run",
        "gold",
        doc=(
            "Declared downstream of the failing gate. With the default "
            "`all_success` trigger rule, Airflow marks this task **skipped** and "
            "never executes it — bad data never reaches the aggregate."
        ),
    )

    demo_finish = EmptyOperator(task_id="finish")

    demo_start >> failing_gate >> would_be_gold >> demo_finish
