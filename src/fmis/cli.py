"""Command-line entry point for every pipeline stage.

Each stage is a separate command with no shared in-process state, which is what
lets the Airflow DAG invoke them as independent tasks: a stage that fails exits
non-zero, Airflow marks the task failed, and downstream tasks never run.

    fmis --help
    fmis produce --max-records 5000
    fmis quality-gate --taint      # prove the gate halts the pipeline
"""

from __future__ import annotations

import json
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from fmis.config import settings

app = typer.Typer(
    add_completion=False,
    help="Financial Market Intelligence System — SDAIA Academy capstone pipeline.",
)
console = Console()


def _emit(title: str, payload: dict) -> None:
    console.print(f"\n[bold green]{title}[/bold green]")
    console.print_json(json.dumps(payload, default=str))


# --------------------------------------------------------------------------
# Stage 1 — ingestion
# --------------------------------------------------------------------------

@app.command("kafka-setup")
def kafka_setup() -> None:
    """Wait for the broker and create the quotes and dead-letter topics."""
    from fmis.ingestion import kafka_admin

    kafka_admin.wait_for_broker()
    kafka_admin.ensure_topics()
    _emit("Topics ready", kafka_admin.topic_counts())


@app.command("produce")
def produce(
    max_records: Optional[int] = typer.Option(None, help="Cap the number of quotes streamed."),
    corruption_rate: Optional[float] = typer.Option(
        None, min=0.0, max=1.0, help="Fraction of payloads to deliberately corrupt."
    ),
    seed: int = typer.Option(7, help="Seed for reproducible corruption injection."),
) -> None:
    """Stream S&P 500 daily quotes to Kafka, corrupting a slice of them."""
    from fmis.ingestion.producer import run_producer

    stats = run_producer(max_records=max_records, corruption_rate=corruption_rate, seed=seed)
    _emit("Producer finished", stats.as_dict())


@app.command("consume")
def consume(
    max_messages: Optional[int] = typer.Option(None, help="Stop after N messages."),
    idle_timeout: float = typer.Option(15.0, help="Exit after this many idle seconds."),
) -> None:
    """Validate every message against the data contract; route failures to the DLQ."""
    from fmis.ingestion.consumer import run_consumer

    stats = run_consumer(max_messages=max_messages, idle_timeout_s=idle_timeout)
    _emit("Consumer finished", stats.as_dict())


@app.command("contract-report")
def contract_report() -> None:
    """Score the data contract against the faults the producer injected."""
    from fmis.ingestion.report import build_scorecard

    scorecard = build_scorecard()

    table = Table(title="Data contract scorecard")
    for column in ("Injected fault", "Injected", "Quarantined", "Escaped", "Catch rate"):
        table.add_column(column, justify="right" if column != "Injected fault" else "left")
    for row in scorecard["per_fault"]:
        table.add_row(
            row["fault"],
            str(row["injected"]),
            str(row["quarantined"]),
            str(row["escaped"]),
            str(row["catch_rate"]),
        )
    console.print(table)
    console.print(f"Overall catch rate: [bold]{scorecard['contract_catch_rate']}[/bold]")

    if scorecard["faults_escaped_total"]:
        raise typer.Exit(code=1)


# --------------------------------------------------------------------------
# Stage 2 — lakehouse
# --------------------------------------------------------------------------

@app.command("bronze")
def bronze() -> None:
    """Land the validated JSON feed into the Bronze Delta table."""
    from fmis.lakehouse.bronze import load_bronze

    _emit("Bronze loaded", load_bronze())


@app.command("silver")
def silver() -> None:
    """Parse, type and deduplicate Bronze into Silver."""
    from fmis.lakehouse.silver import build_silver

    _emit("Silver built", build_silver())


@app.command("gold")
def gold() -> None:
    """Compute the per-ticker aggregate and MERGE it into Gold."""
    from fmis.lakehouse.gold import merge_gold

    _emit("Gold merged", merge_gold())


@app.command("enforce-schema")
def enforce_schema() -> None:
    """Prove that mismatched and invariant-breaking writes are refused."""
    from fmis.lakehouse.schema_enforcement import run_enforcement_demo

    summary = run_enforcement_demo()

    table = Table(title="Write-level enforcement")
    for column in ("Attempt", "Mechanism", "Expected", "Refused", "Exception"):
        table.add_column(column)
    for attempt in summary["attempts"]:
        table.add_row(
            attempt["attempt"],
            attempt["mechanism"],
            attempt["expected"],
            "yes" if attempt["refused"] else "no",
            attempt["exception_type"] or "-",
        )
    console.print(table)


# --------------------------------------------------------------------------
# Stage 4 — quality gate
# --------------------------------------------------------------------------

@app.command("quality-gate")
def quality_gate(
    taint: bool = typer.Option(
        False,
        "--taint",
        help="Inject violations in memory to demonstrate the gate halting the pipeline.",
    ),
) -> None:
    """Validate Silver with Great Expectations. Non-zero exit halts the DAG."""
    from fmis.quality.gate import QualityGateFailure, run_quality_gate

    try:
        summary = run_quality_gate(taint=taint)
    except QualityGateFailure as exc:
        console.print(f"[bold red]QUALITY GATE FAILED[/bold red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"[bold green]Quality gate passed[/bold green]: "
        f"{summary['successful']}/{summary['evaluated']} expectations, "
        f"{summary['rows_validated']} rows validated."
    )


# --------------------------------------------------------------------------
# Stage 3 — RAG
# --------------------------------------------------------------------------

@app.command("rag-index")
def rag_index(
    reset: bool = typer.Option(
        False,
        "--reset",
        help=(
            "Wipe the vector store and re-embed everything. Only needed after "
            "changing the embedding model or chunking parameters — by default "
            "unchanged chunks are reused, which makes a re-index near-instant."
        ),
    ),
    filing_limit: Optional[int] = typer.Option(None, help="Index at most N filings."),
) -> None:
    """Parse, chunk, embed and index the 10-K filings (incremental by default)."""
    from fmis.rag.index import build_index

    _emit("Index built", build_index(reset=reset, filing_limit=filing_limit))


@app.command("rag-ask")
def rag_ask(question: str = typer.Argument(..., help="An investor question.")) -> None:
    """Answer one question, grounded in retrieved filing passages, with citations."""
    from fmis.rag.answer import ensure_model_available
    from fmis.rag.pipeline import answer_question

    ensure_model_available()
    result = answer_question(question)

    console.print(f"\n[bold]{result['question']}[/bold]\n")
    console.print(result["answer"])
    console.print("\n[bold]Sources[/bold]")
    for citation in result["citations"]:
        console.print(f"  {citation['marker']} {citation['citation_label']} "
                      f"({citation['source_file']})")
    if not result["citations"]:
        console.print("  [yellow]none — the model declined to answer from context[/yellow]")
    for warning in result["warnings"]:
        console.print(f"  [yellow]warning:[/yellow] {warning}")


@app.command("rag-demo")
def rag_demo() -> None:
    """Answer the demonstration investor questions and write the evidence bundle."""
    from fmis.rag.pipeline import run_demo

    _emit("RAG demo complete", run_demo()["summary"])


# --------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------

@app.command("config")
def show_config() -> None:
    """Print the resolved configuration (secrets omitted)."""
    payload = settings.model_dump()
    _emit("Resolved configuration", {k: str(v) for k, v in payload.items()})


@app.command("lineage-summary")
def lineage_summary() -> None:
    """Summarise the OpenLineage events emitted so far."""
    from collections import Counter

    path = settings.openlineage_file
    if not path.exists():
        console.print(f"[yellow]No lineage file at {path}[/yellow]")
        raise typer.Exit(code=1)

    states: Counter[str] = Counter()
    jobs: Counter[str] = Counter()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            states[event.get("eventType", "?")] += 1
            jobs[f"{event.get('job', {}).get('name', '?')}"] += 1

    table = Table(title=f"OpenLineage events ({path.name})")
    table.add_column("Job")
    table.add_column("Events", justify="right")
    for job, count in sorted(jobs.items()):
        table.add_row(job, str(count))
    console.print(table)
    console.print(f"By state: {dict(states)}")


if __name__ == "__main__":  # pragma: no cover
    app()
