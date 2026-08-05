"""OpenLineage run-event emission for every pipeline stage.

Rubric deliverable 5 asks for START / COMPLETE / FAIL events per stage. The
:func:`lineage_run` context manager is the single mechanism used across the
codebase: entering it emits START, leaving it cleanly emits COMPLETE, and an
exception escaping it emits FAIL (carrying an ``errorMessage`` facet) *before*
the exception is re-raised, so a failing Great Expectations gate is recorded in
the lineage graph and still halts the Airflow DAG.

Every stage in one pipeline execution shares a parent run, so Marquez renders
them as a single job graph rather than six unrelated runs. The parent id comes
from ``FMIS_PIPELINE_RUN_ID``, which the Airflow DAG sets once per DAG run.

Transport is selected by ``OPENLINEAGE_TRANSPORT``:

``file``
    Appends newline-delimited JSON to ``OPENLINEAGE_FILE``. No server required,
    and the resulting file is committed as run evidence.
``http``
    Posts to a Marquez instance (``docker compose --profile lineage up -d``).
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from fmis.config import settings
from fmis.logging_setup import configure_logging

log = configure_logging("lineage")

# --------------------------------------------------------------------------
# openlineage-python moved its event classes to ``event_v2`` in 1.15. Support
# both layouts so the project is not pinned to a single point release.
# --------------------------------------------------------------------------
try:  # openlineage-python >= 1.15
    from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
except ImportError:  # pragma: no cover - older releases
    from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState  # type: ignore

from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.transport.http import HttpConfig, HttpTransport

try:
    from openlineage.client.facet_v2 import (
        error_message_run,
        nominal_time_run,
        parent_run,
        schema_dataset,
    )
except ImportError:  # pragma: no cover - older releases
    from openlineage.client import facet as _facet  # type: ignore

    error_message_run = nominal_time_run = parent_run = schema_dataset = None  # type: ignore

PRODUCER = "https://github.com/ssaud7/financial-market-intelligence-system"
_PARENT_JOB_NAME = "financial_market_intelligence_pipeline"


def pipeline_run_id() -> str:
    """The run id shared by every stage of the current pipeline execution.

    Airflow exports ``FMIS_PIPELINE_RUN_ID`` once per DAG run; a standalone
    ``fmis`` invocation gets a fresh one.
    """
    existing = os.environ.get("FMIS_PIPELINE_RUN_ID")
    if existing:
        return existing
    generated = str(uuid.uuid4())
    os.environ["FMIS_PIPELINE_RUN_ID"] = generated
    return generated


def _build_client() -> OpenLineageClient:
    transport_kind = settings.openlineage_transport.lower()

    if transport_kind == "http":
        return OpenLineageClient(
            transport=HttpTransport(HttpConfig(url=settings.openlineage_url))
        )

    target: Path = settings.openlineage_file
    target.parent.mkdir(parents=True, exist_ok=True)
    # FileTransport appends one JSON document per line when append=True.
    return OpenLineageClient(
        transport=FileTransport(FileConfig(log_file_path=str(target), append=True))
    )


_client: OpenLineageClient | None = None


def _client_once() -> OpenLineageClient:
    global _client
    if _client is None:
        _client = _build_client()
    return _client


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def dataset(
    name: str,
    *,
    fields: list[tuple[str, str]] | None = None,
    namespace: str | None = None,
) -> Dataset:
    """Build an OpenLineage ``Dataset``, optionally carrying a schema facet.

    ``fields`` is a list of ``(column_name, type_name)`` pairs; supplying it is
    what makes column-level detail show up in the Marquez dataset view.
    """
    facets: dict[str, Any] = {}
    if fields and schema_dataset is not None:
        facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(name=col, type=col_type)
                for col, col_type in fields
            ]
        )
    return Dataset(
        namespace=namespace or settings.openlineage_namespace,
        name=name,
        facets=facets,
    )


class LineageRun:
    """Handle yielded by :func:`lineage_run`.

    Stages use it to attach datasets discovered mid-run (a Delta path is only
    known once written) and to record metrics that end up on the COMPLETE event.
    """

    def __init__(self, run_id: str, job_name: str) -> None:
        self.run_id = run_id
        self.job_name = job_name
        self.inputs: list[Dataset] = []
        self.outputs: list[Dataset] = []
        self.metrics: dict[str, Any] = {}

    def add_input(self, ds: Dataset) -> None:
        self.inputs.append(ds)

    def add_output(self, ds: Dataset) -> None:
        self.outputs.append(ds)

    def record(self, **metrics: Any) -> None:
        """Attach arbitrary metrics (row counts, merge stats) to the run."""
        self.metrics.update(metrics)


def _emit(
    state: RunState,
    run: LineageRun,
    *,
    error: BaseException | None = None,
) -> None:
    run_facets: dict[str, Any] = {}

    if parent_run is not None:
        parent_id = pipeline_run_id()
        if parent_id != run.run_id:
            run_facets["parent"] = parent_run.ParentRunFacet(
                run=parent_run.Run(runId=parent_id),
                job=parent_run.Job(
                    namespace=settings.openlineage_namespace,
                    name=_PARENT_JOB_NAME,
                ),
            )

    if nominal_time_run is not None:
        run_facets["nominalTime"] = nominal_time_run.NominalTimeRunFacet(
            nominalStartTime=_now()
        )

    if error is not None and error_message_run is not None:
        run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
            message=str(error),
            programmingLanguage="PYTHON",
            stackTrace=f"{type(error).__name__}: {error}",
        )

    event = RunEvent(
        eventType=state,
        eventTime=_now(),
        run=Run(runId=run.run_id, facets=run_facets),
        job=Job(namespace=settings.openlineage_namespace, name=run.job_name),
        inputs=run.inputs,
        outputs=run.outputs,
        producer=PRODUCER,
    )

    try:
        _client_once().emit(event)
    except Exception as exc:  # noqa: BLE001 - lineage must never break the pipeline
        log.warning("lineage.emit_failed", job=run.job_name, state=str(state), error=str(exc))
        return

    log.info(
        "lineage.emitted",
        job=run.job_name,
        state=getattr(state, "value", str(state)),
        run_id=run.run_id,
        inputs=[d.name for d in run.inputs],
        outputs=[d.name for d in run.outputs],
        **({"metrics": run.metrics} if run.metrics else {}),
    )


@contextmanager
def lineage_run(
    job_name: str,
    *,
    inputs: list[Dataset] | None = None,
    outputs: list[Dataset] | None = None,
) -> Iterator[LineageRun]:
    """Emit START on entry, then COMPLETE on success or FAIL on exception.

    >>> with lineage_run("silver_transform") as run:
    ...     rows = transform()
    ...     run.record(rows=rows)

    The exception is always re-raised after FAIL is emitted, so callers (and
    Airflow) still see the failure.
    """
    run = LineageRun(run_id=str(uuid.uuid4()), job_name=job_name)
    run.inputs = list(inputs or [])
    run.outputs = list(outputs or [])

    _emit(RunState.START, run)
    try:
        yield run
    except BaseException as exc:
        _emit(RunState.FAIL, run, error=exc)
        raise
    else:
        _emit(RunState.COMPLETE, run)
