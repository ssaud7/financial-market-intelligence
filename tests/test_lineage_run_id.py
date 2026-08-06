"""The parent run id must always be a UUID, whatever Airflow hands us.

This is a regression test for a bug that only surfaced under Airflow: the DAG
exports its own ``run_id`` (``manual__2026-08-07T00:00:00+00:00``), which is not
a UUID, and OpenLineage rejected it with ``ValueError: badly formed hexadecimal
UUID string`` — failing the task. Every stage passed when run from the CLI,
because a CLI run generates a real UUID.
"""

from __future__ import annotations

import uuid

import pytest

from fmis.lineage.emitter import pipeline_run_id


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    monkeypatch.delenv("FMIS_PIPELINE_RUN_ID", raising=False)


def test_generates_a_uuid_when_unset():
    value = pipeline_run_id()
    uuid.UUID(value)  # raises if malformed


def test_a_real_uuid_passes_through(monkeypatch):
    original = str(uuid.uuid4())
    monkeypatch.setenv("FMIS_PIPELINE_RUN_ID", original)
    assert pipeline_run_id() == original


@pytest.mark.parametrize(
    "airflow_run_id",
    [
        "manual__2026-08-07T00:00:00+00:00",
        "scheduled__2026-08-06T12:30:00+00:00",
        "backfill__2026-01-01T00:00:00+00:00",
    ],
)
def test_airflow_run_ids_become_valid_uuids(monkeypatch, airflow_run_id):
    monkeypatch.setenv("FMIS_PIPELINE_RUN_ID", airflow_run_id)
    value = pipeline_run_id()
    uuid.UUID(value), "an Airflow run id must be converted, not passed through"
    assert value != airflow_run_id


def test_derivation_is_deterministic(monkeypatch):
    """Stages are separate processes; they must agree on the parent run id."""
    run_id = "manual__2026-08-07T00:00:00+00:00"
    monkeypatch.setenv("FMIS_PIPELINE_RUN_ID", run_id)
    assert pipeline_run_id() == pipeline_run_id()


def test_different_dag_runs_get_different_parents(monkeypatch):
    monkeypatch.setenv("FMIS_PIPELINE_RUN_ID", "manual__2026-08-07T00:00:00+00:00")
    first = pipeline_run_id()
    monkeypatch.setenv("FMIS_PIPELINE_RUN_ID", "manual__2026-08-08T00:00:00+00:00")
    assert pipeline_run_id() != first
