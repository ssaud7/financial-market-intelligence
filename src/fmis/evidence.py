"""Writing run artefacts to ``evidence/runs/``.

The rubric is explicit that executed output is what earns a deliverable, so
every stage leaves behind a machine-readable record of what it did. Stages call
:func:`write_run_evidence` rather than each assembling its own path and JSON
handling, which keeps the artefacts consistently shaped and consistently placed.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fmis.config import settings
from fmis.logging_setup import configure_logging

log = configure_logging("evidence")


def write_run_evidence(name: str, payload: dict[str, Any]) -> Path:
    """Write ``payload`` to ``evidence/runs/<name>.json`` with a timestamp.

    ``default=str`` keeps this total: Delta history carries timestamps and
    Decimals that json cannot serialise natively, and losing an entire evidence
    file to a type error would be a poor trade for strictness.
    """
    target = settings.evidence_root / "runs" / f"{name}.json"
    target.parent.mkdir(parents=True, exist_ok=True)

    document = {"generated_at": datetime.now(timezone.utc).isoformat(), **payload}
    target.write_text(json.dumps(document, indent=2, default=str), encoding="utf-8")

    log.info("evidence.written", artefact=name, path=str(target))
    return target
