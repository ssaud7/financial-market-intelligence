"""Scores the data contract against the faults the producer actually injected.

Counting rejections proves the dead-letter path runs. It does not prove the
contract is *correct* — a contract that rejected everything would score just as
well. This module closes that gap by joining what the producer injected (from
its run summary) against what the consumer quarantined (from the audit block on
each quarantine record), and reports two things the rubric cares about:

* **Escapes** — an injected fault that was admitted anyway. Any escape is a hole
  in the contract.
* **Misclassifications** — a fault that was caught, but recorded under a
  different reason than expected. Not a correctness failure, but it means the
  reason code would mislead whoever triages the quarantine zone.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fmis.config import settings
from fmis.logging_setup import configure_logging

log = configure_logging("ingestion_report")


def _load_quarantine_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(settings.quarantine_path.glob("rejected_*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def build_scorecard() -> dict[str, Any]:
    producer_summary_path = settings.evidence_root / "runs" / "producer_summary.json"
    consumer_summary_path = settings.evidence_root / "runs" / "consumer_summary.json"

    if not producer_summary_path.exists() or not consumer_summary_path.exists():
        raise FileNotFoundError(
            "Run the producer and consumer before building the scorecard "
            f"(expected {producer_summary_path} and {consumer_summary_path})."
        )

    producer_summary = json.loads(producer_summary_path.read_text(encoding="utf-8"))
    consumer_summary = json.loads(consumer_summary_path.read_text(encoding="utf-8"))
    quarantined = _load_quarantine_records()

    injected: dict[str, int] = producer_summary.get("faults_injected", {})
    caught: Counter[str] = Counter()
    misclassified: list[dict[str, str]] = []

    for record in quarantined:
        audit = record.get("audit") or {}
        fault = audit.get("injected_fault")
        if not fault:
            # A rejection with no injected fault means the source data itself
            # breached the contract — worth surfacing, not an error.
            caught["<organic>"] += 1
            continue
        caught[fault] += 1
        expected = audit.get("expected_reason")
        actual = record.get("reason")
        if expected and actual and expected != actual:
            misclassified.append(
                {"fault": fault, "expected_reason": expected, "actual_reason": actual}
            )

    per_fault = []
    total_escaped = 0
    for fault, produced in sorted(injected.items()):
        caught_count = caught.get(fault, 0)
        escaped = produced - caught_count
        total_escaped += max(escaped, 0)
        per_fault.append(
            {
                "fault": fault,
                "injected": produced,
                "quarantined": caught_count,
                "escaped": escaped,
                "catch_rate": round(caught_count / produced, 4) if produced else None,
            }
        )

    total_injected = sum(injected.values())
    scorecard = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "messages_produced": producer_summary.get("produced"),
        "messages_consumed": consumer_summary.get("consumed"),
        "messages_accepted": consumer_summary.get("accepted"),
        "messages_rejected": consumer_summary.get("rejected"),
        "faults_injected_total": total_injected,
        "faults_escaped_total": total_escaped,
        "contract_catch_rate": (
            round((total_injected - total_escaped) / total_injected, 4) if total_injected else None
        ),
        "organic_rejections": caught.get("<organic>", 0),
        "per_fault": per_fault,
        "misclassified_sample": misclassified[:20],
        "misclassified_total": len(misclassified),
        "rejections_by_reason": consumer_summary.get("rejections_by_reason", {}),
    }

    target = settings.evidence_root / "runs" / "contract_scorecard.json"
    target.write_text(json.dumps(scorecard, indent=2), encoding="utf-8")
    _write_markdown(scorecard)

    log.info(
        "report.scorecard",
        injected=total_injected,
        escaped=total_escaped,
        catch_rate=scorecard["contract_catch_rate"],
        path=str(target),
    )
    return scorecard


def _write_markdown(scorecard: dict[str, Any]) -> None:
    """Render the scorecard as a table for the README and notebooks."""
    lines = [
        "# Data contract scorecard",
        "",
        f"Generated: {scorecard['generated_at']}",
        "",
        f"- Messages produced: **{scorecard['messages_produced']}**",
        f"- Messages consumed: **{scorecard['messages_consumed']}**",
        f"- Admitted to landing zone: **{scorecard['messages_accepted']}**",
        f"- Routed to quarantine + dead-letter topic: **{scorecard['messages_rejected']}**",
        f"- Injected faults escaping the contract: **{scorecard['faults_escaped_total']}**",
        f"- Contract catch rate: **{scorecard['contract_catch_rate']}**",
        "",
        "| Injected fault | Injected | Quarantined | Escaped | Catch rate |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in scorecard["per_fault"]:
        lines.append(
            f"| `{row['fault']}` | {row['injected']} | {row['quarantined']} "
            f"| {row['escaped']} | {row['catch_rate']} |"
        )

    lines += ["", "## Rejections by reason code", "", "| Reason | Count |", "| --- | ---: |"]
    for reason, count in sorted(scorecard["rejections_by_reason"].items()):
        lines.append(f"| `{reason}` | {count} |")

    target: Path = settings.evidence_root / "runs" / "contract_scorecard.md"
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":  # pragma: no cover
    build_scorecard()
