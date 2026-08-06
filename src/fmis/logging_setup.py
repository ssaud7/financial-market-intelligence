"""Structured logging shared by every stage.

Console output stays human-readable for the terminal, while a JSONL copy of
every event is appended to ``evidence/logs/pipeline.jsonl`` so a completed run
leaves behind machine-readable proof of what happened — including the rejection
reason for each quarantined record.

**One file, not one per stage.** structlog routes through a single root logger,
so every handler receives every record regardless of which module emitted it.
Naming the file after the calling stage therefore produced a file whose name
described only whichever stage happened to initialise logging first in that
process — RAG events landing in ``lineage.jsonl``, for instance. Every record
already carries a ``stage`` field, so filtering is a ``grep`` away:

    jq -c 'select(.stage == "consumer")' evidence/logs/pipeline.jsonl
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import structlog

from fmis.config import settings

_CONFIGURED = False


def configure_logging(stage: str, level: int = logging.INFO) -> structlog.BoundLogger:
    """Configure structlog once per process and return a stage-bound logger."""
    global _CONFIGURED

    if not _CONFIGURED:
        # Tests exercise the same code paths as a real run, so without this a
        # `pytest` invocation appends synthetic records to the committed
        # evidence file and quietly corrupts the record of an actual run.
        under_test = "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules

        log_dir: Path = settings.evidence_root / "logs"
        if not under_test:
            log_dir.mkdir(parents=True, exist_ok=True)

        # Console handler: pretty. File handler: one JSON object per line.
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(
            logging.Formatter("%(message)s"),
        )
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(console)
        if not under_test:
            file_handler = logging.FileHandler(log_dir / "pipeline.jsonl", encoding="utf-8")
            file_handler.setFormatter(logging.Formatter("%(message)s"))
            root.addHandler(file_handler)
        root.setLevel(level)

        # Quieten the noisy dependencies so pipeline events stay readable.
        for noisy in ("py4j", "pyspark", "urllib3", "httpx", "chromadb", "great_expectations"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(level),
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
        _CONFIGURED = True

    return structlog.get_logger().bind(stage=stage)
