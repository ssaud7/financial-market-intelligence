"""Kafka producer: streams daily S&P 500 quotes onto ``market.quotes.raw``.

This is a real ``confluent_kafka.Producer`` writing to a real broker — messages
are keyed by ticker so every quote for a symbol lands on the same partition and
preserves per-ticker ordering, which is what makes the downstream MERGE on the
ticker business key well-defined.

A configurable fraction of the stream is deliberately corrupted (see
:mod:`fmis.ingestion.corruption`) so the consumer's data contract and the
dead-letter path can be shown working rather than merely asserted.
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from fmis.config import settings
from fmis.ingestion import kafka_admin
from fmis.ingestion.corruption import (
    HEADER_EXPECTED_REASON,
    HEADER_FAULT,
    pick,
)
from fmis.ingestion.source import iter_quote_payloads, load_quotes
from fmis.lineage import dataset, lineage_run
from fmis.logging_setup import configure_logging

log = configure_logging("producer")


class ProducerStats:
    """Tallies for the run summary written to ``evidence/runs/``."""

    def __init__(self) -> None:
        self.produced = 0
        self.clean = 0
        self.corrupted = 0
        self.delivery_failures = 0
        self.faults_injected: dict[str, int] = {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "produced": self.produced,
            "clean": self.clean,
            "corrupted": self.corrupted,
            "delivery_failures": self.delivery_failures,
            "faults_injected": dict(sorted(self.faults_injected.items())),
        }


def _delivery_report(stats: ProducerStats):
    def callback(err, msg) -> None:
        if err is not None:
            stats.delivery_failures += 1
            log.error("producer.delivery_failed", error=str(err), key=_decode(msg.key()))

    return callback


def _decode(value: bytes | None) -> str | None:
    return value.decode("utf-8", errors="replace") if value else None


def run_producer(
    *,
    max_records: int | None = None,
    corruption_rate: float | None = None,
    tickers: list[str] | None = None,
    seed: int = 7,
    flush_every: int = 2_000,
) -> ProducerStats:
    """Stream the source dataset to Kafka and return the run tallies."""
    max_records = max_records if max_records is not None else settings.producer_max_records
    corruption_rate = (
        corruption_rate if corruption_rate is not None else settings.producer_corruption_rate
    )
    # Seeding the module-level RNG makes both the corruption decision and the
    # choice of fault reproducible for a given seed.
    random.seed(seed)

    settings.ensure_directories()
    kafka_admin.wait_for_broker()
    kafka_admin.ensure_topics()

    stats = ProducerStats()
    started = time.monotonic()

    with lineage_run(
        "kafka_producer",
        outputs=[dataset(f"kafka://{settings.kafka_topic_quotes}")],
    ) as run:
        df = load_quotes(tickers=tickers)
        run.add_input(dataset(f"file://{settings.data_root / 'raw'}"))

        producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "fmis-quote-producer",
                # acks=all: do not count a quote as produced until the broker
                # has durably accepted it.
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "batch.size": 64 * 1024,
                "compression.type": "snappy",
            }
        )
        on_delivery = _delivery_report(stats)

        for record in iter_quote_payloads(df):
            if stats.produced >= max_records:
                break

            payload: dict[str, Any] | bytes = dict(record)
            headers: list[tuple[str, bytes]] = []
            key = str(record.get("ticker") or "UNKNOWN")

            if random.random() < corruption_rate:
                fault = pick()
                payload = fault.apply(dict(record))
                headers = [
                    (HEADER_FAULT, fault.name.encode()),
                    (HEADER_EXPECTED_REASON, fault.expected_reason.value.encode()),
                ]
                stats.corrupted += 1
                stats.faults_injected[fault.name] = stats.faults_injected.get(fault.name, 0) + 1
            else:
                stats.clean += 1

            body = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")

            # BufferError means the local queue is full; drain and retry once.
            try:
                producer.produce(
                    topic=settings.kafka_topic_quotes,
                    key=key.encode("utf-8"),
                    value=body,
                    headers=headers or None,
                    on_delivery=on_delivery,
                )
            except BufferError:
                producer.flush(30)
                producer.produce(
                    topic=settings.kafka_topic_quotes,
                    key=key.encode("utf-8"),
                    value=body,
                    headers=headers or None,
                    on_delivery=on_delivery,
                )

            stats.produced += 1

            if stats.produced % flush_every == 0:
                producer.poll(0)
                log.info("producer.progress", produced=stats.produced)

        remaining = producer.flush(120)
        if remaining:
            log.error("producer.flush_incomplete", undelivered=remaining)
            raise RuntimeError(f"{remaining} messages were never delivered to Kafka")

        elapsed = time.monotonic() - started
        run.record(**stats.as_dict(), elapsed_seconds=round(elapsed, 2))

        log.info(
            "producer.finished",
            **stats.as_dict(),
            elapsed_seconds=round(elapsed, 2),
            rate_per_second=round(stats.produced / elapsed, 1) if elapsed else None,
        )

    _write_summary(stats)
    return stats


def _write_summary(stats: ProducerStats) -> None:
    target: Path = settings.evidence_root / "runs" / "producer_summary.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "topic": settings.kafka_topic_quotes,
                "corruption_rate": settings.producer_corruption_rate,
                **stats.as_dict(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    log.info("producer.summary_written", path=str(target))


if __name__ == "__main__":  # pragma: no cover
    run_producer()
