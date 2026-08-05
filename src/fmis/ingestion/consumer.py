"""Kafka consumer: the ingestion boundary where the data contract is enforced.

Every message is subjected to :class:`~fmis.ingestion.contracts.StockQuote`
before anything is written. There are exactly two outcomes:

* **Admitted** — appended as JSON to the landing zone, which the Bronze loader
  reads. Nothing reaches Bronze without having passed the contract.
* **Rejected** — written to *both* a dead-letter topic and an on-disk quarantine
  zone, each carrying the specific reason code, the field-level violations, and
  the original payload so the record can be replayed after the upstream fix.

Offsets are committed only after the batch has been durably written to the
landing and quarantine zones. That makes the stage at-least-once: a crash
mid-batch replays messages rather than losing them, and the duplicates are
absorbed by the idempotent MERGE in the Gold layer.
"""

from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import ValidationError

from fmis.config import settings
from fmis.ingestion import kafka_admin
from fmis.ingestion.contracts import RejectionReason, StockQuote, classify_rejection
from fmis.ingestion.corruption import HEADER_EXPECTED_REASON, HEADER_FAULT
from fmis.lineage import dataset, lineage_run
from fmis.logging_setup import configure_logging

log = configure_logging("consumer")


class ConsumerStats:
    def __init__(self) -> None:
        self.consumed = 0
        self.accepted = 0
        self.rejected = 0
        self.by_reason: Counter[str] = Counter()

    def as_dict(self) -> dict[str, Any]:
        return {
            "consumed": self.consumed,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "acceptance_rate": round(self.accepted / self.consumed, 4) if self.consumed else None,
            "rejections_by_reason": dict(sorted(self.by_reason.items())),
        }


def _headers_to_dict(headers) -> dict[str, str]:
    """Decode Kafka headers. Audit metadata only — never used for the verdict."""
    if not headers:
        return {}
    out: dict[str, str] = {}
    for key, value in headers:
        out[key] = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
    return out


def _validate(raw: bytes) -> tuple[StockQuote | None, RejectionReason | None, list[dict[str, Any]]]:
    """Apply the contract to one message body.

    Returns ``(quote, None, [])`` on success, or ``(None, reason, violations)``.
    Nothing about the message other than its body influences the outcome.
    """
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, RejectionReason.MALFORMED_JSON, [
            {
                "field": "<message>",
                "reason": RejectionReason.MALFORMED_JSON.value,
                "error_type": type(exc).__name__,
                "message": str(exc),
                "received": repr(raw[:200]),
            }
        ]

    if not isinstance(decoded, dict):
        return None, RejectionReason.WRONG_TYPE, [
            {
                "field": "<message>",
                "reason": RejectionReason.WRONG_TYPE.value,
                "error_type": "not_an_object",
                "message": f"expected a JSON object, received {type(decoded).__name__}",
                "received": repr(decoded)[:200],
            }
        ]

    try:
        return StockQuote.model_validate(decoded), None, []
    except ValidationError as exc:
        reason, violations = classify_rejection(exc)
        return None, reason, violations


def run_consumer(
    *,
    max_messages: int | None = None,
    idle_timeout_s: float = 15.0,
    batch_size: int = 1_000,
    reset_landing: bool = True,
) -> ConsumerStats:
    """Drain the quotes topic, enforcing the contract on every message.

    The loop exits once ``max_messages`` have been handled or the topic has been
    quiet for ``idle_timeout_s`` — the batch-shaped behaviour an Airflow task
    needs, as opposed to an endless streaming service.
    """
    settings.ensure_directories()
    kafka_admin.wait_for_broker()
    kafka_admin.ensure_topics()

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    valid_file = settings.landing_valid_path / f"quotes_{run_stamp}.jsonl"
    quarantine_file = settings.quarantine_path / f"rejected_{run_stamp}.jsonl"

    if reset_landing:
        # The Bronze loader consumes the whole landing directory, so a fresh run
        # starts from a clean slate rather than re-ingesting previous batches.
        _clear_directory(settings.landing_valid_path, "*.jsonl")

    stats = ConsumerStats()
    started = time.monotonic()

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            # Manual commits: offsets advance only after a durable write.
            "enable.auto.commit": False,
            "session.timeout.ms": 45_000,
            "max.poll.interval.ms": 600_000,
        }
    )
    dlq_producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "fmis-dlq-producer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    with lineage_run(
        "kafka_consumer_contract",
        inputs=[dataset(f"kafka://{settings.kafka_topic_quotes}")],
        outputs=[
            dataset(f"file://{settings.landing_valid_path}"),
            dataset(f"kafka://{settings.kafka_topic_dlq}"),
            dataset(f"file://{settings.quarantine_path}"),
        ],
    ) as run:
        try:
            consumer.subscribe([settings.kafka_topic_quotes])

            accepted_buffer: list[str] = []
            rejected_buffer: list[str] = []
            last_message_at = time.monotonic()

            while True:
                if max_messages is not None and stats.consumed >= max_messages:
                    log.info("consumer.max_messages_reached", max_messages=max_messages)
                    break

                message = consumer.poll(timeout=1.0)

                if message is None:
                    if time.monotonic() - last_message_at > idle_timeout_s:
                        log.info("consumer.idle_timeout", seconds=idle_timeout_s)
                        break
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())

                last_message_at = time.monotonic()
                stats.consumed += 1

                quote, reason, violations = _validate(message.value())

                if quote is not None:
                    stats.accepted += 1
                    record = quote.to_bronze_record()
                    record["_kafka"] = {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    }
                    accepted_buffer.append(json.dumps(record))
                else:
                    stats.rejected += 1
                    stats.by_reason[reason.value] += 1
                    envelope = _rejection_envelope(message, reason, violations)
                    rejected_buffer.append(json.dumps(envelope))
                    dlq_producer.produce(
                        topic=settings.kafka_topic_dlq,
                        key=message.key(),
                        value=json.dumps(envelope).encode("utf-8"),
                        headers=[("x-fmis-rejection-reason", reason.value.encode())],
                    )
                    log.warning(
                        "consumer.record_rejected",
                        reason=reason.value,
                        offset=message.offset(),
                        partition=message.partition(),
                        violations=[v["field"] for v in violations],
                        detail=violations[0]["message"] if violations else None,
                    )

                if len(accepted_buffer) + len(rejected_buffer) >= batch_size:
                    _flush(
                        consumer,
                        dlq_producer,
                        accepted_buffer,
                        rejected_buffer,
                        valid_file,
                        quarantine_file,
                    )
                    log.info("consumer.progress", **stats.as_dict())

            _flush(
                consumer,
                dlq_producer,
                accepted_buffer,
                rejected_buffer,
                valid_file,
                quarantine_file,
            )
        finally:
            dlq_producer.flush(60)
            consumer.close()

        elapsed = time.monotonic() - started
        run.record(**stats.as_dict(), elapsed_seconds=round(elapsed, 2))

        log.info(
            "consumer.finished",
            **stats.as_dict(),
            landing_file=str(valid_file),
            quarantine_file=str(quarantine_file),
            elapsed_seconds=round(elapsed, 2),
        )

    _write_summary(stats, valid_file, quarantine_file)

    if stats.consumed == 0:
        raise RuntimeError(
            f"No messages were read from {settings.kafka_topic_quotes}. "
            "Run the producer first, or reset the consumer group offsets."
        )

    return stats


def _rejection_envelope(message, reason: RejectionReason, violations: list[dict[str, Any]]) -> dict:
    headers = _headers_to_dict(message.headers())
    return {
        "rejected_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason.value,
        "violations": violations,
        "kafka": {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "key": message.key().decode("utf-8", errors="replace") if message.key() else None,
            "timestamp_ms": message.timestamp()[1],
        },
        # The original bytes, kept verbatim so the record can be replayed once
        # the upstream defect is fixed.
        "raw_payload": message.value().decode("utf-8", errors="replace"),
        # Audit provenance from the producer's headers. Recorded for scoring in
        # fmis.ingestion.report; it played no part in the verdict above.
        "audit": {
            "injected_fault": headers.get(HEADER_FAULT),
            "expected_reason": headers.get(HEADER_EXPECTED_REASON),
        },
    }


def _flush(
    consumer: Consumer,
    dlq_producer: Producer,
    accepted: list[str],
    rejected: list[str],
    valid_file: Path,
    quarantine_file: Path,
) -> None:
    """Persist both buffers, ensure the DLQ is drained, then commit offsets."""
    if accepted:
        _append_lines(valid_file, accepted)
        accepted.clear()
    if rejected:
        _append_lines(quarantine_file, rejected)
        rejected.clear()

    # Rejected records must be durably in the dead-letter topic before the
    # source offsets advance, or a crash here would lose them entirely.
    dlq_producer.flush(60)
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        # Nothing to commit on an empty first poll is not an error.
        if "no offset" not in str(exc).lower():
            raise


def _append_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def _clear_directory(path: Path, pattern: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for stale in path.glob(pattern):
        stale.unlink()


def _write_summary(stats: ConsumerStats, valid_file: Path, quarantine_file: Path) -> None:
    target = settings.evidence_root / "runs" / "consumer_summary.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_topic": settings.kafka_topic_quotes,
                "dead_letter_topic": settings.kafka_topic_dlq,
                "landing_file": str(valid_file),
                "quarantine_file": str(quarantine_file),
                **stats.as_dict(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    log.info("consumer.summary_written", path=str(target))


if __name__ == "__main__":  # pragma: no cover
    run_consumer()
