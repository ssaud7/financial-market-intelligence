"""Topic administration for the ingestion stage.

``KAFKA_AUTO_CREATE_TOPICS_ENABLE`` is deliberately off on the broker, so topics
are created explicitly here with the partition count and retention the pipeline
expects. The quotes topic is partitioned by ticker; the dead-letter topic keeps
a single partition so rejected records stay in arrival order for inspection.
"""

from __future__ import annotations

import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from fmis.config import settings
from fmis.logging_setup import configure_logging

log = configure_logging("kafka_admin")


def admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})


def wait_for_broker(timeout_s: float = 60.0, interval_s: float = 2.0) -> None:
    """Block until the broker answers a metadata request, or raise.

    Airflow starts the ingestion task the moment the DAG is triggered, which can
    be seconds after ``docker compose up``; without this the first producer run
    would fail on a broker that is merely still booting.
    """
    client = admin_client()
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            metadata = client.list_topics(timeout=5.0)
            log.info(
                "kafka.broker_ready",
                brokers=[f"{b.host}:{b.port}" for b in metadata.brokers.values()],
            )
            return
        except KafkaException as exc:
            last_error = exc
            time.sleep(interval_s)

    raise RuntimeError(
        f"Kafka broker at {settings.kafka_bootstrap_servers} did not become ready within "
        f"{timeout_s:.0f}s. Is `docker compose up -d` running? Last error: {last_error}"
    )


def ensure_topics() -> None:
    """Create the quotes and dead-letter topics if they do not already exist."""
    client = admin_client()
    existing = set(client.list_topics(timeout=10.0).topics)

    wanted = [
        NewTopic(
            settings.kafka_topic_quotes,
            num_partitions=3,
            replication_factor=1,
            config={"retention.ms": str(7 * 24 * 60 * 60 * 1000)},
        ),
        NewTopic(
            settings.kafka_topic_dlq,
            num_partitions=1,
            replication_factor=1,
            # Rejected records are evidence; keep them a good deal longer.
            config={"retention.ms": str(30 * 24 * 60 * 60 * 1000)},
        ),
    ]
    to_create = [t for t in wanted if t.topic not in existing]

    if not to_create:
        log.info("kafka.topics_exist", topics=[t.topic for t in wanted])
        return

    for topic, future in client.create_topics(to_create).items():
        try:
            future.result(timeout=30)
            log.info("kafka.topic_created", topic=topic)
        except KafkaException as exc:
            # TOPIC_ALREADY_EXISTS is benign under concurrent starts.
            if "already exists" in str(exc).lower():
                log.info("kafka.topic_exists", topic=topic)
            else:
                raise


def topic_counts() -> dict[str, int]:
    """High-watermark message count per pipeline topic, summed over partitions.

    Used by the ingestion report and the notebooks as independent proof that
    messages really traversed a broker.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "fmis-topic-counter",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(timeout=10.0)
        counts: dict[str, int] = {}
        for topic in (settings.kafka_topic_quotes, settings.kafka_topic_dlq):
            if topic not in metadata.topics:
                counts[topic] = 0
                continue
            total = 0
            for partition_id in metadata.topics[topic].partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id), timeout=10.0
                )
                total += high - low
            counts[topic] = total
        return counts
    finally:
        consumer.close()
