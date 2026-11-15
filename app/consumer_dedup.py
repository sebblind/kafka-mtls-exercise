#!/usr/bin/env python3
import sys
import json
import hashlib
import logging
from collections import Counter
from pathlib import Path
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from prometheus_client import Counter as PromCounter, Gauge, start_http_server

BASE_DIR = Path(__file__).resolve().parent.parent
CERT_DIR = BASE_DIR / "certs"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# --- Prometheus Metrics ---
MESSAGES_TOTAL = PromCounter("kafka_app_messages_consumed_total", "Total Avro messages consumed")
UNIQUE_MESSAGES = PromCounter("kafka_app_unique_messages_total", "Total unique Avro messages processed")
DUPLICATE_MESSAGES = PromCounter("kafka_app_duplicate_messages_total", "Total duplicate Avro messages detected")
DUPLICATE_RATIO = Gauge("kafka_app_duplicate_percentage", "Percentage of consumed messages that are duplicates")
LOG_EVENTS = PromCounter("kafka_app_log_events_total", "Application log events by level", ["level"])


class PrometheusLogHandler(logging.Handler):
    """Automatically increments Prometheus counters on every WARNING or ERROR log."""
    def emit(self, record):
        if record.levelname in ("WARNING", "ERROR", "CRITICAL"):
            LOG_EVENTS.labels(level=record.levelname).inc()


logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","app":"consumer","msg":"%(message)s"}',
    handlers=[
        logging.FileHandler(LOG_DIR / "app.log"),
        logging.StreamHandler(sys.stdout),
        PrometheusLogHandler(),
    ]
)
logger = logging.getLogger("consumer")
# Initialize zero values so labels appear immediately in Prometheus/Grafana
LOG_EVENTS.labels(level="WARNING").inc(0)
LOG_EVENTS.labels(level="ERROR").inc(0)


def compute_message_hash(record: dict) -> str:
    """Calculates a deterministic SHA-256 hash of the canonical Avro payload."""
    canonical_json = json.dumps(record, sort_keys=True)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def main():
    # Start Prometheus metrics HTTP server on port 8000
    start_http_server(8000)
    logger.info("Prometheus metrics server started on http://localhost:8000/metrics")

    sr_conf = {
        "url": "https://localhost:8081",
        "ssl.ca.location": str(CERT_DIR / "ca.crt"),
    }
    schema_registry_client = SchemaRegistryClient(sr_conf)

    with open(BASE_DIR / "app/schema/order_event.avsc", "r") as f:
        schema_str = f.read()

    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    consumer_conf = {
        "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
        "group.id": "dedup-consumer-group",
        "auto.offset.reset": "earliest",
        "security.protocol": "SSL",
        "ssl.ca.location": str(CERT_DIR / "ca.crt"),
        "ssl.certificate.location": str(CERT_DIR / "python-consumer.crt"),
        "ssl.key.location": str(CERT_DIR / "python-consumer.key"),
    }

    consumer = Consumer(consumer_conf)
    topic = "orders-avro"
    consumer.subscribe([topic])

    seen_hashes = Counter()
    total_consumed = 0
    total_duplicates = 0

    logger.info(f"Consuming Avro messages from '{topic}' over mTLS (Press Ctrl+C to stop)...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka consumer error: {msg.error()}")
                continue

            record = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if record is None:
                continue

            total_consumed += 1
            MESSAGES_TOTAL.inc()

            msg_hash = compute_message_hash(record)
            seen_hashes[msg_hash] += 1
            occurrence = seen_hashes[msg_hash]

            if occurrence > 1:
                total_duplicates += 1
                DUPLICATE_MESSAGES.inc()
                dup_pct = (total_duplicates / total_consumed) * 100.0
                DUPLICATE_RATIO.set(dup_pct)
                logger.warning(
                    f"DUPLICATE DETECTED! order_id={record['order_id']} "
                    f"(occurrence #{occurrence}, sha256={msg_hash[:12]}...) | "
                    f"Stats: total={total_consumed}, unique={len(seen_hashes)}, "
                    f"duplicates={total_duplicates} ({dup_pct:.1f}%)"
                )
            else:
                UNIQUE_MESSAGES.inc()
                dup_pct = (total_duplicates / total_consumed) * 100.0
                DUPLICATE_RATIO.set(dup_pct)
                logger.info(
                    f"PROCESSED UNIQUE: order_id={record['order_id']} item='{record['item']}' "
                    f"amount={record['amount']} (sha256={msg_hash[:12]}...)"
                )

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
