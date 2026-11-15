#!/usr/bin/env python3
import os
import sys
import time
import logging
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

BASE_DIR = Path(__file__).resolve().parent.parent
CERT_DIR = BASE_DIR / "certs"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","app":"producer","msg":"%(message)s"}',
    handlers=[
        logging.FileHandler(LOG_DIR / "app.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("producer")


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for key={msg.key()}: {err}")
    else:
        logger.info(
            f"Delivered key={msg.key().decode('utf-8')} to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}"
        )


def main():
    topic = "orders-avro"
    if "--test-acl-error" in sys.argv:
        topic = "unauthorized-topic"
        logger.warning(f"Testing RBAC ACL denial by producing to '{topic}'")

    # 1. Connect to Schema Registry over HTTPS (TLS)
    sr_conf = {
        "url": "https://localhost:8081",
        "ssl.ca.location": str(CERT_DIR / "ca.crt"),
    }
    schema_registry_client = SchemaRegistryClient(sr_conf)

    with open(BASE_DIR / "app/schema/order_event.avsc", "r") as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    string_serializer = StringSerializer("utf_8")

    # 2. Configure Kafka Producer with mTLS (User:python-producer)
    producer_conf = {
        "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
        "security.protocol": "SSL",
        "ssl.ca.location": str(CERT_DIR / "ca.crt"),
        "ssl.certificate.location": str(CERT_DIR / "python-producer.crt"),
        "ssl.key.location": str(CERT_DIR / "python-producer.key"),
    }
    producer = Producer(producer_conf)

    # 3. Sample batch including intentional exact duplicates (ORD-1001 & ORD-1002 repeated)
    messages = [
        {"order_id": "ORD-1001", "customer_id": "CUST-01", "item": "Focusrite Scarlett 2i2", "amount": 199.0, "timestamp": "2026-11-15T12:00:01Z"},
        {"order_id": "ORD-1002", "customer_id": "CUST-02", "item": "Beyerdynamic DT-770 Pro", "amount": 159.0, "timestamp": "2026-11-15T12:00:02Z"},
        {"order_id": "ORD-1001", "customer_id": "CUST-01", "item": "Focusrite Scarlett 2i2", "amount": 199.0, "timestamp": "2026-11-15T12:00:01Z"},  # DUPLICATE 1
        {"order_id": "ORD-1003", "customer_id": "CUST-03", "item": "Garmin Forerunner 265", "amount": 449.0, "timestamp": "2026-11-15T12:00:05Z"},
        {"order_id": "ORD-1002", "customer_id": "CUST-02", "item": "Beyerdynamic DT-770 Pro", "amount": 159.0, "timestamp": "2026-11-15T12:00:02Z"},  # DUPLICATE 2
        {"order_id": "ORD-1001", "customer_id": "CUST-01", "item": "Focusrite Scarlett 2i2", "amount": 199.0, "timestamp": "2026-11-15T12:00:01Z"},  # DUPLICATE 3
    ]

    logger.info(f"Producing {len(messages)} Avro messages (including 3 intentional duplicates) to '{topic}'...")

    for record in messages:
        producer.poll(0)
        try:
            producer.produce(
                topic=topic,
                key=string_serializer(record["order_id"]),
                value=avro_serializer(record, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=delivery_report,
            )
        except Exception as exc:
            logger.error(f"Produce exception: {exc}")
        time.sleep(0.3)

    producer.flush(10)
    logger.info("Finished producing batch.")


if __name__ == "__main__":
    main()
