#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="kafka-1:9092"
ADMIN_CFG="/etc/kafka/secrets/client-admin.properties"
TOPIC="orders-avro"

echo "==> 1. Granting ACLs for Schema Registry (User:schema-registry)..."
docker exec kafka-1 kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
  --add --allow-principal "User:schema-registry" \
  --operation Read --operation Write --operation Describe --operation Create --operation DescribeConfigs \
  --topic _schemas

docker exec kafka-1 kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
  --add --allow-principal "User:schema-registry" \
  --operation Read --operation Describe \
  --group schema-registry

docker exec kafka-1 kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
  --add --allow-principal "User:schema-registry" \
  --operation Describe --operation DescribeConfigs \
  --cluster

echo "==> 2. Creating application topic '$TOPIC' (3 partitions, RF=3)..."
docker exec kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
  --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 3

echo "==> 3. Granting Least-Privilege ACLs for Python Producer (User:python-producer)..."
docker exec kafka-1 kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
  --add --allow-principal "User:python-producer" \
  --operation Write --operation Describe \
  --topic "$TOPIC"

echo "==> 4. Granting Least-Privilege ACLs for Python Consumer (User:python-consumer)..."
docker exec kafka-1 kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
  --add --allow-principal "User:python-consumer" \
  --operation Read --operation Describe \
  --topic "$TOPIC"

docker exec kafka-1 kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
  --add --allow-principal "User:python-consumer" \
  --operation Read \
  --group dedup-consumer-group

echo "==> 5. Listing all active Cluster ACLs:"
docker exec kafka-1 kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" --list
