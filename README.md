# Kafka KRaft mTLS, Schema Registry TLS, RBAC ACLs & Observability Lab

End-to-end implementation of a 3-node Apache Kafka (KRaft) cluster secured with mutual TLS (mTLS), certificate-based least-privilege ACLs, a TLS-enabled Confluent Schema Registry, Python Avro clients with SHA-256 duplicate detection, and a pre-provisioned Prometheus + Loki + Grafana observability stack.

---

## 1. Architecture & Requirements Mapping

| Exercise Requirement | Implementation in Repository |
| :--- | :--- |
| **1. Install Kafka Cluster** | 3-node ZooKeeper-less KRaft cluster (`kafka-1`, `kafka-2`, `kafka-3`) in `docker-compose.yml` |
| **2. Enable mTLS (Self-Signed)** | Automated OpenSSL + PKCS12 PKI script (`scripts/01-generate-certs.sh`) with `KAFKA_SSL_CLIENT_AUTH: required` |
| **3. Enable TLS on Schema Registry** | `schema-registry` exposed exclusively over HTTPS (`https://localhost:8081`) and authenticating to Kafka via mTLS |
| **4. Configure ACLs (RBAC)** | `StandardAuthorizer` with default-deny (`ALLOW_EVERYONE_IF_NO_ACL_FOUND=false`) and X.509 `CN` principal mapping (`scripts/02-setup-acls.sh`) |
| **5. Python Avro Producer/Consumer over SSL** | `app/producer.py` and `app/consumer_dedup.py` using `confluent-kafka` (`librdkafka`) + `AvroSerializer` / `AvroDeserializer` |
| **6. Calculate Duplicates** | Deterministic canonical JSON SHA-256 payload fingerprinting in `app/consumer_dedup.py` tracking unique vs. duplicate counts and ratios |
| **7. Application Monitoring (Grafana)** | Prometheus (`:8000` client metrics) + Promtail/Loki (structured JSON `WARNING`/`ERROR` logs) + pre-provisioned Grafana dashboard |

### Data & Security Flow

```text
+-----------------------------------------------------------------------------------+
|                              Custom Root CA (ca.crt)                              |
+-----------------------------------------------------------------------------------+
       | signs                         | signs                        | signs
       v                               v                              v
+------------------+         +--------------------+         +-----------------------+
|  Python Clients  |         |  Schema Registry   |         |  3x KRaft Brokers     |
|  (Avro + mTLS)   |========>|  (HTTPS :8081)     |========>|  kafka-1 (:19092)     |
|                  |  HTTPS  |  CN=schema-registry|  mTLS   |  kafka-2 (:29092)     |
| - producer.py    |         +--------------------+         |  kafka-3 (:39092)     |
|   CN=python-     |                                        |  StandardAuthorizer   |
|   producer       |=======================================>|  (Default Deny ACLs)  |
| - consumer_      |              mTLS (SSL)                +-----------------------+
|   dedup.py       |
|   CN=python-     |----+---> :8000/metrics ---> [ Prometheus ] ---+
|   consumer       |    |                                          |--> [ Grafana ]
+------------------+    +---> logs/app.log  ---> [ Promtail/Loki ]-+    (:3000 UI)
```

---

## 2. Repository Structure

```text
kafka-mtls-exercise/
├── README.md
├── docker-compose.yml             # 3 KRaft Brokers, Schema Registry, Prometheus, Loki, Promtail, Grafana
├── scripts/
│   ├── 01-generate-certs.sh       # Generates Root CA, PKCS12 broker/SR keystores, and PEM client certs
│   └── 02-setup-acls.sh           # Bootstraps topics and applies least-privilege ACLs per X.509 CN
├── app/
│   ├── requirements.txt           # Python dependencies (confluent-kafka[avro], prometheus-client)
│   ├── schema/
│   │   └── order_event.avsc       # Avro schema definition (OrderEvent)
│   ├── producer.py                # mTLS Avro producer (sends intentional duplicates + ACL test mode)
│   └── consumer_dedup.py          # mTLS Avro consumer with SHA-256 deduplication & Prometheus metrics
└── monitoring/
    ├── prometheus.yml             # Scrapes Python consumer metrics on port 8000
    ├── loki-config.yml            # Log storage configuration
    ├── promtail-config.yml        # Tails logs/app.log and parses JSON fields (level, app, msg)
    └── grafana/                   # Auto-provisioned datasources & Kafka Application Monitoring dashboard
```

---

## 3. Security, mTLS & RBAC ACL Design

### Principal Mapping
Brokers extract the X.509 Certificate Subject Common Name (`CN`) using:
`KAFKA_SSL_PRINCIPAL_MAPPING_RULES: RULE:^CN=([a-zA-Z0-9.-]*).*$/$1/,DEFAULT`

### Least-Privilege Access Matrix

| X.509 Certificate (`CN`) | Mapped Kafka Principal | Resource | Allowed Operations | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `CN=broker` | `User:broker` | Cluster (`SuperUser`) | `All` | Inter-broker KRaft controller & replication traffic |
| `CN=admin` | `User:admin` | Cluster (`SuperUser`) | `All` | Administrative CLI topic & ACL management |
| `CN=schema-registry` | `User:schema-registry` | Topic: `_schemas` / Group: `schema-registry` | `Read, Write, Create, Describe, DescribeConfigs` | Storing & reading registered Avro schemas |
| `CN=python-producer` | `User:python-producer` | Topic: `orders-avro` | `Write, Describe` | Producing Avro events (cannot consume or write to other topics) |
| `CN=python-consumer` | `User:python-consumer` | Topic: `orders-avro` / Group: `dedup-consumer-group` | `Read, Describe` | Consuming Avro events & committing consumer group offsets |

---

## 4. Duplicate Detection Methodology

Kafka guarantees at-least-once delivery by default, meaning network retries or producer replays can introduce duplicate messages into a partition.

In `app/consumer_dedup.py`:
1. Each incoming Avro binary payload is deserialized against the Schema Registry into a Python dictionary.
2. The dictionary is serialized into **canonical JSON** (`json.dumps(record, sort_keys=True)`) and hashed with **SHA-256**.
3. An in-memory `Counter` tracks the occurrence count of each payload hash:
   * **First occurrence (`count == 1`):** Logged as `INFO` (`PROCESSED UNIQUE`) and increments `kafka_app_unique_messages_total`.
   * **Subsequent occurrences (`count > 1`):** Flagged and logged as `WARNING` (`DUPLICATE DETECTED!`), increments `kafka_app_duplicate_messages_total`, and updates the live duplicate ratio gauge (`kafka_app_duplicate_percentage`).

---

## 5. Quickstart & Demo Runbook

### Prerequisites
* Docker & Docker Compose plugin
* OpenSSL & Java `keytool` (OpenJDK 21)
* Python 3.10+ with `venv`

### Step 1: Generate Certificates (Root CA, Broker Keystores, Client Certs)
```bash
./scripts/01-generate-certs.sh
```

### Step 2: Start Kafka KRaft Cluster & Apply RBAC ACLs
```bash
docker compose up -d kafka-1 kafka-2 kafka-3
sleep 15
./scripts/02-setup-acls.sh
```

### Step 3: Start TLS Schema Registry & Monitoring Stack
```bash
docker compose up -d schema-registry prometheus loki promtail grafana
sleep 12
curl --cacert certs/ca.crt https://localhost:8081/subjects
```

### Step 4: Set Up Python Environment & Run Avro Clients
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r app/requirements.txt

# Terminal 1: Start deduplicating consumer + Prometheus metrics endpoint (:8000)
python app/consumer_dedup.py

# Terminal 2: Produce Avro batch (6 messages: 3 unique + 3 intentional duplicates)
python app/producer.py

# Terminal 2: Trigger an intentional RBAC ACL denial (generates WARNING + ERROR logs)
python app/producer.py --test-acl-error
```

### Step 5: View Application Monitoring in Grafana
* **URL:** `http://localhost:3000/login` (Credentials: `admin` / `admin`)
* **Default Home Dashboard:** *Kafka mTLS Application Monitoring (Duplicates, Warnings & Errors)*
* **Prometheus Metrics Endpoint:** `http://localhost:8000/metrics`

---

## 6. Verification & Inspection Commands

```bash
# Inspect registered Avro schemas and versions over HTTPS
curl --cacert certs/ca.crt https://localhost:8081/subjects
curl --cacert certs/ca.crt https://localhost:8081/subjects/orders-avro-value/versions/1 | jq .

# List active Kafka ACLs using the mTLS admin certificate
docker exec kafka-1 kafka-acls --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/secrets/client-admin.properties --list

# Inspect raw Prometheus metrics exposed by the Python consumer
curl -s http://localhost:8000/metrics | grep kafka_app
```

---

## 7. Stopping & Cleaning Up

```bash
# Stop containers without deleting state
docker compose stop

# Full teardown (removes containers and volumes)
docker compose down -v
```