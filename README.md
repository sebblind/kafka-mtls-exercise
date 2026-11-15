# Kafka KRaft mTLS, Schema Registry TLS, RBAC ACLs & Observability Lab

End-to-end demonstration of a 3-node Apache Kafka (KRaft) cluster secured with mutual TLS (mTLS), certificate-based ACLs, a TLS-enabled Schema Registry, Python Avro clients with SHA-256 duplicate detection, and a Prometheus + Loki + Grafana monitoring stack.

## Architecture & Requirements Mapping

| Exercise Requirement | Implementation in Repository |
| :--- | :--- |
| **1. Install Kafka Cluster** | 3-node KRaft cluster (`kafka-1`, `kafka-2`, `kafka-3`) in `docker-compose.yml` |
| **2. Enable mTLS (Self-Signed)** | Automated OpenSSL + PKCS12 PKI script in `scripts/01-generate-certs.sh` (`SSL` client auth required) |
| **3. Enable TLS on Schema Registry** | `schema-registry` configured with HTTPS listener (`https://localhost:8081`) in `docker-compose.yml` |
| **4. Configure ACLs (RBAC)** | `StandardAuthorizer` with default-deny and X.509 CN principal mapping in `scripts/02-setup-acls.sh` |
| **5. Python Avro Producer/Consumer over SSL** | `app/producer.py` and `app/consumer_dedup.py` using `confluent-kafka` & `AvroSerializer`/`AvroDeserializer` |
| **6. Calculate Duplicates** | Deterministic canonical JSON SHA-256 hashing in `app/consumer_dedup.py` tracking unique vs. duplicate ratios |
| **7. Application Monitoring (Grafana)** | Prometheus (`:8000` client metrics) + Promtail/Loki (structured JSON `WARNING`/`ERROR` logs) + pre-provisioned Grafana dashboard |

---

## Quickstart & Demo Runbook

### 1. Generate Certificates (Root CA, Broker Keystores, Client Certs)
```bash
./scripts/01-generate-certs.sh
