#!/usr/bin/env bash
set -euo pipefail

CERT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/certs"
PASSWORD="changeit"
DAYS=365

echo "==> Cleaning and preparing $CERT_DIR..."
rm -rf "${CERT_DIR:?}"/*
touch "$CERT_DIR/.gitkeep"
cd "$CERT_DIR"

echo "==> 1. Generating Root CA (ca.key & ca.crt)..."
openssl req -new -x509 -keyout ca.key -out ca.crt -days "$DAYS" \
  -passout pass:"$PASSWORD" \
  -subj "/C=SE/ST=Stockholm/L=Solna/O=KafkaLab/OU=Platform/CN=KafkaLabRootCA"

# Create a shared PKCS12 Truststore containing the Root CA for Java components
keytool -keystore kafka.truststore.p12 -storetype PKCS12 \
  -alias CARoot -import -file ca.crt -storepass "$PASSWORD" -noprompt

# Helper function to generate certs, PEMs, and PKCS12 keystores
generate_cert() {
  local NAME=$1
  local CN=$2
  local SAN=$3

  echo "==> Generating certificate for $NAME (CN=$CN)..."

  # 1. Generate private key (unencrypted PEM for Python / librdkafka)
  openssl genrsa -out "${NAME}.key" 2048

  # 2. Create CSR (Certificate Signing Request)
  openssl req -new -key "${NAME}.key" -out "${NAME}.csr" \
    -subj "/C=SE/ST=Stockholm/L=Solna/O=KafkaLab/OU=Platform/CN=${CN}"

  # 3. Create OpenSSL extension config for SANs
  cat <<EXT > "${NAME}.ext"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = ${SAN}
EXT

  # 4. Sign the CSR with our Root CA
  openssl x509 -req -in "${NAME}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "${NAME}.crt" -days "$DAYS" -sha256 -extfile "${NAME}.ext" \
    -passin pass:"$PASSWORD"

  # 5. Export a PKCS12 (.p12) keystore for Java (Brokers, Schema Registry, CLI)
  openssl pkcs12 -export -in "${NAME}.crt" -inkey "${NAME}.key" \
    -chain -CAfile ca.crt -name "$NAME" \
    -out "${NAME}.keystore.p12" -password pass:"$PASSWORD"

  # Clean up temporary CSR and ext files
  rm -f "${NAME}.csr" "${NAME}.ext"
}

# 2. Generate Server Certificates (Brokers & Schema Registry)
generate_cert "kafka-1"         "broker"          "DNS:kafka-1,DNS:localhost,IP:127.0.0.1"
generate_cert "kafka-2"         "broker"          "DNS:kafka-2,DNS:localhost,IP:127.0.0.1"
generate_cert "kafka-3"         "broker"          "DNS:kafka-3,DNS:localhost,IP:127.0.0.1"
generate_cert "schema-registry" "schema-registry" "DNS:schema-registry,DNS:localhost,IP:127.0.0.1"

# 3. Generate Client Certificates (Distinct CNs for Kafka ACL RBAC)
generate_cert "admin"           "admin"           "DNS:localhost,IP:127.0.0.1"
generate_cert "python-producer" "python-producer" "DNS:localhost,IP:127.0.0.1"
generate_cert "python-consumer" "python-consumer" "DNS:localhost,IP:127.0.0.1"

# 4. Create credential files required by Confluent Docker images
echo "$PASSWORD" > credentials

# 5. Create admin.properties for running kafka-acls / kafka-topics CLI over mTLS
cat <<PROP > client-admin.properties
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12
ssl.truststore.password=${PASSWORD}
ssl.truststore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/admin.keystore.p12
ssl.keystore.password=${PASSWORD}
ssl.key.password=${PASSWORD}
ssl.keystore.type=PKCS12
PROP

chmod 644 "$CERT_DIR"/*
echo "==> Done! All certificates generated in $CERT_DIR"
