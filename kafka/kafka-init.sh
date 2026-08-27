#!/bin/bash
set -euo pipefail

export KAFKA_CFG_NODE_ID="${KAFKA_CFG_NODE_ID:-1}"
export KAFKA_CFG_PROCESS_ROLES="${KAFKA_CFG_PROCESS_ROLES:-broker,controller}"
export KAFKA_CFG_CONTROLLER_QUORUM_VOTERS="${KAFKA_CFG_CONTROLLER_QUORUM_VOTERS:-1@kafka:9093}"
export KAFKA_CFG_LISTENERS="${KAFKA_CFG_LISTENERS:-SASL_PLAINTEXT://:9092,CONTROLLER://:9093}"
export KAFKA_CFG_ADVERTISED_LISTENERS="${KAFKA_CFG_ADVERTISED_LISTENERS:-SASL_PLAINTEXT://localhost:9092}"
export KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP="${KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP:-CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT}"
export KAFKA_CFG_CONTROLLER_LISTENER_NAMES="${KAFKA_CFG_CONTROLLER_LISTENER_NAMES:-CONTROLLER}"
export KAFKA_CFG_INTER_BROKER_LISTENER_NAME="${KAFKA_CFG_INTER_BROKER_LISTENER_NAME:-SASL_PLAINTEXT}"
export KAFKA_CFG_SASL_ENABLED_MECHANISMS="${KAFKA_CFG_SASL_ENABLED_MECHANISMS:-PLAIN}"
export KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL="${KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL:-PLAIN}"
export KAFKA_CFG_ALLOW_EVERYONE_IF_NO_ACL_FOUND="${KAFKA_CFG_ALLOW_EVERYONE_IF_NO_ACL_FOUND:-true}"
export KAFKA_CLIENT_USERS="${KAFKA_CLIENT_USERS:-admin}"
export KAFKA_CLIENT_PASSWORDS="${KAFKA_CLIENT_PASSWORDS:-admin}"

cat > /tmp/admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";
EOF

/opt/bitnami/scripts/kafka/run.sh &
kafka_pid=$!

for i in $(seq 1 60); do
  if /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list --command-config /tmp/admin.properties >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

/opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic flink-input \
  --partitions 3 \
  --replication-factor 1 \
  --command-config /tmp/admin.properties

/opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic flink-output \
  --partitions 3 \
  --replication-factor 1 \
  --command-config /tmp/admin.properties

wait "$kafka_pid"
