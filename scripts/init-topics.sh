#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka1:9092}"
DEFAULT_PARTITIONS="${DEFAULT_PARTITIONS:-3}"
DEFAULT_RF="${DEFAULT_RF:-3}"
# Format: TOPICS="events_topic:3:3;orders:6:3;payments"  (#parts:#rf ixtiyoriy)
TOPICS="${TOPICS:-events_topic}"

echo "Bootstrap server: $BOOTSTRAP"
echo "Default partitions: $DEFAULT_PARTITIONS, default RF: $DEFAULT_RF"

wait_for_kafka() {
  echo "Kafka tayyor bo'lishini kutyapman..."
  for i in {1..60}; do
    if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
      echo "Kafka reachable ✅"
      return 0
    fi
    echo "  ...kutish ($i/60)"
    sleep 2
  done
  echo "Kafka hali ham javob bermadi ❌"
  return 1
}

create_topic() {
  local def="$1"
  IFS=':' read -r name parts rf <<< "$def"
  parts="${parts:-$DEFAULT_PARTITIONS}"
  rf="${rf:-$DEFAULT_RF}"

  if [[ -z "$name" ]]; then
    return 0
  fi

  echo "Topic yaratish: $name (partitions=$parts, rf=$rf)"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$parts" \
    --replication-factor "$rf"

  # Brokerlar konfiguratsiyasi bilan moslash (min.insync.replicas=2)
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --alter --topic "$name" \
    --config min.insync.replicas=2 || true
}

create_connect_topics() {
  echo "Kafka Connect ichki topiclarini tekshirish/yaratish..."
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic _connect-configs \
    --partitions 1 --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2 || true

  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic _connect-offsets \
    --partitions 25 --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2 || true

  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic _connect-status \
    --partitions 5 --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2 || true
}

main() {
  wait_for_kafka

  # Custom app topiclar
  IFS=';' read -ra defs <<< "$TOPICS"
  for d in "${defs[@]}"; do
    create_topic "$d"
  done

  # Connect uchun zarur topiclar
  create_connect_topics

  echo "Mavjud topiclar ro'yxati:"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
  echo "Init done ✅"
}

main
