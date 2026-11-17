#!/bin/sh
set -eu

ACTION="${1:-add}"
export MSYS_NO_PATHCONV=1
KAFKA_PRODUCER="docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic blocklist.v1 \
  --property parse.key=true --property null.marker=NULL --property key.separator=|"

ENTRIES="ACCOUNT|acc_tok_00000013|CONFIRMED_FRAUD
ACCOUNT|acc_tok_00000014|MULE_ACCOUNT
ACCOUNT|acc_tok_00000015|CONFIRMED_FRAUD
DEVICE|dev_demo00000001|STOLEN_DEVICE
DEVICE|dev_demo00000002|EMULATOR_FARM"

now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
case "$ACTION" in
  add)
    echo "$ENTRIES" | while IFS='|' read -r type token reason; do
      printf '%s|{"entityToken":"%s","entityType":"%s","reason":"%s","listedAt":"%s"}\n' "$token" "$token" "$type" "$reason" "$now"
    done | $KAFKA_PRODUCER
    echo "lista de bloqueio semeada: 3 contas e 2 dispositivos em blocklist.v1"
    ;;
  remove)
    echo "$ENTRIES" | while IFS='|' read -r type token reason; do
      printf '%s|NULL\n' "$token"
    done | $KAFKA_PRODUCER
    echo "lista de bloqueio limpa: tombstone para cada token"
    ;;
  *)
    echo "uso: sh infra/seed/blocklist.sh [add|remove]" >&2
    exit 1
    ;;
esac
