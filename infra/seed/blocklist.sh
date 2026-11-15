#!/bin/sh
# Semeia (ou limpa) a lista de bloqueio de demonstração em blocklist.v1 (spec 004, H7).
# add    publica 3 contas e 2 dispositivos marcados como fraude; a regra R10 passa a disparar para quem transacionar com eles
# remove publica um tombstone (valor nulo) para cada um, e o bloqueio some na hora
# Uso (na raiz do repositório, com o Kafka no ar): sh infra/seed/blocklist.sh [add|remove]
# Todos os tokens são sintéticos. O motivo segue o contrato blocklist-entry.v1 (código em maiúsculas).
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
