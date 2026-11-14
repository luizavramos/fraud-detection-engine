#!/bin/sh
# Registra (ou atualiza) os conectores Debezium do banco legado. Cada <nome>.json desta pasta vira o conector <nome>.
# Idempotente: PUT em /connectors/<nome>/config cria ou atualiza; tenta de novo se o Connect estiver rebalanceando.
# Uso: sh infra/connect/register-connectors.sh [nome ...]   (padrão: customers-cdc; CONNECT_URL padrão http://localhost:18083)
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:18083}"
DIR="$(cd "$(dirname "$0")" && pwd)"
[ "$#" -gt 0 ] || set -- customers-cdc

for NAME in "$@"; do
  registered=0
  for attempt in 1 2 3 4 5 6; do
    code="$(curl -s -o /tmp/connect-register.out -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
      --data-binary "@$DIR/$NAME.json" "$CONNECT_URL/connectors/$NAME/config" || echo 000)"
    case "$code" in
      200|201) registered=1; break ;;
      *) echo "$NAME, tentativa $attempt: HTTP $code $(head -c 200 /tmp/connect-register.out 2>/dev/null)" >&2; sleep 5 ;;
    esac
  done
  if [ "$registered" -ne 1 ]; then
    echo "não foi possível registrar o conector $NAME em $CONNECT_URL" >&2
    exit 1
  fi
  echo "conector $NAME registrado em $CONNECT_URL"

  running=0
  for i in $(seq 1 30); do
    state="$(curl -sf "$CONNECT_URL/connectors/$NAME/status" | grep -o '"state":"[A-Z]*"' | head -1 || true)"
    case "$state" in *RUNNING*) running=1; break ;; esac
    sleep 2
  done
  if [ "$running" -ne 1 ]; then
    echo "conector $NAME não chegou a RUNNING; veja: curl $CONNECT_URL/connectors/$NAME/status" >&2
    exit 1
  fi
  echo "conector $NAME em execução"
done
