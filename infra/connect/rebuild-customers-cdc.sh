#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:18083}"
NAME=customers-cdc
SLOT=customers_cdc_slot

call() {
  code="$(curl -s -o /dev/null -w '%{http_code}' -X "$1" "$CONNECT_URL$2" || true)"
  echo "${code:-000}"
}

wait_state() {
  for i in $(seq 1 60); do
    curl -sf "$CONNECT_URL/connectors/$NAME/status" | grep -o '"state":"[A-Z]*"' | head -1 | grep -q "$1" && return 0
    sleep 1
  done
  echo "o conector $NAME não chegou a $1; veja: curl $CONNECT_URL/connectors/$NAME/status" >&2
  exit 1
}

echo "parando o conector $NAME"
call PUT "/connectors/$NAME/stop" >/dev/null
wait_state STOPPED

echo "apagando os offsets do conector"
code="$(call DELETE "/connectors/$NAME/offsets")"
case "$code" in 200|204) ;; *) echo "não foi possível apagar os offsets (HTTP $code)" >&2; exit 1 ;; esac

echo "removendo o slot de replicação $SLOT"
dropped=0
for i in 1 2 3 4 5 6; do
  if docker compose exec -T bank-db psql -U "${BANK_DB_USER:-bank}" -d bank -v ON_ERROR_STOP=1 \
      -c "SELECT pg_drop_replication_slot('$SLOT')" >/dev/null 2>&1; then
    dropped=1
    break
  fi
  sleep 3
done
if [ "$dropped" -ne 1 ]; then
  echo "não foi possível remover o slot $SLOT (ainda ativo?)" >&2
  exit 1
fi

echo "retomando o conector (snapshot inicial)"
call PUT "/connectors/$NAME/resume" >/dev/null
wait_state RUNNING
echo "conector $NAME em execução; os perfis estão sendo republicados"
