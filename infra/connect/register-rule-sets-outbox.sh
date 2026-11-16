#!/bin/sh
# Registra (ou atualiza) o conector Debezium que publica a outbox do rule-management em rule-sets.v1.
# Idempotente: PUT em /connectors/<nome>/config cria ou atualiza. Tenta de novo se o Connect estiver rebalanceando
# (acontece logo depois de remover um conector) e mostra a resposta do Connect quando falha.
# Uso: sh infra/connect/register-rule-sets-outbox.sh   (CONNECT_URL padrão: http://localhost:18083)
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:18083}"
NAME=rule-sets-outbox
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"   # o curl lê o JSON por caminho relativo: com MSYS_NO_PATHCONV=1 o caminho absoluto do Git Bash chega errado ao curl do Windows e o corpo vai vazio (HTTP 500 no Connect)

registered=0
for attempt in $(seq 1 30); do
  rm -f /tmp/connect-register.out
  code="$(curl -s -o /tmp/connect-register.out -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
    --data-binary "@$NAME.json" "$CONNECT_URL/connectors/$NAME/config" || true)"
  code="${code:-000}"
  case "$code" in
    200|201) registered=1; break ;;
    *) echo "tentativa $attempt: HTTP $code $(head -c 200 /tmp/connect-register.out 2>/dev/null)" >&2; sleep 5 ;;
  esac
done
if [ "$registered" -ne 1 ]; then
  echo "não foi possível registrar o conector $NAME em $CONNECT_URL" >&2
  exit 1
fi
echo "conector $NAME registrado em $CONNECT_URL"

for i in $(seq 1 30); do
  state="$(curl -sf "$CONNECT_URL/connectors/$NAME/status" | grep -o '"state":"[A-Z]*"' | head -1 || true)"
  case "$state" in *RUNNING*) echo "conector em execução"; exit 0 ;; esac
  sleep 2
done
echo "conector não chegou a RUNNING; veja: curl $CONNECT_URL/connectors/$NAME/status" >&2
exit 1
