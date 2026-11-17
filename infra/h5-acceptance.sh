#!/bin/sh
set -eu

RULES="${RULES_URL:-http://localhost:8082}"
ENGINE="${ENGINE_URL:-http://localhost:8084}"
LIMIT_SECONDS="${LIMIT_SECONDS:-35}"
RULE="H5$(date +%s)"
ACCOUNT="acc_tok_h5$(date +%s)"
KAFKA="docker compose exec -T kafka /opt/kafka/bin"
export MSYS_NO_PATHCONV=1

fail() { echo "FALHOU: $*" >&2; exit 1; }
api() {
  user="$1"; pass="$2"; method="$3"; path="$4"; body="${5:-}"
  if [ -n "$body" ]; then
    curl -s -u "$user:$pass" -X "$method" -H 'Content-Type: application/json' -d "$body" "$RULES$path"
  else
    curl -s -u "$user:$pass" -X "$method" "$RULES$path"
  fi
}
analista() { api analista analista-local "$@"; }
coordenador() { api coordenador coordenador-local "$@"; }
active_version() { api motor motor-local GET /v1/rule-sets/active | grep -o '"version":[0-9]*' | head -1 | cut -d: -f2; }

identity() {
  id="$(docker compose ps -q detection-engine)"
  started="$(docker inspect -f '{{.State.StartedAt}}' "$id")"
  restarts="$(docker inspect -f '{{.RestartCount}}' "$id")"
  proc="$(docker compose exec -T detection-engine sh -c 'cut -d" " -f22 /proc/1/stat')"
  echo "$id $started $restarts $proc"
}

counter=0
send() {
  counter=$((counter + 1))
  txid="h5-$(date +%s)-$counter-abcdef"
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  value='{"schemaVersion":1,"transactionId":"'"$txid"'","accountId":"'"$ACCOUNT"'","customerId":"cus_tok_h5000001","type":"PIX","amount":{"value":'"$1"',"currency":"BRL"},"counterparty":{"accountToken":"acc_tok_dest00h5","bankIspb":"60746948"},"channel":"MOBILE","deviceIdHash":"dev_h5device01","occurredAt":"'"$now"'"}'
  printf '%s|%s\n' "$ACCOUNT" "$value" | $KAFKA/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic transaction-commands.v1 \
    --property parse.key=true --property 'key.separator=|' >/dev/null 2>&1
  echo "$txid"
}

decision_of() {
  $KAFKA/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic fraud-decisions.v1 --from-beginning --timeout-ms 4000 2>/dev/null \
    | grep "\"transactionId\":\"$1\"" | head -1 || true
}

wait_for() {
  start="$(date +%s)"
  while :; do
    txid="$(send "$1")"
    sleep 2
    decision="$(decision_of "$txid")"
    if [ -n "$decision" ] && echo "$decision" | grep -Eq "$2"; then
      elapsed=$(( $(date +%s) - start ))
      echo "  $3: ok em ${elapsed}s (decisão: $(echo "$decision" | grep -o '"ruleSetVersion":[0-9]*'))" >&2
      [ "$elapsed" -le "$LIMIT_SECONDS" ] || fail "$3 demorou ${elapsed}s (limite ${LIMIT_SECONDS}s)"
      return 0
    fi
    [ $(( $(date +%s) - start )) -le $((LIMIT_SECONDS + 25)) ] || fail "$3: a decisão não refletiu a mudança"
  done
}

echo "motor pronto?"
curl -sf "$ENGINE/actuator/health/readiness" >/dev/null || fail "o motor não está pronto em $ENGINE (suba o ambiente, veja o cabeçalho)"
before="$(identity)"
echo "identidade antes: $before"

echo "1. criar e ativar uma regra nova ($RULE): amount > limit"
analista POST /v1/rules '{"ruleId":"'"$RULE"'","expression":"tx.amount > params.limit","params":{"limit":100000},"paramOverrides":{},"weight":70,"reasonTemplate":"H5 acima de {params.limit}"}' >/dev/null
coordenador POST "/v1/rules/$RULE/versions/1:activate" >/dev/null
version_a="$(active_version)"
wait_for 150000 "\"ruleId\":\"$RULE\"" "regra nova dispara"

echo "2. mudar o threshold (limit 500000, nova versão da regra)"
analista POST "/v1/rules/$RULE/versions" '{"expression":"tx.amount > params.limit","params":{"limit":500000},"paramOverrides":{},"weight":70,"reasonTemplate":"H5 acima de {params.limit}"}' >/dev/null
coordenador POST "/v1/rules/$RULE/versions/2:activate" >/dev/null
version_b="$(active_version)"
wait_for 150000 "\"ruleSetVersion\":$version_b" "conjunto com o novo threshold chegou"
txid="$(send 150000)"; sleep 3
decision_of "$txid" | grep -q "\"ruleId\":\"$RULE\"" && fail "a regra ainda disparou com o threshold novo"
echo "  150000 já não dispara a regra"

echo "3. desligar a regra (kill switch)"
analista POST "/v1/rules/$RULE:disable" >/dev/null
version_c="$(active_version)"
wait_for 600000 "\"ruleSetVersion\":$version_c" "conjunto sem a regra chegou"
txid="$(send 600000)"; sleep 3
decision_of "$txid" | grep -q "\"ruleId\":\"$RULE\"" && fail "a regra desligada ainda disparou"
echo "  600000 também não dispara: a regra saiu"

echo "4. rollback para o conjunto $version_a (o da regra com limit 100000)"
coordenador POST "/v1/rule-sets/$version_a:rollback" >/dev/null
version_d="$(active_version)"
wait_for 150000 "\"ruleId\":\"$RULE\"" "rollback devolveu a regra"

after="$(identity)"
echo "identidade depois: $after"
[ "$before" = "$after" ] || fail "o motor foi reiniciado durante o teste ($before -> $after)"
echo "OK: as quatro mudanças valeram em até ${LIMIT_SECONDS}s, sem reiniciar o motor (versões $version_a, $version_b, $version_c, $version_d)"
