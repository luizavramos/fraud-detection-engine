#!/bin/sh
set -eu

MOCKS="${MOCKS_URL:-http://localhost:18090}"
DIR="$(cd "$(dirname "$0")" && pwd)"
failures=0

check() {
  if [ "$2" = "$3" ]; then
    echo "  ok: $1"
  else
    echo "  FALHOU: $1 (esperado '$2', obtido '$3')" >&2
    failures=$((failures + 1))
  fi
}
code() { curl -s -o /dev/null -w '%{http_code}' -m 20 "$@"; }
body() { curl -s -m 20 "$@"; }

echo "1. respostas de sucesso"
check "antifraude aceita o webhook" 202 "$(code -X POST -H 'Content-Type: application/json' -H 'X-Signature: abc' -H 'Idempotency-Key: a1' -d '{}' "$MOCKS/antifraud-legacy/webhooks/fraud-alerts")"
check "antifraude devolve o número do caso" yes "$(body -X POST -H 'Idempotency-Key: a1' -d '{}' "$MOCKS/antifraud-legacy/webhooks/fraud-alerts" | grep -q '"caseId"' && echo yes || echo no)"
check "push aceita" 202 "$(code -X POST -H 'Idempotency-Key: a1:PUSH' -d '{}' "$MOCKS/push-provider/v1/push")"
check "e-mail aceita" 202 "$(code -X POST -H 'Idempotency-Key: a1:EMAIL' -d '{}' "$MOCKS/email-provider/v1/email")"
check "SMS aceita" 202 "$(code -X POST -H 'Idempotency-Key: a1:SMS' -d '{}' "$MOCKS/sms-provider/v1/sms")"
check "o provedor ecoa o Idempotency-Key" yes "$(body -X POST -H 'Idempotency-Key: chave-xyz:PUSH' -d '{}' "$MOCKS/push-provider/v1/push" | grep -q 'chave-xyz:PUSH' && echo yes || echo no)"

echo "2. contatos"
check "contato de uma conta existente" 200 "$(code "$MOCKS/contacts/v1/contacts/acc_tok_00000001")"
check "o contato traz push, e-mail e telefone" yes "$(body "$MOCKS/contacts/v1/contacts/acc_tok_00000001" | grep -q '"pushToken"' && body "$MOCKS/contacts/v1/contacts/acc_tok_00000001" | grep -q '"email"' && body "$MOCKS/contacts/v1/contacts/acc_tok_00000001" | grep -q '"phone"' && echo yes || echo no)"
check "conta sem contato devolve 404" 404 "$(code "$MOCKS/contacts/v1/contacts/acc_tok_nocontact01")"

echo "3. falha configurável e isolada por simulador"
sh "$DIR/mock-control.sh" push-provider unavailable >/dev/null
check "push em 503" 503 "$(code -X POST -d '{}' "$MOCKS/push-provider/v1/push")"
check "e-mail segue aceitando com o push fora" 202 "$(code -X POST -d '{}' "$MOCKS/email-provider/v1/email")"
check "antifraude segue aceitando com o push fora" 202 "$(code -X POST -d '{}' "$MOCKS/antifraud-legacy/webhooks/fraud-alerts")"
sh "$DIR/mock-control.sh" push-provider slow >/dev/null
start="$(date +%s)"
check "push lento ainda responde 202" 202 "$(code -X POST -d '{}' "$MOCKS/push-provider/v1/push")"
elapsed=$(( $(date +%s) - start ))
check "push lento demora pelo menos 4 s" yes "$([ "$elapsed" -ge 4 ] && echo yes || echo no)"
sh "$DIR/mock-control.sh" push-provider ok >/dev/null
check "push volta ao normal" 202 "$(code -X POST -d '{}' "$MOCKS/push-provider/v1/push")"

echo "4. diário de requisições"
sh "$DIR/mock-control.sh" reset >/dev/null
code -X POST -H 'Idempotency-Key: j1:PUSH' -d '{}' "$MOCKS/push-provider/v1/push" >/dev/null
code -X POST -H 'Idempotency-Key: j1:PUSH' -d '{}' "$MOCKS/push-provider/v1/push" >/dev/null
code -X POST -H 'Idempotency-Key: j2:EMAIL' -d '{}' "$MOCKS/email-provider/v1/email" >/dev/null
check "2 chamadas ao push" 2 "$(sh "$DIR/mock-control.sh" count push-provider)"
check "1 chamada ao e-mail" 1 "$(sh "$DIR/mock-control.sh" count email-provider)"
check "2 chamadas com a mesma chave j1:PUSH" 2 "$(sh "$DIR/mock-control.sh" count-key push-provider 'j1:PUSH')"

if [ "$failures" -ne 0 ]; then
  echo "$failures verificação(ões) falharam" >&2
  exit 1
fi
echo "simuladores OK"
