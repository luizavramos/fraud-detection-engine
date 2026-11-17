#!/bin/sh
set -eu

MOCKS="${MOCKS_URL:-http://localhost:18090}"

id_of() {
  case "$1" in
    antifraud-legacy) echo 00000000-0000-4000-8000-000000000001 ;;
    push-provider) echo 00000000-0000-4000-8000-000000000002 ;;
    email-provider) echo 00000000-0000-4000-8000-000000000003 ;;
    sms-provider) echo 00000000-0000-4000-8000-000000000004 ;;
    contacts) echo 00000000-0000-4000-8000-000000000005 ;;
    *) echo "simulador desconhecido: $1" >&2; exit 2 ;;
  esac
}

admin() { curl -s -m 20 -H 'Content-Type: application/json' "$@"; }

remove_failure() {
  admin -o /dev/null -X DELETE "$MOCKS/__admin/mappings/$(id_of "$1")" || true
}

set_failure() {
  remove_failure "$1"
  admin -o /dev/null -X POST "$MOCKS/__admin/mappings" -d '{
    "id": "'"$(id_of "$1")"'",
    "priority": 1,
    "request": { "urlPathPattern": "/'"$1"'/.*" },
    "response": { "status": '"$2"', "fixedDelayMilliseconds": '"$3"', "headers": { "Content-Type": "application/json" }, "body": "{\"simulated\":true}" }
  }'
}

count_json() {
  if [ -n "${2:-}" ]; then
    admin -X POST "$MOCKS/__admin/requests/count" -d '{"urlPathPattern":"/'"$1"'/.*","headers":{"Idempotency-Key":{"equalTo":"'"$2"'"}}}'
  else
    admin -X POST "$MOCKS/__admin/requests/count" -d '{"urlPathPattern":"/'"$1"'/.*"}'
  fi
}

case "${1:-}" in
  reset)
    for sim in antifraud-legacy push-provider email-provider sms-provider contacts; do remove_failure "$sim"; done
    admin -o /dev/null -X DELETE "$MOCKS/__admin/requests"
    echo "simuladores normais e diário zerado"
    ;;
  count)
    count_json "$2" | tr -d "[:space:]" | sed 's/.*"count" *: *\([0-9]*\).*/\1/'
    ;;
  count-key)
    count_json "$2" "$3" | tr -d "[:space:]" | sed 's/.*"count" *: *\([0-9]*\).*/\1/'
    ;;
  antifraud-legacy|push-provider|email-provider|sms-provider|contacts)
    case "${2:-}" in
      unavailable) set_failure "$1" 503 0; echo "$1: respondendo 503" ;;
      slow) set_failure "$1" 202 5000; echo "$1: respondendo 202 depois de 5 s" ;;
      ok) remove_failure "$1"; echo "$1: normal" ;;
      *) echo "uso: mock-control.sh $1 unavailable|slow|ok" >&2; exit 2 ;;
    esac
    ;;
  *)
    echo "uso: mock-control.sh <simulador> unavailable|slow|ok | reset | count <simulador> | count-key <simulador> <chave>" >&2
    exit 2
    ;;
esac
