#!/bin/sh
set -eu
RATE="${RATE:-2000}"
LIVE_START="${LIVE_START:-60}"
LIVE_FILE="${LIVE_FILE:-/tmp/live.txt}"
LIVE_PER_TICK="${LIVE_PER_TICK:-25}"
export MSYS_NO_PATHCONV=1 BACKFILL_RATE_PER_SECOND="$RATE" TRANSACTION_SERVICE_CONTEXT="${TRANSACTION_SERVICE_CONTEXT:-../transaction-service}"
KAFKA="docker compose exec -T kafka /opt/kafka/bin"
PROM="http://localhost:9090/api/v1/query"

q() { curl -s -m 10 -G "$PROM" --data-urlencode "query=$1" | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p'; }
p99() { q "histogram_quantile(0.99, sum(rate(engine_e2e_latency_seconds_bucket[$1]))by(le))"; }
lag() { $KAFKA/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group detection-engine 2>/dev/null \
  | awk '$2=="transaction-history-backfill.v1" && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}'; }

echo "# $(date -u +%T) taxa=$RATE/s; tráfego ao vivo a partir de ${LIVE_START}s (~$((LIVE_PER_TICK * 10))/s)"
echo "# $(date -u +%T) linha de base (motor ocioso): p99 5 min = $(p99 5m) s; atraso do histórico = $(lag)"
docker compose rm -sf transaction-service >/dev/null 2>&1 || true
docker volume rm motor-fraude_transaction-service-data >/dev/null 2>&1 || true
reset_ok=0
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  if $KAFKA/kafka-streams-application-reset.sh --bootstrap-server localhost:9092 --application-id transaction-service \
      --input-topics cdc.bank.transactions > /tmp/app-reset.log 2>&1; then reset_ok=1; break; fi
  sleep 10
done
echo "# $(date -u +%T) reset do transaction-service: $([ "$reset_ok" = 1 ] && echo ok || echo FALHOU)"
[ "$reset_ok" = 1 ] || { tail -3 /tmp/app-reset.log; exit 1; }
docker compose --profile app up -d transaction-service >/dev/null 2>&1
for i in $(seq 1 40); do curl -sf -m 3 localhost:8096/v1/backfill/status >/dev/null 2>&1 && break; sleep 3; done
START=$(date +%s)
echo "# $(date -u +%T) transaction-service no ar; acompanhando"

live_started=0; live_ended=0; live_pid=""
prev=0; prev_t=0; last_state=""; flips=0; peak=0
while :; do
  now=$(date +%s); t=$((now - START))
  if [ "$live_started" = 0 ] && [ "$t" -ge "$LIVE_START" ]; then
    live_started=1
    (head -n 30000 "$LIVE_FILE" | docker compose exec -T kafka sh -c '
      i=0; while IFS= read -r l; do printf "%s\n" "$l"; i=$((i+1)); [ $((i % '"$LIVE_PER_TICK"')) -eq 0 ] && sleep 0.1; done |
      /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transaction-commands.v1 \
        --property parse.key=true --property key.separator="|"' > /dev/null 2>&1) &
    live_pid=$!
    echo "# $(date -u +%T) t=${t}s tráfego ao vivo iniciado"
  fi
  s="$(curl -s -m 5 localhost:8096/v1/backfill/status || true)"
  state="$(echo "$s" | sed -n 's/.*"state":"\([A-Z]*\)".*/\1/p')"
  pub="$(echo "$s" | sed -n 's/.*"published":\([0-9]*\).*/\1/p')"
  if [ -n "$state" ]; then
    delta=$(( ${pub:-0} - prev )); prev=${pub:-0}
    elapsed=$(( t - prev_t )); [ "$elapsed" -lt 1 ] && elapsed=1; prev_t=$t
    rate=$(( delta / elapsed )); [ "$rate" -gt "$peak" ] && peak=$rate
    [ -n "$last_state" ] && [ "$state" != "$last_state" ] && flips=$((flips + 1))
    last_state="$state"
    echo "t=${t}s estado=$state publicados=$pub (~${rate}/s) atraso_motor=$(lag)"
    [ "$state" = COMPLETED ] && break
  fi
  [ "$t" -gt 1500 ] && { echo "# desistiu aos 1500 s"; break; }
  sleep 5
done
echo "# transições de estado: $flips; pico de publicação observado (janela de 5 s): ${peak}/s (limite $RATE/s)"
[ -n "$live_pid" ] && wait "$live_pid" 2>/dev/null || true
echo "# $(date -u +%T) latência p99 do motor: últimos 2 min = $(p99 2m) s; últimos 10 min = $(p99 10m) s; atraso do histórico = $(lag)"
total() { $KAFKA/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }
echo "# comandos ao vivo no tópico: $(total transaction-commands.v1); decisões geradas: $(total fraud-decisions.v1)"
echo "# duração do backfill:$(( $(date +%s) - START )) s"
