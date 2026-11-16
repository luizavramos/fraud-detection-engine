#!/bin/sh
# Mede a latência fim a fim do motor (engine_e2e_latency_seconds, p50/p99) com tráfego ao vivo constante e SEM backfill,
# para isolar o efeito de uma variável por vez (nível de log, memória do contêiner, aquecimento).
# Manda LINES comandos (padrão 24000) a ~250/s para transaction-commands.v1 com ids novos e, passada a coleta do
# Prometheus, calcula os quantis só do que entrou durante o envio (increase sobre a duração).
#
# Pré-requisitos: kafka, prometheus e o motor no ar e com o atraso em 0; /tmp/live.txt com linhas "conta|json"
# (ver infra/backfill-verify.sh). Uso: sh infra/engine-latency.sh [rótulo]
set -eu
LABEL="${1:-sem-rotulo}"
LINES="${LINES:-24000}"
PER_TICK="${PER_TICK:-25}"          # 25 linhas por 0,1 s = ~250/s
LINGER="${LINGER:-1000}"            # linger.ms do produtor; o padrão do console-producer (1000) soma até 1 s à latência medida
LIVE_FILE="${LIVE_FILE:-/tmp/live.txt}"
export MSYS_NO_PATHCONV=1
KAFKA="docker compose exec -T kafka /opt/kafka/bin"
PROM="http://localhost:9090/api/v1/query"
q() { curl -s -m 10 -G "$PROM" --data-urlencode "query=$1" | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p'; }

NEW="lv$(date +%s)_"
sed -E "s/lv[0-9]+_/$NEW/g" "$LIVE_FILE" | head -n "$LINES" > /tmp/live-latency.txt
START=$(date +%s)
docker compose exec -T kafka sh -c '
  i=0; while IFS= read -r l; do printf "%s\n" "$l"; i=$((i+1)); [ $((i % '"$PER_TICK"')) -eq 0 ] && sleep 0.1; done |
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transaction-commands.v1 \
    --producer-property linger.ms='"$LINGER"' --property parse.key=true --property key.separator="|"' < /tmp/live-latency.txt > /tmp/live-latency.out 2>&1 &
SEND_PID=$!
# CPU dos contêineres durante o envio (docker stats a cada ~10 s); o pico por contêiner entra no resumo
: > /tmp/live-latency-cpu.txt
while kill -0 "$SEND_PID" 2>/dev/null; do
  docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' >> /tmp/live-latency-cpu.txt 2>/dev/null || true
  sleep 8
done
wait "$SEND_PID" || true
cpu="$(awk '{v=$2; sub(/%/,"",v); n=$1; sub(/motor-fraude-/,"",n); sub(/-1$/,"",n); if (v+0>m[n]) m[n]=v+0} END{for(k in m) printf "%s=%d%% ", k, m[k]}' /tmp/live-latency-cpu.txt)"
DUR=$(( $(date +%s) - START ))
sleep 25                              # o Prometheus coleta a cada 15 s
W=$((DUR + 25 + 5))
n="$(q "sum(increase(engine_e2e_latency_seconds_count[${W}s]))")"
p50="$(q "histogram_quantile(0.5, sum(increase(engine_e2e_latency_seconds_bucket[${W}s]))by(le))")"
p90="$(q "histogram_quantile(0.9, sum(increase(engine_e2e_latency_seconds_bucket[${W}s]))by(le))")"
p99="$(q "histogram_quantile(0.99, sum(increase(engine_e2e_latency_seconds_bucket[${W}s]))by(le))")"
mem="$(docker stats --no-stream --format '{{.MemUsage}}' motor-fraude-detection-engine-1 2>/dev/null | cut -d/ -f1)"
echo "[$LABEL] enviados=$LINES em ${DUR}s (~$((LINES / (DUR > 0 ? DUR : 1)))/s); decisões na janela=$n; p50=${p50}s p90=${p90}s p99=${p99}s; memória do motor=$mem; pico de CPU: ${cpu}"
