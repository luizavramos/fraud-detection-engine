#!/bin/sh
# Teste de caos H6 da spec 004: derruba uma instância do motor no meio da carga e confere, com o consumidor-auditor, que cada
# transação aceita tem exatamente uma decisão (nenhuma faltante, nenhuma duplicada, nenhum alerta em duplicidade).
#
# Antes (na raiz do repositório; empacote receiver, detection-engine, load-generator e auditor com ./mvnw -DskipTests package):
#   docker compose up -d kafka kafka-init redis prometheus
#   docker compose --profile app --profile chaos up -d --build receiver detection-engine detection-engine-2
#   Espere as duas instâncias dividirem as partições (até ~1 min, o intervalo do probing rebalance):
#   docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group detection-engine --members
# Uso: sh infra/chaos-h6.sh
# Escala local: 150 TPS por 45 s (mais 5 s de aquecimento) = 7.500 conexões, abaixo do limite de portas efêmeras do Windows.
set -eu

TPS="${TPS:-150}"
DURATION="${DURATION:-45s}"
KILL_AFTER="${KILL_AFTER:-25}"
VICTIM="${VICTIM:-motor-fraude-detection-engine-1}"
WORK="${WORK:-/tmp/h6}"
GENERATOR="${GENERATOR:-../motor-fraude/load-generator/target/load-generator.jar}"
AUDITOR="${AUDITOR:-../motor-fraude/auditor/target/auditor.jar}"
export MSYS_NO_PATHCONV=1

mkdir -p "$WORK"
rm -f "$WORK/aceitas-caos.log" "$WORK/carga-caos.txt"

echo "carga: $TPS TPS por $DURATION, matando $VICTIM depois de ${KILL_AFTER}s"
java -jar "$GENERATOR" --tps "$TPS" --duration "$DURATION" --warmup 5s --seed 11 --accepted-log "$WORK/aceitas-caos.log" \
  > "$WORK/carga-caos.txt" 2>&1 &
load_pid=$!

sleep "$KILL_AFTER"
echo "docker kill $VICTIM às $(date -u +%H:%M:%S)"
docker kill "$VICTIM" >/dev/null
wait "$load_pid" || true

grep -E "Requisições|Respostas" "$WORK/carga-caos.txt" || true
echo "aceitas no log: $(wc -l < "$WORK/aceitas-caos.log")"
echo "auditoria (espera o motor restante terminar e os dados transacionais ficarem visíveis)..."
java -jar "$AUDITOR" --accepted-log "$WORK/aceitas-caos.log" --idle 30s --timeout 4m
