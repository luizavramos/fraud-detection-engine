#!/bin/sh
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
