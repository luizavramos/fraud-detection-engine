#!/usr/bin/env bash
# Teste de aceite da H1 (spec 000): "docker compose up -d" deixa tudo healthy em até 3 minutos
# e os tópicos Kafka existem com as partições e a política definidas.
#
# Uso: infra/smoke.sh [--fresh]
#   --fresh   derruba antes e apaga os volumes (prova o primeiro start do zero)
# Variáveis: TIMEOUT (segundos, padrão 180). O tempo conta a partir do "up"; imagens já devem estar baixadas
#            (rode "docker compose pull" antes — o primeiro download não faz parte do SLA de 3 minutos).
set -u
export MSYS_NO_PATHCONV=1   # Git Bash no Windows: não converter caminhos "/opt/..." dos comandos do container

cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; . ./.env; set +a; fi

TIMEOUT="${TIMEOUT:-180}"
GRAFANA_PORT="${GRAFANA_HOST_PORT:-3000}"
PROM_PORT="${PROMETHEUS_HOST_PORT:-9090}"
CONNECT_PORT="${CONNECT_HOST_PORT:-18083}"
PG_USER="${POSTGRES_USER:-motorfraude}"
BANK_USER="${BANK_DB_USER:-bank}"
KT=/opt/kafka/bin/kafka-topics.sh

pass=0; fail=0
ok()   { pass=$((pass + 1)); echo "  ok    $1"; }
bad()  { fail=$((fail + 1)); echo "  FALHA $1"; }
check(){ if eval "$2" >/dev/null 2>&1; then ok "$1"; else bad "$1"; fi; }

if [ "${1:-}" = "--fresh" ]; then
  echo ">> docker compose down -v"
  docker compose down -v --remove-orphans >/dev/null 2>&1
fi

echo ">> docker compose up -d"
start=$(date +%s)
docker compose up -d >/dev/null 2>&1 || { echo "docker compose up falhou"; docker compose up -d; exit 1; }

HEALTHY_SERVICES="kafka postgres bank-db redis connect prometheus grafana"
status_of() { docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$(docker compose ps -q "$1")" 2>/dev/null; }

echo ">> aguardando healthchecks (até ${TIMEOUT}s)"
while :; do
  pending=""
  for s in $HEALTHY_SERVICES; do
    [ "$(status_of "$s")" = "healthy" ] || pending="$pending $s"
  done
  init_id="$(docker compose ps -a -q kafka-init)"
  init_state="$(docker inspect --format '{{.State.Status}}/{{.State.ExitCode}}' "$init_id" 2>/dev/null)"
  [ "$init_state" = "exited/0" ] || pending="$pending kafka-init"
  [ -z "$pending" ] && break
  elapsed=$(( $(date +%s) - start ))
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "TEMPO ESGOTADO (${elapsed}s). Pendentes:$pending"
    docker compose ps -a
    exit 1
  fi
  sleep 3
done
elapsed=$(( $(date +%s) - start ))
echo "   tudo pronto em ${elapsed}s"

echo ">> H1.1 containers healthy"
for s in $HEALTHY_SERVICES; do ok "$s healthy"; done
ok "kafka-init terminou com código 0"

echo ">> H1.2 tópicos (kafka-topics --describe)"
# nome:partições:política   (plan 000 §3.1, perfil local)
EXPECTED="
transaction-commands.v1:6:delete
transaction-commands.dlq:1:delete
customer-profile.v1:6:compact
transaction-history-backfill.v1:6:delete
blocklist.v1:1:compact
rule-sets.v1:1:compact
fraud-decisions.v1:6:delete
fraud-alerts.v1:3:delete
alerts.retry.1m:1:delete
alerts.retry.5m:1:delete
alerts.retry.30m:1:delete
alerts.dlq:1:delete
cdc.bank.customers:3:delete
cdc.bank.customer_devices:3:delete
cdc.bank.transactions:3:delete
"
# Uma única JVM (kafka-topics.sh --describe sem --topic lista todos): cada chamada custa alguns segundos.
ALL_TOPICS="$(docker compose exec -T kafka "$KT" --bootstrap-server localhost:29092 --describe 2>/dev/null)"
topic_line() { echo "$ALL_TOPICS" | awk -v t="$1" '$1=="Topic:" && $2==t'; }
partitions_of() { topic_line "$1" | awk '{ for (i = 1; i < NF; i++) if ($i=="PartitionCount:") print $(i+1) }'; }
for line in $EXPECTED; do
  name="${line%%:*}"; rest="${line#*:}"; parts="${rest%%:*}"; policy="${rest#*:}"
  got_parts="$(partitions_of "$name")"
  if [ "$got_parts" = "$parts" ] && topic_line "$name" | grep -q "cleanup.policy=$policy"; then
    ok "$name ($parts partições, $policy)"
  else
    bad "$name (esperado $parts/$policy; obtido ${got_parts:-ausente})"
  fi
done
cp_a="$(partitions_of transaction-commands.v1)"
cp_b="$(partitions_of customer-profile.v1)"
cp_c="$(partitions_of transaction-history-backfill.v1)"
if [ -n "$cp_a" ] && [ "$cp_a" = "$cp_b" ] && [ "$cp_a" = "$cp_c" ]; then ok "co-particionamento (commands = profile = backfill = $cp_a)"; else bad "co-particionamento"; fi

echo ">> infraestrutura de apoio"
check "postgres: bancos rules_db e alerts_db" \
  "[ \"\$(docker compose exec -T postgres psql -U $PG_USER -d postgres -Atc \"select count(*) from pg_database where datname in ('rules_db','alerts_db')\")\" = 2 ]"
check "postgres (rules_db): wal_level=logical, para o Debezium da outbox" \
  "[ \"\$(docker compose exec -T postgres psql -U $PG_USER -d postgres -Atc 'show wal_level')\" = logical ]"
check "bank-db: wal_level=logical" \
  "[ \"\$(docker compose exec -T bank-db psql -U $BANK_USER -d bank -Atc 'show wal_level')\" = logical ]"
check "bank-db: 10 mil clientes sintéticos" \
  "[ \"\$(docker compose exec -T bank-db psql -U $BANK_USER -d bank -Atc 'select count(*) from bank.customers')\" = 10000 ]"
check "bank-db: publicações dbz_customers e dbz_transactions" \
  "[ \"\$(docker compose exec -T bank-db psql -U $BANK_USER -d bank -Atc \"select count(*) from pg_publication where pubname in ('dbz_customers','dbz_transactions')\")\" = 2 ]"
check "bank-db: colunas de dado pessoal presentes (provam a minimização)" \
  "[ \"\$(docker compose exec -T bank-db psql -U $BANK_USER -d bank -Atc \"select count(*) from information_schema.columns where table_schema='bank' and column_name in ('full_name','cpf','email','phone','payer_name','payer_cpf')\")\" = 6 ]"
check "redis responde PONG" "[ \"\$(docker compose exec -T redis redis-cli ping)\" = PONG ]"
check "kafka connect lista conectores" "curl -sf http://localhost:$CONNECT_PORT/connectors"
check "prometheus pronto" "curl -sf http://localhost:$PROM_PORT/-/ready"

echo ">> H1.3 grafana"
GF="http://localhost:$GRAFANA_PORT"
GF_AUTH="admin:${GRAFANA_ADMIN_PASSWORD:-admin}"
check "datasource Prometheus provisionado" "curl -sf -u $GF_AUTH $GF/api/datasources | grep -q '\"type\":\"prometheus\"'"
check "dashboard \"Motor de fraude\" provisionado" "curl -sf -u $GF_AUTH \"$GF/api/search?query=Motor\" | grep -q 'Motor de fraude'"

echo
echo "resultado: $pass ok, $fail falhas, healthy em ${elapsed}s (limite ${TIMEOUT}s)"
[ "$fail" -eq 0 ]
