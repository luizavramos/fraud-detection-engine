#!/bin/sh
# Roda o processo inteiro no compose, de ponta a ponta, e confere o resultado:
#   infraestrutura -> gestão de regras (conjunto semeado v1 chega ao motor pela outbox + Debezium) -> perfil do cliente (CDC)
#   -> motor, notificações e receiver -> lista de bloqueio -> duas transações enviadas ao receiver
#   -> decisão em fraud-decisions.v1, alerta HIGH em fraud-alerts.v1 e entregas ao antifraude e ao cliente (simuladores).
# Opcional: WITH_BACKFILL=1 semeia N transações no banco legado e roda a carga de histórico (spec 006).
#
# Uso (na raiz deste repositório; os repositórios de código ficam ao lado, ver README):  sh infra/run-all.sh
# Variáveis:
#   BUILD=1           empacota os JARs (./mvnw -DskipTests package) em cada repositório antes; sem isso, só empacota o que faltar
#   WITH_BACKFILL=1   inclui a carga de histórico (N=2000 transações por padrão, ver N)
#   WITH_OBS=1        sobe também Prometheus e Grafana (usa mais memória; com Docker de 4 GB, deixe desligado)
#   N                 transações do backfill (padrão 2000)
# Com 4 GB no Docker, não suba nada além disso ao mesmo tempo (ver "Memória" no README).
set -eu

export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# pasta dos repositórios de código: CODE_DIR, ou a do RECEIVER_CONTEXT do .env (o compose usa os mesmos *_CONTEXT), ou ao lado
env_ctx="$(sed -n 's/^RECEIVER_CONTEXT=//p' "$ROOT/.env" 2>/dev/null | head -1 | tr -d '\r')"
if [ -n "${CODE_DIR:-}" ]; then CODE="$CODE_DIR"
elif [ -n "$env_ctx" ]; then CODE="$(cd "$ROOT/$(dirname "$env_ctx")" && pwd)"
else CODE="$ROOT/.."; fi
RECEIVER="${RECEIVER_URL:-http://localhost:8081}"
RULES="${RULES_URL:-http://localhost:8082}"
MOCKS="${MOCKS_URL:-http://localhost:18090}"
N="${N:-2000}"
KAFKA="docker compose exec -T kafka /opt/kafka/bin"
failures=0
step=0

T0="$(date +%s)"; TS="$T0"
say() { now="$(date +%s)"; [ "$step" -gt 0 ] && echo "  (etapa $step: $((now - TS))s; total $((now - T0))s)"; TS="$now"; step=$((step + 1)); echo; echo "== $step. $*"; }
fail() { echo "FALHOU: $*" >&2; exit 1; }
check() { # descrição condição(0 = ok)
  if [ "$2" -eq 0 ]; then echo "  ok: $1"; else echo "  FALHOU: $1" >&2; failures=$((failures + 1)); fi
}
wait_until() { # descrição segundos comando...
  desc="$1"; limit="$2"; shift 2
  start="$(date +%s)"
  until "$@" >/dev/null 2>&1; do
    [ $(( $(date +%s) - start )) -lt "$limit" ] || fail "$desc: não ficou pronto em ${limit}s"
    sleep 3
  done
  echo "  pronto: $desc ($(( $(date +%s) - start ))s)"
}
ready() { curl -sf -m 5 "http://localhost:$1/actuator/health/readiness"; }
topic_has() { # tópico texto -> 0 se alguma mensagem (lida só do que foi confirmado) contém o texto
  $KAFKA/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic "$1" --from-beginning --timeout-ms 5000 \
    --isolation-level read_committed 2>/dev/null | grep -q "$2"
}
wait_topic() { desc="$1"; limit="$2"; shift 2; wait_until "$desc" "$limit" topic_has "$@"; }

# sobe serviços da aplicação; só reconstrói a imagem se ela não existir (ou BUILD=1), para não depender do Docker Hub à toa
app_up() {
  flags=""
  for s in "$@"; do docker image inspect "motor-fraude_$s" >/dev/null 2>&1 || flags="--build"; done
  [ "${BUILD:-0}" = "1" ] && flags="--build"
  docker compose --profile app up -d $flags "$@"
}
package() { # repositório
  jar="$(ls "$CODE/$1"/target/"$1"*.jar 2>/dev/null | head -1 || true)"
  if [ "${BUILD:-0}" = "1" ] || [ -z "$jar" ]; then
    echo "  empacotando $1 ..."
    [ -d "$CODE/$1" ] || fail "repositório $1 não encontrado em $CODE (clone ao lado, ver README)"
    (cd "$CODE/$1" && ./mvnw -q -DskipTests package) || fail "falha ao empacotar $1 (JAVA_HOME aponta para o JDK 21?)"
  fi
}

say "Empacotar o que falta (BUILD=1 refaz tudo)"
for r in rule-management receiver customer-service detection-engine notification-service transaction-service; do package "$r"; done

say "Infraestrutura (Kafka, Postgres, banco legado, Redis, Connect, simuladores dos provedores)"
# Em etapas, não tudo de uma vez: subir Kafka, Postgres, banco legado e Connect juntos já derrubou o Docker Desktop
# (4 GB, notebook de 8 GB). Cada etapa espera ficar saudável antes da próxima.
docker compose up -d --wait kafka postgres
docker compose up -d --wait kafka-init redis provider-mocks 2>/dev/null || docker compose up -d kafka-init redis provider-mocks
docker compose up -d --wait bank-db
docker compose up -d connect
[ "${WITH_OBS:-0}" = "1" ] && docker compose up -d prometheus grafana
wait_until "Kafka Connect" 240 curl -sf -m 5 http://localhost:18083/connectors
wait_until "tópicos criados" 120 sh -c 'test "$(docker inspect -f "{{.State.Status}} {{.State.ExitCode}}" "$(docker compose ps -a -q kafka-init)")" = "exited 0"'

say "Gestão de regras e a outbox (o conjunto v1, já semeado, chega ao tópico rule-sets.v1)"
app_up rule-management
wait_until "rule-management" 180 ready 8082
sh infra/connect/register-rule-sets-outbox.sh
wait_topic "conjunto de regras em rule-sets.v1" 90 rule-sets.v1 '"version"'

say "Perfil do cliente (CDC do banco legado -> customer-service)"
sh infra/connect/register-connectors.sh customers-cdc
app_up customer-service
wait_until "customer-service" 180 ready 8083
wait_topic "perfis do banco legado em cdc.bank.customers" 90 cdc.bank.customers '"customer_id"\|"customerId"\|"op"'
sleep 15   # o customer-service consome o snapshot do CDC (10 mil clientes) para o estado local
if [ "${WITH_BACKFILL:-0}" = "1" ]; then
  # O backfill vem ANTES do motor: o histórico fica no tópico transaction-history-backfill.v1 (7 dias) e o motor o lê quando
  # subir. Assim Connect, banco legado e transaction-service não precisam coexistir com motor, notificações e receiver.
  say "Carga de histórico (spec 006): $N transações do banco legado para o tópico do motor"
  CUTOVER_AT="${CUTOVER_AT:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
  export CUTOVER_AT
  echo "  CUTOVER_AT=$CUTOVER_AT, N=$N"
  ANCHOR="$CUTOVER_AT" N="$N" sh infra/seed/transactions.sh
  app_up transaction-service
  wait_until "transaction-service" 180 ready 8086
  sh infra/connect/register-connectors.sh transactions-cdc
  wait_until "backfill COMPLETED" 300 sh -c 'curl -sf -m 5 http://localhost:8096/v1/backfill/status | grep -q COMPLETED'
  curl -s -m 5 http://localhost:8096/v1/backfill/status; echo
  wait_topic "histórico em transaction-history-backfill.v1" 60 transaction-history-backfill.v1 '"transactionId"'
  docker compose stop transaction-service
fi
if [ "${KEEP_ALL:-0}" != "1" ]; then
  # Com 4 GB no Docker, motor + notificações + receiver não cabem junto do Connect e do banco legado (o Docker travou com
  # i/o timeout). O perfil já está no estado do customer-service, as regras no tópico e, se pedido, o histórico também;
  # a mudança de regras a quente e um novo snapshot precisam deles, por isso KEEP_ALL=1 os mantém.
  echo "  liberando memória: parando Connect e banco legado (KEEP_ALL=1 mantém)"
  docker compose stop connect bank-db
fi

say "Motor, notificações e receiver"
sh infra/seed/blocklist.sh add >/dev/null
app_up detection-engine notification-service receiver
wait_until "detection-engine" 240 ready 8084
wait_until "notification-service" 180 ready 8085
wait_until "receiver" 180 ready 8081

say "Transações enviadas ao receiver"
env -u MSYS_NO_PATHCONV sh infra/mocks/mock-control.sh reset >/dev/null   # esse script usa curl -o, que falha com MSYS_NO_PATHCONV=1
RUN="run$(date +%s)"
# conta nova a cada execução: o notification-service suprime o aviso ao cliente por conta numa janela de 10 min (spec 005, FR-007)
ACCOUNT="acc_tok_$(date +%s | cut -c3-10)"
send() { # transactionId contraparte valor
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # sem `curl -o`: com MSYS_NO_PATHCONV=1 o curl do Windows falha ao gravar em /tmp (código 23) e o set -e matava o script
  curl -s -m 10 -w '\n%{http_code}' -X POST "$RECEIVER/v1/transactions:evaluate" \
    -H 'Content-Type: application/json' -H 'X-Client-Id: pix-core' -H "Idempotency-Key: $1" \
    -d '{"schemaVersion":1,"transactionId":"'"$1"'","accountId":"'"$ACCOUNT"'","customerId":"cus_tok_00000001","type":"PIX","amount":{"value":'"$3"',"currency":"BRL"},"counterparty":{"accountToken":"'"$2"'","bankIspb":"60701190"},"channel":"MOBILE","deviceIdHash":"dev_0f3a9c1d5b7e2a64","occurredAt":"'"$now"'"}' | tail -n 1
}
TX_OK="${RUN}-normal-aaaa"
TX_BAD="${RUN}-flagged-bbbb"
code_ok="$(send "$TX_OK" acc_tok_00004711 1500)"
code_bad="$(send "$TX_BAD" acc_tok_00000013 1500)"
check "receiver aceitou a transação normal (HTTP $code_ok)" "$([ "$code_ok" = 202 ] && echo 0 || echo 1)"
check "receiver aceitou a transação com contraparte em lista de bloqueio (HTTP $code_bad)" "$([ "$code_bad" = 202 ] && echo 0 || echo 1)"

say "Resultado: decisões, alerta e entregas"
wait_topic "decisão da transação normal" 60 fraud-decisions.v1 "\"transactionId\":\"$TX_OK\""
wait_topic "decisão da transação em lista de bloqueio" 60 fraud-decisions.v1 "\"transactionId\":\"$TX_BAD\""
wait_topic "alerta da transação em lista de bloqueio" 60 fraud-alerts.v1 "\"transactionId\":\"$TX_BAD\""
topic_has fraud-alerts.v1 "\"transactionId\":\"$TX_OK\"" && sem=1 || sem=0
check "a transação normal não gerou alerta" "$sem"
sleep 8   # o notification-service consome o alerta e chama os provedores
count() { env -u MSYS_NO_PATHCONV sh infra/mocks/mock-control.sh count "$1"; }   # contagem exata do WireMock (sem curl -o)
anti="$(count antifraud-legacy)"
push="$(count push-provider)"
check "antifraude recebeu o alerta uma vez (chamadas: $anti)" "$([ "$anti" -eq 1 ] && echo 0 || echo 1)"
check "cliente recebeu o aviso HIGH por push (chamadas: $push)" "$([ "$push" -ge 1 ] && echo 0 || echo 1)"

echo
echo "  (etapa $step: $(( $(date +%s) - TS ))s)"; echo "Tempo total: $(( $(date +%s) - T0 ))s"
echo "Endereços: receiver $RECEIVER, regras $RULES, motor http://localhost:8084, notificações http://localhost:8085, Connect http://localhost:18083, simuladores $MOCKS"
if [ "$failures" -eq 0 ]; then
  echo "OK: o processo inteiro funcionou (0 falhas)."
else
  echo "$failures verificação(ões) falharam." >&2
  exit 1
fi
