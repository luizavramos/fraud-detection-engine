#!/bin/sh
set -eu

BOOTSTRAP="${BOOTSTRAP_SERVER:-kafka:29092}"
PROFILE="${PROFILE:-local}"
PARALLEL="${PARALLEL:-5}"
KT=/opt/kafka/bin/kafka-topics.sh

export KAFKA_HEAP_OPTS="-Xms32m -Xmx128m"
export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -Xshare:auto"

case "$PROFILE" in
  local) RF=1 ;;
  prod)  RF=3 ;;
  *) echo "PROFILE inválido: $PROFILE (use local ou prod)" >&2; exit 2 ;;
esac

D3=259200000
D7=604800000
D14=1209600000

TOPICS="
transaction-commands.v1|6|48|delete|$D7
transaction-commands.dlq|1|6|delete|$D14
customer-profile.v1|6|48|compact|-
transaction-history-backfill.v1|6|48|delete|$D7
blocklist.v1|1|6|compact|-
rule-sets.v1|1|1|compact|-
fraud-decisions.v1|6|48|delete|$D3
fraud-alerts.v1|3|12|delete|$D7
alerts.retry.1m|1|6|delete|$D14
alerts.retry.5m|1|6|delete|$D14
alerts.retry.30m|1|6|delete|$D14
alerts.dlq|1|6|delete|$D14
cdc.bank.customers|3|12|delete|$D7
cdc.bank.customer_devices|3|12|delete|$D7
cdc.bank.transactions|3|12|delete|$D7
"

echo "Aguardando o broker em $BOOTSTRAP ..."
tries=0
until "$KT" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -ge 30 ]; then echo "Broker indisponível" >&2; exit 1; fi
  sleep 2
done

LOGDIR="$(mktemp -d)"
pids=""
failed=0
n=0

wait_batch() {
  for p in $pids; do wait "$p" || failed=1; done
  pids=""
}

while IFS='|' read -r name p_local p_prod cleanup retention; do
  [ -n "$name" ] || continue
  if [ "$PROFILE" = "prod" ]; then partitions="$p_prod"; else partitions="$p_local"; fi
  set -- --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$name" \
         --partitions "$partitions" --replication-factor "$RF" \
         --config "cleanup.policy=$cleanup"
  if [ "$retention" != "-" ]; then set -- "$@" --config "retention.ms=$retention"; fi
  "$KT" "$@" >"$LOGDIR/$name.log" 2>&1 &
  pids="$pids $!"
  n=$((n + 1))
  if [ $((n % PARALLEL)) -eq 0 ]; then wait_batch; fi
done <<EOT
$TOPICS
EOT
wait_batch

if [ "$failed" -ne 0 ]; then
  echo "ERRO ao criar tópicos:" >&2
  grep -L '^Created topic\|already exists' "$LOGDIR"/*.log | while read -r f; do echo "--- $f" >&2; cat "$f" >&2; done
  exit 1
fi
echo "$n tópicos criados/confirmados."

ALL="$("$KT" --bootstrap-server "$BOOTSTRAP" --describe)"
count() { echo "$ALL" | awk -v t="$1" '$1=="Topic:" && $2==t { for (i = 1; i < NF; i++) if ($i=="PartitionCount:") print $(i+1) }'; }
a=$(count transaction-commands.v1)
b=$(count customer-profile.v1)
c=$(count transaction-history-backfill.v1)
if [ -z "$a" ] || [ "$a" != "$b" ] || [ "$a" != "$c" ]; then
  echo "ERRO: tópicos não co-particionados (commands=$a profile=$b backfill=$c)" >&2
  exit 1
fi

echo "Tópicos prontos (profile=$PROFILE, partições dos tópicos co-particionados=$a)."
