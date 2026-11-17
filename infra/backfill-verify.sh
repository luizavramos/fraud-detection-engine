#!/bin/sh
set -eu

FIRST="${1:?conta inicial}"
LAST="${2:?conta final}"
CUTOVER_AT="${CUTOVER_AT:-2026-11-15T00:00:00Z}"
DECISION_WAIT="${DECISION_WAIT:-40}"
export MSYS_NO_PATHCONV=1
KAFKA="docker compose exec -T kafka /opt/kafka/bin"
PSQL="docker compose exec -T bank-db psql -v ON_ERROR_STOP=1 -U bank -d bank -At -F ,"
RUN="vfy$(date +%s)"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
DAY="$(date -u +%Y-%m-%d)T00:00:00Z"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

acc() { printf 'acc_tok_%08d' "$1"; }
FIRST_ACC="$(acc "$FIRST")"; LAST_ACC="$(acc "$LAST")"

$PSQL <<SQL > "$WORK/sql.csv"
select a.account_token, coalesce(c.n, 0), coalesce(c.sum_cents, 0), coalesce(m.max_cents, 0)
from (select distinct account_token from bank.transactions where account_token between '$FIRST_ACC' and '$LAST_ACC') a
left join (
    select account_token, count(*) n, sum(amount_cents) sum_cents from bank.transactions
    where occurred_at >= timestamptz '$DAY' - 29 * 86400 * interval '1 second'
      and occurred_at >= timestamptz '$CUTOVER_AT' - 90 * 86400 * interval '1 second' and occurred_at < timestamptz '$CUTOVER_AT'
    group by 1) c using (account_token)
left join (
    select account_token, max(amount_cents) max_cents from bank.transactions
    where occurred_at >= timestamptz '$DAY' - 89 * 86400 * interval '1 second'
      and occurred_at >= timestamptz '$CUTOVER_AT' - 90 * 86400 * interval '1 second' and occurred_at < timestamptz '$CUTOVER_AT'
    group by 1) m using (account_token)
order by 1;
SQL
[ -s "$WORK/sql.csv" ] || { echo "nenhuma conta em $FIRST_ACC..$LAST_ACC no banco legado" >&2; exit 2; }

: > "$WORK/extra.csv"
if [ -n "${EXTRA_COMMANDS_FILE:-}" ]; then
  grep -v '"transactionId":"vfy' "$EXTRA_COMMANDS_FILE" | awk '
    match($0, /"transactionId":"[^"]*"/) { id = substr($0, RSTART + 17, RLENGTH - 18); if (id in seen) next; seen[id] = 1 }
    match($0, /"accountId":"[^"]*"/) { a = substr($0, RSTART + 13, RLENGTH - 14) }
    match($0, /"amount":\{"value":[0-9]+/) { v = substr($0, RSTART + 18, RLENGTH - 18); print a "," v }' > "$WORK/extra.csv"
fi
awk -F, '
  NR == FNR { n[$1]++; s[$1] += $2; if ($2 + 0 > m[$1] + 0) m[$1] = $2; next }
  { c = $2 + n[$1]; t = $3 + s[$1]; mx = ($4 + 0 > m[$1] + 0) ? $4 : m[$1]; printf "%s,%d,%d,%d\n", $1, c, (c == 0 ? 0 : int(t / c)), mx }
' "$WORK/extra.csv" "$WORK/sql.csv" > "$WORK/expected.csv"
TOTAL="$(wc -l < "$WORK/expected.csv" | tr -d ' ')"

if [ -n "${DECISIONS_FILE:-}" ]; then
  RUN="${RUN_ID:?informe RUN_ID junto com DECISIONS_FILE}"
  grep "\"transactionId\":\"${RUN}_" "$DECISIONS_FILE" > "$WORK/decisions.txt" || true
else
  cut -d, -f1 "$WORK/expected.csv" | awk -v run="$RUN" -v now="$NOW" '{
    printf "%s|{\"schemaVersion\":1,\"transactionId\":\"%s_%s\",\"accountId\":\"%s\",\"customerId\":\"cus_tok_%s\",\"type\":\"PIX\",", $1, run, NR, $1, substr($1, 9)
    printf "\"amount\":{\"value\":5000,\"currency\":\"BRL\"},\"counterparty\":{\"accountToken\":\"acc_tok_dest0001\",\"bankIspb\":\"60746948\"},"
    printf "\"channel\":\"MOBILE\",\"deviceIdHash\":\"dev_aaaaaaaa\",\"occurredAt\":\"%s\"}\n", now }' > "$WORK/commands.txt"
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transaction-commands.v1 \
    --property parse.key=true --property key.separator='|' < "$WORK/commands.txt" > /dev/null

  echo "RUN_ID=$RUN"
  for attempt in 1 2 3 4; do
    $KAFKA/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud-decisions.v1 --from-beginning \
      --timeout-ms $((DECISION_WAIT * 250)) 2>/dev/null > "$WORK/all-decisions.txt" || true
    grep "\"transactionId\":\"${RUN}_" "$WORK/all-decisions.txt" > "$WORK/decisions.txt" || true
    [ "$(wc -l < "$WORK/decisions.txt" | tr -d ' ')" -ge "$TOTAL" ] && break
  done
fi
GOT="$(wc -l < "$WORK/decisions.txt" | tr -d ' ')"

bad=0
while IFS=, read -r account n avg mx; do
  line="$(grep "\"accountId\":\"$account\"" "$WORK/decisions.txt" | head -1 || true)"
  if [ -z "$line" ]; then echo "SEM DECISÃO: $account"; bad=$((bad + 1)); continue; fi
  e_avg="$(echo "$line" | sed -n 's/.*"features.avgAmount30d":\([0-9-]*\).*/\1/p')"
  e_max="$(echo "$line" | sed -n 's/.*"features.maxAmount90d":\([0-9-]*\).*/\1/p')"
  e_cnt="$(echo "$line" | sed -n 's/.*"features.count30d":\([0-9-]*\).*/\1/p')"
  if [ "$e_avg" != "$avg" ] || [ "$e_max" != "$mx" ] || [ "$e_cnt" != "$((n + 1))" ]; then
    echo "DIFERENÇA $account: motor avg=$e_avg max=$e_max count=$e_cnt (inclui a transação avaliada); referência avg=$avg max=$mx count=$n (+1)"
    bad=$((bad + 1))
  fi
done < "$WORK/expected.csv"

echo "contas conferidas: $TOTAL; decisões recebidas: $GOT; com diferença ou sem decisão: $bad"
[ "$bad" -eq 0 ]
