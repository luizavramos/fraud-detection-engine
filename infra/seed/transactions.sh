#!/bin/sh
# Gera transações sintéticas no banco legado (spec 006, T603): padrão 1 milhão, espalhadas por 120 dias antes de ANCHOR.
# Determinístico: o mesmo N e o mesmo ANCHOR geram sempre as mesmas linhas (contas 1..10000, as mesmas do init.sql).
# Idempotente: se a tabela já tiver N ou mais linhas, não faz nada (FORCE=1 apaga e gera de novo).
# Rode ANTES de registrar o conector transactions-cdc: o snapshot do Debezium lê o que já estiver na tabela.
# Uso: sh infra/seed/transactions.sh            (N=1000000, ANCHOR=agora)
#      N=20000 ANCHOR=2026-11-15T00:00:00Z sh infra/seed/transactions.sh
# Variáveis: N, ANCHOR, FORCE, BANK_DB_CONTAINER (padrão: serviço bank-db do compose)
set -eu

N="${N:-1000000}"
ANCHOR="${ANCHOR:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
FORCE="${FORCE:-0}"

if [ -n "${BANK_DB_CONTAINER:-}" ]; then
  PSQL="docker exec -i $BANK_DB_CONTAINER psql -v ON_ERROR_STOP=1 -U bank -d bank -At"
else
  PSQL="docker compose exec -T bank-db psql -v ON_ERROR_STOP=1 -U bank -d bank -At"
fi

existing="$($PSQL -c 'select count(*) from bank.transactions')"
if [ "$FORCE" = "1" ]; then
  $PSQL -c 'truncate bank.transactions'
  existing=0
fi
if [ "$existing" -ge "$N" ]; then
  echo "bank.transactions já tem $existing linhas (>= $N); nada a fazer (FORCE=1 para refazer)"
  exit 0
fi

echo "gerando $N transações em 120 dias antes de $ANCHOR ..."
$PSQL <<SQL
insert into bank.transactions (transaction_id, account_token, counterparty_token, amount_cents, device_hash, occurred_at, payer_name, payer_cpf)
select
    'txn_' || lpad(n::text, 10, '0'),
    'acc_tok_' || lpad(((n % 10000) + 1)::text, 8, '0'),
    'acc_tok_' || lpad((((n * 7 + 3) % 10000) + 1)::text, 8, '0'),
    1000 + (n * 7919) % 500000,
    'dev_' || substr(md5('acc_tok_' || lpad(((n % 10000) + 1)::text, 8, '0') || ':' || (1 + n % 4)), 1, 16),
    timestamptz '$ANCHOR' - ((n * 104729) % (120 * 86400)) * interval '1 second',
    'Pagador Sintetico ' || n,
    lpad((n % 1000)::text, 3, '0') || '.' || lpad((n % 997)::text, 3, '0') || '.' || lpad((n % 991)::text, 3, '0') || '-' || lpad((n % 100)::text, 2, '0')
from generate_series(1::bigint, $N) as n
on conflict (transaction_id) do nothing;
SQL

$PSQL -c "select count(*) || ' linhas; de ' || min(occurred_at) || ' a ' || max(occurred_at) from bank.transactions"
