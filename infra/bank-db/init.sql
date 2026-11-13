-- "Banco legado" do banco: fonte do CDC (Debezium) das specs 003 e 006.
-- Todos os dados são SINTÉTICOS. As colunas full_name, cpf, email, phone e payer_* existem DE PROPÓSITO,
-- para provar que o pipeline não deixa dado pessoal vazar para o Kafka (LGPD, constituição VI).
-- Aqui só há tokens opacos (acc_tok_…, cus_tok_…, dev_…) como identificadores.

CREATE SCHEMA bank;

CREATE TABLE bank.customers (
    account_token             text PRIMARY KEY,
    customer_token            text        NOT NULL,
    full_name                 text        NOT NULL,   -- dado pessoal sintético
    cpf                       text        NOT NULL,   -- dado pessoal sintético
    email                     text        NOT NULL,   -- dado pessoal sintético
    phone                     text        NOT NULL,   -- dado pessoal sintético
    segment                   text        NOT NULL CHECK (segment IN ('PF', 'PJ')),
    account_opened_at         timestamptz NOT NULL,
    last_credential_change_at timestamptz
);

CREATE TABLE bank.customer_devices (
    account_token text        NOT NULL REFERENCES bank.customers (account_token),
    device_hash   text        NOT NULL,
    first_seen_at timestamptz NOT NULL,
    PRIMARY KEY (account_token, device_hash)
);

CREATE TABLE bank.transactions (
    transaction_id     text PRIMARY KEY,
    account_token      text        NOT NULL,
    counterparty_token text        NOT NULL,
    amount_cents       bigint      NOT NULL CHECK (amount_cents > 0),
    device_hash        text,
    occurred_at        timestamptz NOT NULL,
    payer_name         text,                          -- dado pessoal sintético
    payer_cpf          text                           -- dado pessoal sintético
);
CREATE INDEX transactions_occurred_at_idx ON bank.transactions (occurred_at);

-- 10 mil clientes sintéticos. Determinístico: mesma carga a cada criação do volume.
INSERT INTO bank.customers
SELECT
    'acc_tok_' || lpad(n::text, 8, '0'),
    'cus_tok_' || lpad(n::text, 8, '0'),
    'Cliente Sintetico ' || n,
    -- CPF sintético com formato válido de máscara; não pretende ser um CPF real
    substr(lpad((n * 7919 % 1000000000)::text, 9, '0'), 1, 3) || '.' ||
    substr(lpad((n * 7919 % 1000000000)::text, 9, '0'), 4, 3) || '.' ||
    substr(lpad((n * 7919 % 1000000000)::text, 9, '0'), 7, 3) || '-' || lpad((n % 100)::text, 2, '0'),
    'cliente' || n || '@exemplo.invalid',
    '+5511' || lpad((900000000 + n)::text, 9, '0'),
    CASE WHEN n % 10 = 0 THEN 'PJ' ELSE 'PF' END,
    now() - ((n * 37 % 1800) + 30) * interval '1 day',
    CASE WHEN n % 3 = 0 THEN now() - ((n * 11 % 90) + 1) * interval '1 day' END
FROM generate_series(1, 10000) AS n;

-- 1 a 4 dispositivos por conta
INSERT INTO bank.customer_devices
SELECT
    c.account_token,
    'dev_' || substr(md5(c.account_token || ':' || d), 1, 16),
    c.account_opened_at + (d * interval '20 day')
FROM bank.customers c
CROSS JOIN generate_series(1, 4) AS d
WHERE d <= 1 + (right(c.account_token, 2)::int % 4);

-- Uma publicação por conector Debezium (plan 000 §5.1): conectores não compartilham slot nem publicação.
CREATE PUBLICATION dbz_customers    FOR TABLE bank.customers, bank.customer_devices;
CREATE PUBLICATION dbz_transactions FOR TABLE bank.transactions;
