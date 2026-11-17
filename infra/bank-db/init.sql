
CREATE SCHEMA bank;

CREATE TABLE bank.customers (
    account_token             text PRIMARY KEY,
    customer_token            text        NOT NULL,
    full_name                 text        NOT NULL,
    cpf                       text        NOT NULL,
    email                     text        NOT NULL,
    phone                     text        NOT NULL,
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
    payer_name         text,
    payer_cpf          text
);
CREATE INDEX transactions_occurred_at_idx ON bank.transactions (occurred_at);

INSERT INTO bank.customers
SELECT
    'acc_tok_' || lpad(n::text, 8, '0'),
    'cus_tok_' || lpad(n::text, 8, '0'),
    'Cliente Sintetico ' || n,
    substr(lpad((n * 7919 % 1000000000)::text, 9, '0'), 1, 3) || '.' ||
    substr(lpad((n * 7919 % 1000000000)::text, 9, '0'), 4, 3) || '.' ||
    substr(lpad((n * 7919 % 1000000000)::text, 9, '0'), 7, 3) || '-' || lpad((n % 100)::text, 2, '0'),
    'cliente' || n || '@exemplo.invalid',
    '+5511' || lpad((900000000 + n)::text, 9, '0'),
    CASE WHEN n % 10 = 0 THEN 'PJ' ELSE 'PF' END,
    now() - ((n * 37 % 1800) + 30) * interval '1 day',
    CASE WHEN n % 3 = 0 THEN now() - ((n * 11 % 90) + 1) * interval '1 day' END
FROM generate_series(1, 10000) AS n;

INSERT INTO bank.customer_devices
SELECT
    c.account_token,
    'dev_' || substr(md5(c.account_token || ':' || d), 1, 16),
    c.account_opened_at + (d * interval '20 day')
FROM bank.customers c
CROSS JOIN generate_series(1, 4) AS d
WHERE d <= 1 + (right(c.account_token, 2)::int % 4);

CREATE PUBLICATION dbz_customers    FOR TABLE bank.customers, bank.customer_devices;
CREATE PUBLICATION dbz_transactions FOR TABLE bank.transactions;
