-- Ledger schema. All money is in minor units (paise), never floating point.

DROP TABLE IF EXISTS outbox, entries, transfers, idempotency_keys, accounts CASCADE;

CREATE TABLE accounts (
    id       TEXT PRIMARY KEY,
    balance  BIGINT NOT NULL CHECK (balance >= 0)
);

CREATE TABLE transfers (
    id           BIGSERIAL PRIMARY KEY,
    src          TEXT NOT NULL REFERENCES accounts(id),
    dst          TEXT NOT NULL REFERENCES accounts(id),
    amount       BIGINT NOT NULL CHECK (amount > 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Double-entry: every transfer writes exactly two rows summing to zero.
CREATE TABLE entries (
    id           BIGSERIAL PRIMARY KEY,
    transfer_id  BIGINT NOT NULL REFERENCES transfers(id),
    account_id   TEXT NOT NULL REFERENCES accounts(id),
    delta        BIGINT NOT NULL
);
CREATE INDEX entries_transfer_idx ON entries(transfer_id);

-- The idempotency barrier. The unique constraint is what makes retries safe:
-- two concurrent requests carrying the same key cannot both insert.
CREATE TABLE idempotency_keys (
    key           TEXT NOT NULL,
    endpoint      TEXT NOT NULL,
    request_hash  TEXT NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('in_flight', 'done')),
    response      JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (key, endpoint)
);

-- Transactional outbox: written in the same transaction as the ledger rows,
-- so the ledger and the event stream cannot diverge.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    transfer_id   BIGINT NOT NULL,
    payload       JSONB NOT NULL,
    published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox(id) WHERE published_at IS NULL;
