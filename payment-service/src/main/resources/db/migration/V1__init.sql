-- Payment Service schema
-- Owns: payments, idempotency_records

CREATE TABLE payments (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      VARCHAR(30)  NOT NULL DEFAULT 'PENDING',
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE idempotency_records (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  VARCHAR(255) UNIQUE NOT NULL,
    processed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
