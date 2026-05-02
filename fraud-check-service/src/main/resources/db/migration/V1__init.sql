-- Fraud Check Service schema

CREATE TABLE fraud_checks (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       VARCHAR(100) NOT NULL,
    customer_id    VARCHAR(100) NOT NULL,
    amount         NUMERIC(12, 2) NOT NULL,
    status         VARCHAR(20) NOT NULL,   -- CLEARED | DECLINED | VOIDED
    decline_reason VARCHAR(255),
    checked_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fraud_checks_order_id ON fraud_checks (order_id);

-- Idempotency: prevents double-processing of redelivered Kafka messages
CREATE TABLE idempotency_records (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  VARCHAR(255) NOT NULL UNIQUE,
    processed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
