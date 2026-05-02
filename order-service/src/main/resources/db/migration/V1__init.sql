-- Order Service schema
-- Owns: orders, saga_instances, idempotency_records

CREATE TABLE orders (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(100) NOT NULL,
    status      VARCHAR(30)  NOT NULL DEFAULT 'PENDING',
    total_amount NUMERIC(10,2) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE order_items (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id   UUID NOT NULL REFERENCES orders(id),
    product_id VARCHAR(100) NOT NULL,
    quantity   INT  NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL
);

-- Orchestration saga state (persisted — orchestrator is stateless)
CREATE TABLE saga_instances (
    id         UUID PRIMARY KEY,
    order_id   VARCHAR(100) NOT NULL,
    state      VARCHAR(50)  NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Idempotency for deduplication on at-least-once Kafka delivery
CREATE TABLE idempotency_records (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  VARCHAR(255) UNIQUE NOT NULL,
    processed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
