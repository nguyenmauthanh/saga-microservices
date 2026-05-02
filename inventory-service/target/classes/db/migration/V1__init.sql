-- Inventory Service schema
-- Owns: inventory_items, inventory_reservations, idempotency_records

CREATE TABLE inventory_items (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      VARCHAR(100) UNIQUE NOT NULL,
    name            VARCHAR(255) NOT NULL,
    price           NUMERIC(10,2) NOT NULL,
    available_stock INT NOT NULL DEFAULT 0
);

CREATE TABLE inventory_reservations (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id   VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    quantity   INT NOT NULL,
    status     VARCHAR(30)  NOT NULL DEFAULT 'RESERVED',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE idempotency_records (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  VARCHAR(255) UNIQUE NOT NULL,
    processed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
