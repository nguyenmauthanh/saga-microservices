-- Add discount and shipping fee columns to orders (backward-compatible)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount_amount NUMERIC(10,2) DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS shipping_fee    NUMERIC(10,2) DEFAULT 0;
