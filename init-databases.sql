-- Create separate databases for each saga participant
-- Each service owns its data completely — no shared tables

CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE fraud_db;
CREATE DATABASE payment_db;
