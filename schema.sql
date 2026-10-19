CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    customer_id TEXT NOT NULL,
    items JSONB NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL
);
