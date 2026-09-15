CREATE TABLE IF NOT EXISTS processed_orders (
    id UUID PRIMARY KEY,
    order_id INTEGER NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    item_name TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (order_id, event_type)
);