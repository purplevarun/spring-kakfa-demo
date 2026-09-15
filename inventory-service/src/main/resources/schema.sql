CREATE TABLE IF NOT EXISTS processed_orders (
    id UUID PRIMARY KEY,
    order_id INTEGER NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    item_name TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (order_id, event_type)
);

CREATE TABLE IF NOT EXISTS event_deliveries (
    id UUID PRIMARY KEY,
    processed_order_id UUID NOT NULL REFERENCES processed_orders(id),
    order_id INTEGER NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    item_name TEXT NOT NULL,
    topic VARCHAR(200) NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    outcome VARCHAR(20) NOT NULL CHECK (outcome IN ('PROCESSED', 'DUPLICATE', 'CONFLICT')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (topic, partition_id, kafka_offset)
);

CREATE INDEX IF NOT EXISTS event_deliveries_received_at_idx ON event_deliveries (received_at DESC);