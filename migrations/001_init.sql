
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE warehouses(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(20) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE products(
    sku VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    unit_weight_grams INT NOT NULL CHECK(unit_weight_grams > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE inventory_items(
    id BIGSERIAL PRIMARY KEY,
    warehouse_id UUID NOT NULL REFERENCES warehouses(id) ON DELETE RESTRICT,
    sku VARCHAR(64) NOT NULL REFERENCES products(sku) ON DELETE RESTRICT,
    available_qty INT NOT NULL DEFAULT 0,
    reserved_qty INT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_available_non_negative CHECK(available_qty >= 0),
    CONSTRAINT chk_reserved_non_negative CHECK(reserved_qty >= 0),
    CONSTRAINT uq_warehouse_sku UNIQUE (warehouse_id , sku)
);

CREATE INDEX idx_inventory_sku_avail ON inventory_items(sku , available_qty) WHERE available_qty > 0;

CREATE TYPE order_status AS ENUM( 'RESERVED', 'CONFIRMED', 'FULFILLED', 'CANCELLED', 'EXPIRED');

CREATE TABLE orders(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    customer_id UUID NOT NULL,
    status order_status NOT NULL DEFAULT 'RESERVED',
    dest_latitude DOUBLE PRECISION NOT NULL,
    dest_longitude DOUBLE PRECISION NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_expired ON orders(expires_at) WHERE status = 'RESERVED';

CREATE TABLE order_items(
    id BIGSERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    warehouse_id UUID NOT NULL REFERENCES warehouses(id),
    sku VARCHAR(64) NOT NULL REFERENCES products(sku),
    quantity INT NOT NULL CHECK(quantity > 0)
);

CREATE INDEX idx_order_items_order ON order_items(order_id);

CREATE TABLE idempotency_key(
    key VARCHAR(128) PRIMARY KEY,
    request_hash CHAR(64) NOT NULL,
    state VARCHAR(12) NOT NULL DEFAULT 'IN_PROGRESS',
    response_status INT,
    response_body JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE inventory_audit_log(
    id BIGSERIAL PRIMARY KEY,
    warehouse_id UUID NOT NULL REFERENCES warehouses(id),
    sku VARCHAR(64) NOT NULL REFERENCES products(sku),
    order_id UUID REFERENCES orders(id),
    event_type VARCHAR(20) NOT NULL,
    delta_available INT NOT NULL,
    delta_reserved INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_wh_sku ON inventory_audit_log(warehouse_id , sku);