-- Initialize a minimal-but-rich example schema: minicom
CREATE SCHEMA IF NOT EXISTS minicom;
SET search_path TO minicom, public;

-- Core entities
CREATE TABLE store (
  id            SERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  settings      JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX store_settings_gin ON store USING gin (settings);

CREATE TABLE product (
  id            SERIAL PRIMARY KEY,
  store_id      INTEGER NOT NULL,
  sku           TEXT UNIQUE NOT NULL,
  price_cents   INTEGER NOT NULL CHECK (price_cents >= 0),
  deleted_at    TIMESTAMP NULL
);

ALTER TABLE product
  ADD CONSTRAINT product_store_fk
  FOREIGN KEY (store_id) REFERENCES store(id) ON DELETE CASCADE;

CREATE TABLE customer (
  id            SERIAL PRIMARY KEY,
  email         TEXT NOT NULL,
  password      TEXT NOT NULL,
  created_at    TIMESTAMP NOT NULL DEFAULT now()
);

-- Functional unique index to simulate common email lookup patterns
CREATE UNIQUE INDEX customer_email_lower_uq ON customer (lower(email));

CREATE TABLE "order" (
  id            SERIAL PRIMARY KEY,
  store_id      INTEGER NOT NULL,
  customer_id   INTEGER NULL,        -- intentionally no FK to simulate imperfect integrity
  placed_at     TIMESTAMP NOT NULL DEFAULT now(),
  is_test       BOOLEAN NOT NULL DEFAULT FALSE
);

ALTER TABLE "order"
  ADD CONSTRAINT order_store_fk
  FOREIGN KEY (store_id) REFERENCES store(id) ON DELETE CASCADE;

-- Partial index: common filter for production-like queries
CREATE INDEX order_placed_non_test_idx ON "order" (placed_at) WHERE is_test = FALSE;

CREATE TABLE order_item (
  id            SERIAL PRIMARY KEY,
  order_id      INTEGER NOT NULL,
  product_id    INTEGER NOT NULL,
  qty           INTEGER NOT NULL CHECK (qty > 0),
  price_cents   INTEGER NOT NULL CHECK (price_cents >= 0)
);

ALTER TABLE order_item
  ADD CONSTRAINT order_item_order_fk FOREIGN KEY (order_id) REFERENCES "order"(id) ON DELETE CASCADE,
  ADD CONSTRAINT order_item_product_fk FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE RESTRICT;

CREATE TABLE coupon (
  id            SERIAL PRIMARY KEY,
  store_id      INTEGER NOT NULL,
  code          TEXT UNIQUE NOT NULL,
  kind          TEXT NOT NULL CHECK (kind IN ('percent','fixed')),
  active        BOOLEAN NOT NULL DEFAULT TRUE
);

ALTER TABLE coupon
  ADD CONSTRAINT coupon_store_fk FOREIGN KEY (store_id) REFERENCES store(id) ON DELETE CASCADE;

CREATE TABLE coupon_redemption (
  id            SERIAL PRIMARY KEY,
  coupon_id     INTEGER NOT NULL,
  order_id      INTEGER NULL   -- intentionally no FK to "order"
);

ALTER TABLE coupon_redemption
  ADD CONSTRAINT coupon_redemption_coupon_fk FOREIGN KEY (coupon_id) REFERENCES coupon(id) ON DELETE CASCADE;

CREATE TABLE shipment (
  id            SERIAL PRIMARY KEY,
  order_id      INTEGER NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('pending','shipped','delivered')),
  shipped_at    TIMESTAMP NULL
);

ALTER TABLE shipment
  ADD CONSTRAINT shipment_order_fk FOREIGN KEY (order_id) REFERENCES "order"(id) ON DELETE CASCADE;

