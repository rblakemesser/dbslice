SET search_path TO minicom, public;

-- Stores
INSERT INTO store (name, settings) VALUES
  ('Alpha Store', '{"theme":"light","features":{"coupons":true}}'),
  ('Beta Store',  '{"theme":"dark","features":{"coupons":false}}');

-- Products
INSERT INTO product (store_id, sku, price_cents, deleted_at) VALUES
  (1, 'ALPHA-TSHIRT', 1999, NULL),
  (1, 'ALPHA-MUG',     999, NOW()), -- soft deleted
  (2, 'BETA-STICKER',  299, NULL);

-- Customers
INSERT INTO customer (email, password) VALUES
  ('alice@example.com', 'hash1'),
  ('bob@example.com',   'hash2');

-- Orders (one real, one test, one with NULL customer)
INSERT INTO "order" (store_id, customer_id, placed_at, is_test) VALUES
  (1, 1, NOW() - INTERVAL '2 days', FALSE),
  (1, NULL, NOW() - INTERVAL '1 day', FALSE),
  (2, 2, NOW(), TRUE);

-- Order items
INSERT INTO order_item (order_id, product_id, qty, price_cents) VALUES
  (1, 1, 1, 1999),
  (2, 1, 2, 1999),
  (3, 3, 5, 299);

-- Coupons and redemptions
INSERT INTO coupon (store_id, code, kind, active) VALUES
  (1, 'ALPHA10', 'percent', TRUE),
  (2, 'BETA5',   'fixed',   TRUE);

INSERT INTO coupon_redemption (coupon_id, order_id) VALUES
  (1, 1),
  (1, NULL),     -- orphaned redemption (no order)
  (2, 3);

-- Shipments
INSERT INTO shipment (order_id, status, shipped_at) VALUES
  (1, 'shipped', NOW() - INTERVAL '1 day'),
  (2, 'pending', NULL);

