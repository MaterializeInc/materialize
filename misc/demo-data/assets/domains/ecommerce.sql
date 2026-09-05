-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- E-commerce: orders → line_items → order_totals
--
-- Demonstrates:
--   * multi-row child generation via generate_series fanout
--   * cross-domain join to shared `people` (customer FK)
--   * aggregate-as-view (`order_totals`) — the classic MV use case
--
-- Prerequisites: scaffold.sql, common/people.sql
-- =============================================================================

-- Static product catalog. Sixteen products keeps line_items tractable.
CREATE VIEW products (id, name, category) AS VALUES
    ( 0, 'Coffee Beans 1lb',     'Grocery'),
    ( 1, 'Olive Oil 500ml',      'Grocery'),
    ( 2, 'Dish Soap',            'Household'),
    ( 3, 'Paper Towels 6pk',     'Household'),
    ( 4, 'Notebook A5',          'Stationery'),
    ( 5, 'Ballpoint Pen 12pk',   'Stationery'),
    ( 6, 'USB-C Cable 1m',       'Electronics'),
    ( 7, 'Wireless Mouse',       'Electronics'),
    ( 8, 'T-Shirt Plain',        'Apparel'),
    ( 9, 'Wool Socks',           'Apparel'),
    (10, 'Yoga Mat',             'Fitness'),
    (11, 'Resistance Band Set',  'Fitness'),
    (12, 'Cast Iron Skillet',    'Kitchen'),
    (13, 'Chef Knife 8in',       'Kitchen'),
    (14, 'Houseplant Pothos',    'Home'),
    (15, 'Candle Lavender',      'Home');

-- One order per moment.
-- Byte budget for `random`:
--   [0..2] order id (24-bit space)
--   [3]    customer_id (mod 256 ⇒ aligns with people pool)
--   [6]    n_items, clamped to 1..8
CREATE VIEW orders_core AS
SELECT
    moment,
    random,
    get_byte(random, 0) +
    get_byte(random, 1) * 256 +
    get_byte(random, 2) * 65536                AS id,
    get_byte(random, 3)                        AS customer_id,
    1 + mod(get_byte(random, 6)::int, 8)       AS n_items
FROM random;

CREATE MATERIALIZED VIEW orders AS
SELECT
    o.id,
    o.customer_id,
    p.name        AS customer_name,
    p.region      AS customer_region,
    o.moment      AS placed_at,
    o.n_items
FROM orders_core o
JOIN people p ON p.id = o.customer_id;

-- Line items: each order spawns n_items via generate_series, each line
-- re-hashes (order.random || line_no) for its own bytes.
-- Line-item byte budget:
--   [0..1] product_id (mod 16)
--   [2]    qty (1..8)
--   [3..5] unit_price_cents (24-bit ⇒ up to ~$167k; realistic skew via mask)
CREATE MATERIALIZED VIEW line_items AS
WITH expanded AS (
    SELECT
        id AS order_id,
        generate_series(1, n_items) AS line_no,
        digest(random::text || generate_series(1, n_items)::text, 'md5') AS random
    FROM orders_core
)
SELECT
    order_id,
    line_no,
    mod(get_byte(random, 0) + get_byte(random, 1) * 256, 16)             AS product_id,
    1 + mod(get_byte(random, 2)::int, 8)                                 AS qty,
    -- Prices skewed small: 24-bit cents, but we keep low byte dominant
    -- so most items are cheap and a few are pricey.
    (get_byte(random, 3) +
     get_byte(random, 4) * 256 +
     get_byte(random, 5) * 4)::numeric / 100.0                           AS unit_price
FROM expanded;

-- Aggregate-as-view. THIS is the demo: the running sum stays consistent with
-- line_items in real time, no batch job, no consistency window.
CREATE VIEW order_totals AS
SELECT
    li.order_id,
    SUM(li.qty * li.unit_price) AS total
FROM line_items li
GROUP BY li.order_id;

-- Customer running spend across the retention window. Shows people-side joins.
CREATE VIEW customer_spend AS
SELECT
    o.customer_id,
    o.customer_name,
    COUNT(DISTINCT o.id)              AS orders_placed,
    SUM(li.qty * li.unit_price)       AS total_spent
FROM orders o
JOIN line_items li ON li.order_id = o.id
GROUP BY o.customer_id, o.customer_name;

-- -----------------------------------------------------------------------------
-- Validation:
--
-- Heartbeat:
--   COPY (SUBSCRIBE (SELECT COUNT(*) FROM orders) WITH (progress = true)) TO STDOUT;
--
-- Invariant: every line_item references an existing order (should be 0).
--   SELECT COUNT(*) FROM line_items li
--   LEFT JOIN orders o ON o.id = li.order_id WHERE o.id IS NULL;
--
-- Invariant: total line-items = total declared fanout, in aggregate.
-- (A per-order version of this fails because the 24-bit `id` space has
-- birthday-paradox collisions at 86k+ rows — see SKILL.md "Invariants vs.
-- id collisions". The aggregate form is unaffected since both sides count
-- rows regardless of id.)
--   SELECT (SELECT COUNT(*)    FROM line_items) =
--          (SELECT SUM(n_items) FROM orders_core) AS fanout_balances;
-- -----------------------------------------------------------------------------
