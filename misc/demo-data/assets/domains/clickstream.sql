-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- Clickstream: sessions → page_views → conversions
--
-- Demonstrates:
--   * funnel analytics as a maintained view
--   * window-like behavior via per-session offsets within the moment
--   * cross-domain join to `people` for user identity
--
-- Prerequisites: scaffold.sql, common/people.sql
-- =============================================================================

-- One session per moment.
-- Byte budget:
--   [0..2] session_id
--   [3]    user_id (mod 256 ⇒ people pool)
--   [4]    n_views (clamped 3..15)
--   [5]    source channel index
--   [6]    device-type index
CREATE VIEW sessions_core AS
SELECT
    moment,
    random,
    get_byte(random, 0) +
    get_byte(random, 1) * 256 +
    get_byte(random, 2) * 65536                                  AS id,
    get_byte(random, 3)                                          AS user_id,
    3 + mod(get_byte(random, 4)::int, 13)                        AS n_views,
    (ARRAY['organic','paid','referral','direct','email'])
        [1 + mod(get_byte(random, 5)::int, 5)]                   AS source,
    (ARRAY['desktop','mobile','tablet'])
        [1 + mod(get_byte(random, 6)::int, 3)]                   AS device
FROM random;

CREATE MATERIALIZED VIEW sessions AS
SELECT
    s.id,
    s.user_id,
    p.name        AS user_name,
    p.region      AS user_region,
    s.moment      AS started_at,
    s.source,
    s.device,
    s.n_views
FROM sessions_core s
JOIN people p ON p.id = s.user_id;

-- Page views: each session emits n_views events. Pages are chosen by hash and
-- skewed so /checkout is rarer than /landing (the funnel narrows).
-- Page distribution: low byte values → /landing, /product, /search (common),
-- high byte values → /cart, /checkout (rare). This produces a realistic funnel.
CREATE MATERIALIZED VIEW page_views AS
WITH expanded AS (
    SELECT
        id AS session_id,
        moment AS session_start,
        generate_series(1, n_views) AS view_no,
        digest(random::text || generate_series(1, n_views)::text, 'md5') AS random
    FROM sessions_core
)
SELECT
    session_id,
    view_no,
    -- Each view offset within the session, in seconds. View N happens at
    -- session_start + (N + small jitter) seconds.
    session_start
        + (view_no::text || ' seconds')::interval
        + (mod(get_byte(random, 1)::int, 30)::text || ' seconds')::interval
                                                                 AS viewed_at,
    -- Skewed page picker: bias towards top-of-funnel.
    CASE
        WHEN get_byte(random, 0) < 80  THEN '/landing'
        WHEN get_byte(random, 0) < 150 THEN '/product'
        WHEN get_byte(random, 0) < 200 THEN '/search'
        WHEN get_byte(random, 0) < 230 THEN '/cart'
        ELSE                                '/checkout'
    END                                                          AS path
FROM expanded;

-- Conversions: sessions that reached /checkout.
CREATE VIEW conversions AS
SELECT DISTINCT
    pv.session_id,
    s.user_id,
    s.user_name,
    s.source,
    s.started_at
FROM page_views pv
JOIN sessions s ON s.id = pv.session_id
WHERE pv.path = '/checkout';

-- Funnel: pageview counts per stage, the classic clickstream dashboard.
CREATE VIEW funnel AS
SELECT path, COUNT(*) AS views
FROM page_views
GROUP BY path;

-- Conversion rate by source channel. A useful "stays correct under load" demo.
CREATE VIEW conversion_by_source AS
SELECT
    s.source,
    COUNT(DISTINCT s.id)                       AS sessions,
    COUNT(DISTINCT c.session_id)               AS conversions,
    COUNT(DISTINCT c.session_id)::float
        / NULLIF(COUNT(DISTINCT s.id), 0)      AS conversion_rate
FROM sessions s
LEFT JOIN conversions c ON c.session_id = s.id
GROUP BY s.source;

-- -----------------------------------------------------------------------------
-- Validation:
--
-- Heartbeat:
--   COPY (SUBSCRIBE (SELECT COUNT(*) FROM sessions) WITH (progress = true)) TO STDOUT;
--
-- Current funnel:
--   SELECT * FROM funnel ORDER BY views DESC;
--
-- Cross-domain demo (requires ecommerce.sql also loaded):
-- Which users converted AND placed an order in the window?
--   SELECT c.user_name FROM conversions c
--   WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.user_id);
-- -----------------------------------------------------------------------------
