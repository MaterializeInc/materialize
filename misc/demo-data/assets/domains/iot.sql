-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- IoT: devices + readings + alerts
--
-- Demonstrates:
--   * high-cardinality fan-out (multiple readings per moment)
--   * per-device aggregates over a sliding window (running avg, max)
--   * threshold-based alerting as a continuously-maintained view
--
-- Standalone domain: no `people` join. Devices are their own identity space.
--
-- Prerequisites: scaffold.sql
-- =============================================================================

-- Static device fleet of 128 devices. Each has a sensor type and a per-device
-- alert threshold. Threshold derived from the device id so it's stable.
CREATE VIEW devices AS
SELECT
    id::int                                                       AS id,
    'dev-' || lpad(id::text, 4, '0')                              AS device_name,
    (ARRAY['temperature','pressure','humidity','vibration'])
        [1 + mod(id, 4)]                                          AS sensor_type,
    -- Site picked deterministically from the id; 16 sites of ~8 devices each.
    'site-' || lpad((mod(id, 16))::text, 2, '0')                  AS site,
    -- Threshold: most devices ~200, a few aggressive ones ~150.
    150 + mod(id * 7, 100)                                        AS threshold
FROM generate_series(0, 127) AS id;

CREATE DEFAULT INDEX ON devices;

-- Readings. Each moment emits 8 readings, each one re-hashing to pick a
-- device and a value. Total cardinality ≈ retention_seconds × 8.
-- Reading byte budget (per re-hash):
--   [0]    device_id (mod 128)
--   [1..2] reading value (16-bit, scaled into roughly the threshold range)
CREATE MATERIALIZED VIEW readings AS
WITH expanded AS (
    SELECT
        moment,
        digest(random::text || generate_series(1, 8)::text, 'md5') AS random,
        generate_series(1, 8) AS slot
    FROM random
)
SELECT
    moment                                                AS observed_at,
    slot,
    mod(get_byte(random, 0)::int, 128)                    AS device_id,
    (get_byte(random, 1) + get_byte(random, 2) * 256)
        ::numeric / 100.0                                 AS value
FROM expanded;

-- Per-device running aggregates over the live retention window. This is the
-- IoT demo: a streaming dashboard that stays current.
CREATE VIEW device_stats AS
SELECT
    d.id                                                  AS device_id,
    d.device_name,
    d.sensor_type,
    d.site,
    COUNT(*)                                              AS readings_in_window,
    AVG(r.value)                                          AS avg_value,
    MAX(r.value)                                          AS max_value,
    MIN(r.value)                                          AS min_value
FROM devices d
JOIN readings r ON r.device_id = d.id
GROUP BY d.id, d.device_name, d.sensor_type, d.site;

-- Alerts: readings exceeding their device's threshold. A continuously
-- maintained, joinable, streaming alert table.
CREATE MATERIALIZED VIEW alerts AS
SELECT
    r.observed_at,
    r.device_id,
    d.device_name,
    d.sensor_type,
    d.site,
    r.value,
    d.threshold,
    r.value - d.threshold                                 AS overshoot
FROM readings r
JOIN devices d ON d.id = r.device_id
WHERE r.value > d.threshold;

-- Per-site alert load. Useful for "which site is hot right now" dashboards.
CREATE VIEW site_alert_load AS
SELECT
    site,
    COUNT(*)             AS active_alerts,
    MAX(overshoot)       AS worst_overshoot
FROM alerts
GROUP BY site;

-- -----------------------------------------------------------------------------
-- Validation:
--
-- Heartbeat:
--   COPY (SUBSCRIBE (SELECT COUNT(*) FROM readings) WITH (progress = true)) TO STDOUT;
--
-- Top noisy sites right now:
--   SELECT * FROM site_alert_load ORDER BY active_alerts DESC LIMIT 5;
--
-- Invariant: every alert is reachable from a reading (FK integrity). 0 expected:
--   SELECT COUNT(*) FROM alerts a
--   LEFT JOIN readings r ON r.observed_at = a.observed_at AND r.device_id = a.device_id
--   WHERE r.device_id IS NULL;
-- -----------------------------------------------------------------------------
