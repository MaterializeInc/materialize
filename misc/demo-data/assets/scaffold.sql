-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- Moments scaffold: a sliding window of timestamps you can hash for entropy.
--
-- This file is stable boilerplate. Domains build on top of `moments` and
-- `random`; they should never need to edit anything here.
--
-- The trick: build a 130-year `generate_series` of years, but filter so only
-- the years near `mz_now()` survive. Cascade through days/hours/minutes/seconds.
-- The `UNION ALL SELECT * FROM empty` at each level blocks the optimizer from
-- inlining and pre-computing the whole timeline. Each level keeps its rows for
-- `retention` past their end, so the window below is never starved.
--
-- Load with:   \i scaffold.sql
-- =============================================================================

-- Everything this skill creates lives in the `materialize_demo` schema, so it
-- cannot collide with your own objects and teardown.sql is one DROP SCHEMA.
-- Every CREATE is IF NOT EXISTS, so re-running any file is safe; it also
-- means the knobs below only take effect on a fresh load.
CREATE SCHEMA IF NOT EXISTS materialize_demo;
SET search_path = materialize_demo;

-- Knobs. Override before \i, e.g.:
--   \set retention '6 hours'
--   \i scaffold.sql
\if :{?retention} \else \set retention '1 day' \endif
\if :{?tick}      \else \set tick      '1 second' \endif

\echo Scaffold: retention=:retention, tick=:tick

CREATE TABLE IF NOT EXISTS empty (e TIMESTAMP);

-- Each year-long interval of interest.
CREATE VIEW IF NOT EXISTS years AS
SELECT *
FROM generate_series(
    '1970-01-01 00:00:00+00',
    '2099-01-01 00:00:00+00',
    '1 year') year
WHERE mz_now() BETWEEN year AND year + '1 year' + :'retention'::interval;

-- Each day-long interval of interest.
CREATE VIEW IF NOT EXISTS days AS
SELECT * FROM (
    SELECT generate_series(year, year + '1 year' - '1 day'::interval, '1 day') AS day
    FROM years
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN day AND day + '1 day' + :'retention'::interval;

-- Each hour-long interval of interest.
CREATE VIEW IF NOT EXISTS hours AS
SELECT * FROM (
    SELECT generate_series(day, day + '1 day' - '1 hour'::interval, '1 hour') AS hour
    FROM days
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN hour AND hour + '1 hour' + :'retention'::interval;

-- Each minute-long interval of interest.
CREATE VIEW IF NOT EXISTS minutes AS
SELECT * FROM (
    SELECT generate_series(hour, hour + '1 hour' - '1 minute'::interval, '1 minute') AS minute
    FROM hours
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN minute AND minute + '1 minute' + :'retention'::interval;

-- Each second-long interval of interest.
CREATE VIEW IF NOT EXISTS seconds AS
SELECT * FROM (
    SELECT generate_series(minute, minute + '1 minute' - '1 second'::interval, '1 second') AS second
    FROM minutes
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN second AND second + '1 second' + :'retention'::interval;

-- Indexes in order. Each level depends on the prior being indexed so the
-- expansion fires incrementally rather than re-scanning the cascade.
CREATE DEFAULT INDEX IF NOT EXISTS ON years;
CREATE DEFAULT INDEX IF NOT EXISTS ON days;
CREATE DEFAULT INDEX IF NOT EXISTS ON hours;
CREATE DEFAULT INDEX IF NOT EXISTS ON minutes;
CREATE DEFAULT INDEX IF NOT EXISTS ON seconds;

-- Public surface #1: a sliding-window stream of timestamps.
-- Cardinality = retention / tick. Defaults give 86,400 rows (24h of seconds).
-- The `mod(...)` clause thins to the requested tick rate. Tick must be a
-- whole number of seconds.
CREATE VIEW IF NOT EXISTS moments AS
SELECT second AS moment FROM seconds
WHERE mz_now() >= second
  AND mz_now() <  second + :'retention'::interval
  AND mod(EXTRACT(EPOCH FROM second)::bigint,
          EXTRACT(EPOCH FROM :'tick'::interval)::bigint) = 0;

-- Public surface #2: deterministic pseudorandom bytes per moment.
-- Use `get_byte(random, N)` in domains to pull 0..255 values for fields,
-- foreign keys, distributions. Re-hashing `random || something` lets a row
-- spawn many child rows that stay stable across re-derivation.
CREATE VIEW IF NOT EXISTS random AS
SELECT moment, digest(moment::text, 'md5') AS random
FROM moments;
