-- Temporal-bucketing demo.
--
-- Builds a millisecond-granularity stream of "moments" via the auction-style
-- generate_series hierarchy, joins it against a one-row table, and indexes the
-- result. The temporal filter retains each milli for `retention` (defaults to
-- 22 days, the replica-expiration horizon), so the index always carries one
-- pending retraction per outstanding milli.
--
-- With `enable_compute_temporal_bucketing = false` (default) those pending
-- retractions accumulate in the index's merge batcher. With it set to true,
-- they accumulate in the temporal bucketing operator instead. Compare
-- `mz_internal.mz_dataflow_arrangement_sizes` / arrangement records between
-- the two settings to see the move.
--
-- Tunables:
--   retention   how long each milli lives (drives the steady-state index size)
--   horizon     buffer that gates how far ahead the hierarchy expands
--
-- At ms granularity, retention * 1000 = outstanding future updates. Keep
-- retention small at first (e.g. '10 seconds') to sanity-check, then dial up.

DROP SCHEMA IF EXISTS temporal_demo CASCADE;
CREATE SCHEMA temporal_demo;
SET search_path = temporal_demo;

-- Empty sentinel so each level can reference its parent's column name.
CREATE TABLE empty (e TIMESTAMP);

-- One-row table to join against.
CREATE TABLE one_row (x int NOT NULL);
INSERT INTO one_row VALUES (1);

-- Year-long intervals overlapping [now, now + 1 year + horizon].
CREATE VIEW years AS
SELECT * FROM generate_series(
    '1970-01-01 00:00:00+00'::timestamp,
    '2099-01-01 00:00:00+00'::timestamp,
    '1 year') AS year
WHERE mz_now() BETWEEN year AND year + '1 year'::interval + '22 days'::interval;

-- Day-long intervals.
CREATE VIEW days AS
SELECT * FROM (
    SELECT generate_series(year, year + '1 year'::interval - '1 day'::interval, '1 day') AS day
    FROM years
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN day AND day + '1 day'::interval + '22 days'::interval;

-- Hour-long intervals.
CREATE VIEW hours AS
SELECT * FROM (
    SELECT generate_series(day, day + '1 day'::interval - '1 hour'::interval, '1 hour') AS hour
    FROM days
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN hour AND hour + '1 hour'::interval + '22 days'::interval;

-- Minute-long intervals.
CREATE VIEW minutes AS
SELECT * FROM (
    SELECT generate_series(hour, hour + '1 hour'::interval - '1 minute'::interval, '1 minute') AS minute
    FROM hours
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN minute AND minute + '1 minute'::interval + '22 days'::interval;

-- Second-long intervals.
CREATE VIEW seconds AS
SELECT * FROM (
    SELECT generate_series(minute, minute + '1 minute'::interval - '1 second'::interval, '1 second') AS second
    FROM minutes
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN second AND second + '1 second'::interval + '22 days'::interval;

-- Millisecond ticks: the leaf of the hierarchy.
CREATE VIEW millis AS
SELECT * FROM (
    SELECT generate_series(
        second,
        second + '1 second'::interval - '1 millisecond'::interval,
        '1 millisecond') AS milli
    FROM seconds
    UNION ALL SELECT * FROM empty
)
WHERE mz_now() BETWEEN milli AND milli + '1 millisecond'::interval + '22 days'::interval;

-- Indexes ensure each level only expands a thin slice of its parent.
CREATE DEFAULT INDEX ON years;
CREATE DEFAULT INDEX ON days;
CREATE DEFAULT INDEX ON hours;
CREATE DEFAULT INDEX ON minutes;
CREATE DEFAULT INDEX ON seconds;
CREATE DEFAULT INDEX ON millis;

-- The view we actually want to index: ms-granularity moments joined against a
-- one-row table, with a temporal filter setting the retention window. Every
-- live row has a pending future retraction queued in the indexed arrangement.
CREATE VIEW moments AS
SELECT m.milli AS moment, o.x
FROM millis m, one_row o
WHERE mz_now() >= m.milli
  AND mz_now() < m.milli + '22 days'::interval;

CREATE INDEX moments_idx ON moments (moment);

-- To toggle temporal bucketing (system privilege required):
--   ALTER SYSTEM SET enable_compute_temporal_bucketing = true;
--   ALTER SYSTEM SET compute_temporal_bucketing_summary = '2s';
-- Rebuild the index after toggling so the new plan is rendered.
