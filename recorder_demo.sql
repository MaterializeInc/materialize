-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Recorder prototype demo (design doc 20260604_recorders.md, PR #36909).
--
-- Run against bin/environmentd:
--   as mz_system (psql -h 127.0.0.1 -p 6877 -U mz_system materialize):
--     the ALTER SYSTEM statements and CREATE/DROP RECORDER;
--   the rest as materialize (psql -h 127.0.0.1 -p 6875 -U materialize materialize).
--
-- Note: all collections read by a recorder body need RETAIN HISTORY for now;
-- the AT LEAST clamp uses the dataflow-wide since, so a 1s-compacting input
-- (default) drags the changelog start forward and re-emits the snapshot.

-- 1. Setup (mz_system).
ALTER SYSTEM SET enable_changes_table_function = true;
ALTER SYSTEM SET enable_logical_compaction_window = true;

-- 2. Inputs: an event stream and a dimension table.
CREATE TABLE events (id int, fk text) WITH (RETAIN HISTORY = FOR '1 hour');
CREATE TABLE dim (key text, val text) WITH (RETAIN HISTORY = FOR '1 hour');
INSERT INTO dim VALUES ('a', 'EUR 0.92');

-- 3. A DELTA TABLE: implicit mz_timestamp / mz_diff columns.
CREATE DELTA TABLE enriched (id int, fk text, val text)
  WITH (RETAIN HISTORY = FOR '1 hour');
SHOW COLUMNS FROM enriched;

-- 4. The recorder (mz_system): records enriched event deltas. The bare `dim`
--    reference is a frozen lookup (freeze-by-typing): evaluated at processing
--    time, recorded once, never recomputed.
CREATE RECORDER rec AS RECORD (
  SELECT e.id, e.fk, d.val, e.mz_timestamp, e.mz_diff
  FROM CHANGES(events AS OF AT LEAST 0) e
  JOIN dim d ON e.fk = d.key
) INTO enriched;

-- 5. An event arrives and is recorded with the dim value of *now*.
INSERT INTO events VALUES (1, 'a');
SELECT * FROM enriched;          -- 1 | a | EUR 0.92 | <ts> | 1

-- 6. The dimension changes; the recorded row does NOT (frozen).
UPDATE dim SET val = 'EUR 0.95' WHERE key = 'a';
SELECT * FROM enriched;          -- unchanged

-- 7. A new event freezes the new value.
INSERT INTO events VALUES (2, 'a');
SELECT * FROM enriched;          -- second row with EUR 0.95

-- 8. Deletes are recorded as retraction deltas (mz_diff = -1).
DELETE FROM events WHERE id = 1;
SELECT * FROM enriched ORDER BY mz_timestamp;

-- 9. "INTEGRATE" by hand: the current state is the integral of the deltas.
--    (Liveness from SUM(mz_diff) per key; value from the latest insert. The
--    frozen value makes whole-row retraction non-exact when the dim changed
--    in between -- the design doc's documented consolidation hazard.)
CREATE VIEW current_enriched AS
  SELECT DISTINCT ON (id, fk) id, fk, val
  FROM (SELECT id, fk FROM enriched GROUP BY id, fk HAVING SUM(mz_diff) > 0) live
  JOIN enriched USING (id, fk)
  WHERE mz_diff > 0
  ORDER BY id, fk, mz_timestamp DESC;
SELECT * FROM current_enriched;  -- only event 2, with its frozen EUR 0.95

-- 10. Cleanup (mz_system).
-- DROP RECORDER rec;
