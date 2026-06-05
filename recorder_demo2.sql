-- Recorder prototype demo 2: arrival-time stamping for CDC data.
--
-- Customer story: data is ingested from an external Postgres system, and the
-- time it entered Materialize must be available *as data*. Today that is
-- emulated by looping the data through Kafka to attach a timestamp and
-- reingesting it. A recorder stamps it on ingestion instead.
--
-- Setup outside this script:
--   docker run --name demo-pg -d -p 25432:5432 -e POSTGRES_PASSWORD=postgres \
--     postgres:16 -c wal_level=logical
--   In Postgres: CREATE TABLE orders (id int PRIMARY KEY, item text, qty int);
--     ALTER TABLE orders REPLICA IDENTITY FULL;
--     CREATE PUBLICATION mz_source FOR TABLE orders;
--   In Materialize (flags as in recorder_demo.sql), plus:
--     CREATE SECRET pgpass AS 'postgres';
--     CREATE CONNECTION pg_conn TO POSTGRES (HOST '127.0.0.1', PORT 25432,
--       USER postgres, PASSWORD SECRET pgpass, DATABASE postgres,
--       SSL MODE disable);
--
-- Note: requires the prototype's relaxed read-then-write dependency check
-- (sources readable in INSERT ... SELECT); see coord/read_then_write.rs.

-- 1. Ingest via a regular Postgres CDC source.
CREATE SOURCE pg_src FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source');
CREATE TABLE orders FROM SOURCE pg_src (REFERENCE orders)
  WITH (RETAIN HISTORY = FOR '1 hour');

-- 2. The stamped changelog and the recorder. `now()` is frozen at processing
--    time: the arrival timestamp, recorded once, never recomputed.
CREATE DELTA TABLE orders_log (id int, item text, qty int, arrived_at timestamptz)
  WITH (RETAIN HISTORY = FOR '1 hour');
CREATE RECORDER order_arrivals AS RECORD (  -- as mz_system
  SELECT c.id, c.item, c.qty, now() AS arrived_at, c.mz_timestamp, c.mz_diff
  FROM CHANGES(orders AS OF AT LEAST 0) c
) INTO orders_log;

-- 3. Insert / update rows in Postgres; each change arrives stamped:
--    inserts as +1 deltas, updates as retract/insert pairs, all with
--    arrived_at = when the change entered Materialize.
SELECT * FROM orders_log ORDER BY mz_timestamp;

-- 4. Present it as data: current orders, each with its arrival time.
CREATE VIEW orders_with_arrival AS
  SELECT DISTINCT ON (id) id, item, qty, arrived_at
  FROM (SELECT id FROM orders_log GROUP BY id HAVING SUM(mz_diff) > 0) live
  JOIN orders_log USING (id)
  WHERE mz_diff > 0
  ORDER BY id, mz_timestamp DESC;
SELECT * FROM orders_with_arrival ORDER BY id;
