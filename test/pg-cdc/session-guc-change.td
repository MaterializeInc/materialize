# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Retractions of TEXT COLUMNS values compare the text PostgreSQL rendered for
# the before-image against the text stored at insertion. That rendering follows
# session GUCs that default from the database, so a database-level change picked
# up by a fresh walsender makes the before-image differ from the stored row and
# the retraction fails to cancel it. Materialize pins the rendering GUCs on its
# connections, so each leg below must ingest cleanly.
#
# Each leg changes one GUC at the database level, terminates the walsender so
# the next change is decoded by a session started after the change, then
# updates a row and reads the table back through operators with multiplicity
# checks. While the bug exists a leg fails with "Non-positive multiplicity in
# DistinctBy".

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
ALTER DATABASE postgres RESET timezone;
ALTER DATABASE postgres RESET datestyle;
ALTER DATABASE postgres RESET intervalstyle;
ALTER DATABASE postgres RESET extra_float_digits;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;
CREATE TYPE audit_t AS (ts timestamptz, iv interval, f float8, note text);
CREATE TABLE events (id int, val int, audit audit_t);
ALTER TABLE events REPLICA IDENTITY FULL;
INSERT INTO events VALUES (1, 0, ROW('2026-07-22 15:15:00+00'::timestamptz, '1 day 02:03:04'::interval, 0.1::float8 + 0.2::float8, 'a')::audit_t), (2, 0, ROW('2026-07-22 15:16:00+00'::timestamptz, '2 years 3 mons'::interval, 1e-7::float8, 'b')::audit_t);
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')

> CREATE TABLE events FROM SOURCE mz_source (REFERENCE events)
  WITH (TEXT COLUMNS (audit))

> SELECT count(*) FROM events
2

# TimeZone leg.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER DATABASE postgres SET timezone = 'Europe/Berlin';
SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL;

# If the terminated walsender decodes the UPDATE before exiting, it still
# renders with the old settings and nothing diverges. The corruption requires
# the UPDATE to be decoded by a fresh session.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE events SET val = 1 WHERE id = 2;

# Ingestion barrier through a plan without multiplicity checks.
> SELECT sum(val)::int FROM events
1

> SELECT count(*) FROM (SELECT DISTINCT id, val, audit FROM events)
2

> WITH t AS (SELECT * FROM events) SELECT count(*) FROM t GROUP BY row(t.*) HAVING count(*) < 0

# DateStyle leg.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER DATABASE postgres SET datestyle = 'Postgres, MDY';
SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE events SET val = 1 WHERE id = 1;

> SELECT sum(val)::int FROM events
2

> SELECT count(*) FROM (SELECT DISTINCT id, val, audit FROM events)
2

> WITH t AS (SELECT * FROM events) SELECT count(*) FROM t GROUP BY row(t.*) HAVING count(*) < 0

# IntervalStyle leg.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER DATABASE postgres SET intervalstyle = 'sql_standard';
SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE events SET val = 2 WHERE id = 2;

> SELECT sum(val)::int FROM events
3

> SELECT count(*) FROM (SELECT DISTINCT id, val, audit FROM events)
2

> WITH t AS (SELECT * FROM events) SELECT count(*) FROM t GROUP BY row(t.*) HAVING count(*) < 0

# extra_float_digits leg. Zero rounds float8 to 15 significant digits, so
# 0.30000000000000004 renders as 0.3.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER DATABASE postgres SET extra_float_digits = 0;
SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE events SET val = 2 WHERE id = 1;

> SELECT sum(val)::int FROM events
4

> SELECT count(*) FROM (SELECT DISTINCT id, val, audit FROM events)
2

> WITH t AS (SELECT * FROM events) SELECT count(*) FROM t GROUP BY row(t.*) HAVING count(*) < 0

> SELECT id, val, audit FROM events ORDER BY id
1 2 "(\"2026-07-22 15:15:00+00\",\"1 day 02:03:04\",0.30000000000000004,a)"
2 2 "(\"2026-07-22 15:16:00+00\",\"2 years 3 mons\",1e-07,b)"

# Database-level settings outlive this file. Restore them for the rest of the
# suite.
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER DATABASE postgres RESET timezone;
ALTER DATABASE postgres RESET datestyle;
ALTER DATABASE postgres RESET intervalstyle;
ALTER DATABASE postgres RESET extra_float_digits;
