# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test replica expiration.

## No retractions: expiration=30d, temporal filter width=20d
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '30d'
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );
> CREATE VIEW events_view AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '20 days';
> CREATE DEFAULT INDEX ON events_view;
> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;
> SELECT records FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%events_view_primary_idx';
2000
> DROP TABLE events CASCADE;
> DROP CLUSTER test;

## Does retractions: expiration=20d, temporal filter width=30d
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '20d'
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );
> CREATE VIEW events_view AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '30 days';
> CREATE DEFAULT INDEX ON events_view;
> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;
> SELECT records FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%events_view_primary_idx';
1000
> DROP TABLE events CASCADE;
> DROP CLUSTER test;


# Test materialize views. `mz_introspection` does not report number of records, but we just make
# sure that the queries work.

## No retractions: expiration=30d, temporal filter width=20d
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '30d'
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );
> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;
> CREATE MATERIALIZED VIEW events_mv AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '20 days';
> SELECT count(*) FROM events_mv where content like '1%';
112
> DROP TABLE events CASCADE;
> DROP CLUSTER test;

## Does retractions: expiration=20d, temporal filter width=30d
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '20d'
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );
> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;
> CREATE MATERIALIZED VIEW events_mv AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '30 days';
> SELECT count(*) FROM events_mv where content like '1%';
112
> DROP TABLE events CASCADE;
> DROP CLUSTER test;


# Check that transitive refresh disables expiration even when enabled: expiration=20d, temporal filter width=30d

## No transitive refresh enables expiration, thus no retractions
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '20d'
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    id INT,
    content TEXT,
    event_ts TIMESTAMP
  );
> INSERT INTO events SELECT x, x::text, now() FROM generate_series(1, 1000) AS x;
> CREATE MATERIALIZED VIEW mv AS
  SELECT id, content, event_ts from events WHERE id < 100;
> CREATE VIEW view1 AS
  SELECT id, content, event_ts
  FROM mv
  WHERE content like '1%';
> CREATE VIEW view2 AS
  SELECT id, content, event_ts
  FROM view1
  WHERE content like '__';
> CREATE VIEW view3 AS
  SELECT id, content, event_ts
  FROM view2
  WHERE mz_now() <= event_ts + INTERVAL '30 days';
> CREATE DEFAULT INDEX ON view3;
> SELECT records FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%view3_primary_idx';
10
> DROP TABLE events CASCADE;
> DROP MATERIALIZED VIEW if exists mv CASCADE;
> DROP VIEW if exists view1 CASCADE;
> DROP VIEW if exists view2 CASCADE;
> DROP VIEW if exists view3 CASCADE;
> DROP CLUSTER test;

## Transitive refresh disables expiration, resulting in retractions
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '20d'
> CREATE CLUSTER test (SIZE = '1');
> SET CLUSTER TO test;
> CREATE TABLE events (
    id INT,
    content TEXT,
    event_ts TIMESTAMP
  );
> INSERT INTO events SELECT x, x::text, now() FROM generate_series(1, 1000) AS x;
> CREATE MATERIALIZED VIEW mv
  WITH (REFRESH AT CREATION, REFRESH AT '3000-01-01 23:59') AS
  SELECT id, content, event_ts from events WHERE id < 100;
> CREATE VIEW view1 AS
  SELECT id, content, event_ts
  FROM mv
  WHERE content like '1%';
> CREATE VIEW view2 AS
  SELECT id, content, event_ts
  FROM view1
  WHERE content like '__';
> CREATE VIEW view3 AS
  SELECT id, content, event_ts
  FROM view2
  WHERE mz_now() <= event_ts + INTERVAL '30 days';
> CREATE DEFAULT INDEX ON view3;
> SELECT records FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%view3_primary_idx';
20
> DROP TABLE events CASCADE;
> DROP MATERIALIZED VIEW if exists mv CASCADE;
> DROP VIEW if exists view1 CASCADE;
> DROP VIEW if exists view2 CASCADE;
> DROP VIEW if exists view3 CASCADE;
> DROP CLUSTER test;


# Views with constant values disable expiration even when enabled: expiration=20d, temporal filter width=30d

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '20d';
> CREATE CLUSTER test (SIZE = '1');
> SET CLUSTER TO test;
> CREATE VIEW events_over_time AS VALUES ('joe', 100), ('mike', 101), ('sam', 200), ('end', 18446144073709551615);
> CREATE VIEW events AS SELECT * FROM events_over_time WHERE mz_now() <= column2 + 2592000000; -- 30d in ms
> CREATE DEFAULT INDEX ON events;
> SUBSCRIBE events WITH (progress) AS OF 0;
mz_timestamp mz_progressed mz_diff column1 column2
----
0 false 1 end 18446144073709551615
0 false 1 joe 100
0 false 1 mike 101
0 false 1 sam 200
0 true <null> <null> <null>
18446144076301551616 false -1 end 18446144073709551615
2592000101 false -1 joe 100
2592000102 false -1 mike 101
2592000201 false -1 sam 200
> DROP VIEW if exists events_over_time CASCADE;
> DROP VIEW if exists events CASCADE;
> DROP CLUSTER test;


# Ensure disabling expiration results in retractions

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = 0;
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );
> CREATE VIEW events_view AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '30 days';
> CREATE DEFAULT INDEX ON events_view;
> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;
> SELECT records FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%events_view_primary_idx';
2000
> DROP TABLE events CASCADE;
> DROP CLUSTER test;


# Test that a constant collection is not expired on a cluster with replication factor 0

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '30d';
> CREATE CLUSTER test (SIZE = '1', REPLICATION FACTOR = 0);
> SET CLUSTER TO test;
# `now()` cannot be materialized.
> CREATE MATERIALIZED VIEW events_mv AS
  SELECT x::text AS content, '2024-10-09 07:05:10.318+00'::timestamptz AS event_ts
  FROM generate_series(1, 1000) AS x;
> CREATE VIEW events_view AS
  SELECT event_ts, content
  FROM events_mv
  WHERE mz_now() <= event_ts + INTERVAL '30 years';
> CREATE DEFAULT INDEX ON events_view;
> ALTER CLUSTER test SET (REPLICATION FACTOR = 1);
> SELECT count(*) FROM events_view;
1000
> DROP MATERIALIZED VIEW events_mv CASCADE;
> DROP CLUSTER test CASCADE;
