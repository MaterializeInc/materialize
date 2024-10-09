# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Index

## No retractions
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '30d'
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );
> CREATE VIEW last_30_days AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '20 days';
> CREATE DEFAULT INDEX ON last_30_days;
> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;
> SELECT records FROM mz_introspection.mz_arrangement_sizes mas JOIN mz_introspection.mz_dataflow_operators mdo ON mas.operator_id = mdo.id AND name = 'ArrangeBy[[Column(0), Column(1)]]';
2000
> DROP TABLE events CASCADE;
> DROP CLUSTER test;

## Does retractions
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = '20d'
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );
> CREATE VIEW last_30_days AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '30 days';
> CREATE DEFAULT INDEX ON last_30_days;
> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;
> SELECT records FROM mz_introspection.mz_arrangement_sizes mas JOIN mz_introspection.mz_dataflow_operators mdo ON mas.operator_id = mdo.id AND name = 'ArrangeBy[[Column(0), Column(1)]]';
1000
> DROP TABLE events CASCADE;
> DROP CLUSTER test;


# Check that transitive refresh disables expiration

## No transitive refresh enables expiration, resulting in no retractions
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
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
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


# Constant MVs, indexes, subscribes disables

# TODO


# Ensure disabling expiration results in retractions
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration_offset = 0;
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test
> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );
> CREATE VIEW last_30_days AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '30 days';
> CREATE DEFAULT INDEX ON last_30_days;
> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;
> SELECT records FROM mz_introspection.mz_arrangement_sizes mas JOIN mz_introspection.mz_dataflow_operators mdo ON mas.operator_id = mdo.id AND name = 'ArrangeBy[[Column(0), Column(1)]]';
2000
> DROP TABLE events CASCADE;
> DROP CLUSTER test;
