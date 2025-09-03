# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the contents of `mz_wallclock_lag_history`.
#
# These tests rely on testdrive's retry feature, as `mz_wallclock_lag_history`
# is only refreshed periodically, so data is likely not immediately available.

$ set-sql-timeout duration=120s

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET wallclock_lag_recording_interval = '1s'

> CREATE CLUSTER storage SIZE 'scale=1,workers=1'
> CREATE CLUSTER compute SIZE 'scale=1,workers=1', REPLICATION FACTOR 2

# Set up a bunch of frontiered objects and test that their wallclock lags get
# reported and are reasonably small.

> CREATE SOURCE src IN CLUSTER storage FROM LOAD GENERATOR counter (UP TO 100)

> CREATE TABLE tbl (a int)

> CREATE VIEW src_plus_tbl AS SELECT counter + a AS a FROM src, tbl
> CREATE INDEX idx IN CLUSTER compute ON src_plus_tbl (a)
> CREATE MATERIALIZED VIEW mv IN CLUSTER compute AS SELECT * FROM src_plus_tbl

> CREATE MATERIALIZED VIEW mv_const IN CLUSTER compute AS SELECT 1
> CREATE DEFAULT INDEX idx_const IN CLUSTER compute ON mv_const

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CONNECTION csr_conn
  TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}')
> CREATE SINK snk
  IN CLUSTER storage
  FROM mv
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT DISTINCT ON(o.name, r.name)
    o.name, r.name, l.lag >= '0s', l.lag < '20s'
  FROM mz_internal.mz_wallclock_lag_history l
  JOIN mz_objects o ON o.id = l.object_id
  LEFT JOIN mz_cluster_replicas r ON r.id = l.replica_id
  WHERE l.object_id LIKE 'u%'
  ORDER BY o.name, r.name, l.occurred_at DESC
idx          r1     true  true
idx          r2     true  true
idx_const    r1     true  true
idx_const    r2     true  true
mv           r1     true  true
mv           r2     true  true
mv           <null> true  true
mv_const     r1     true  true
mv_const     r2     true  true
mv_const     <null> true  true
snk          <null> true  true
src          <null> true  true
src_progress <null> true  true
tbl          <null> true  true

> SELECT DISTINCT ON(o.name)
    o.name, l.lag >= '0s', l.lag < '20s'
  FROM mz_internal.mz_wallclock_global_lag_history l
  JOIN mz_objects o ON o.id = l.object_id
  WHERE l.object_id LIKE 'u%'
  ORDER BY o.name, l.occurred_at DESC
idx          true  true
idx_const    true  true
mv           true  true
mv_const     true  true
snk          true  true
src          true  true
src_progress true  true
tbl          true  true

> SELECT DISTINCT ON(o.name)
    o.name, l.lag >= '0s', l.lag < '20s'
  FROM mz_internal.mz_wallclock_global_lag_recent_history l
  JOIN mz_objects o ON o.id = l.object_id
  WHERE l.object_id LIKE 'u%'
  ORDER BY o.name, l.occurred_at DESC
idx          true  true
idx_const    true  true
mv           true  true
mv_const     true  true
snk          true  true
src          true  true
src_progress true  true
tbl          true  true

> SELECT o.name, l.lag >= '0s', l.lag < '20s'
  FROM mz_internal.mz_wallclock_global_lag l
  JOIN mz_objects o ON o.id = l.object_id
  WHERE l.object_id LIKE 'u%'
idx          true  true
idx_const    true  true
mv           true  true
mv_const     true  true
snk          true  true
src          true  true
src_progress true  true
tbl          true  true

> SELECT DISTINCT o.name, l.count > 0, l.labels
  FROM mz_internal.mz_wallclock_global_lag_histogram l
  JOIN mz_objects o ON o.id = l.object_id
  WHERE l.object_id LIKE 'u%' AND l.lag_seconds < 10
idx          true  {}
idx_const    true  {}
mv           true  {}
mv_const     true  {}
snk          true  {}
src          true  {}
src_progress true  {}
tbl          true  {}

# Test that history lag values are rounded to seconds.

> SELECT DISTINCT ON(o.name, r.name)
    o.name, r.name, l.lag = date_trunc('second', l.lag)
  FROM mz_internal.mz_wallclock_lag_history l
  JOIN mz_objects o ON o.id = l.object_id
  LEFT JOIN mz_cluster_replicas r ON r.id = l.replica_id
  WHERE l.object_id LIKE 'u%'
  ORDER BY o.name, r.name, l.occurred_at DESC
idx          r1     true
idx          r2     true
idx_const    r1     true
idx_const    r2     true
mv           r1     true
mv           r2     true
mv           <null> true
mv_const     r1     true
mv_const     r2     true
mv_const     <null> true
snk          <null> true
src          <null> true
src_progress <null> true
tbl          <null> true

# Test annotation of histogram measurements with labels.

$ postgres-execute connection=mz_system
ALTER CLUSTER compute SET (WORKLOAD CLASS 'compute')
ALTER CLUSTER storage SET (WORKLOAD CLASS 'storage')

> SELECT DISTINCT o.name, l.count > 0, l.labels
  FROM mz_internal.mz_wallclock_global_lag_histogram l
  JOIN mz_objects o ON o.id = l.object_id
  WHERE l.object_id LIKE 'u%' AND l.lag_seconds < 10
idx          true  "{}"
idx          true  "{\"workload_class\":\"compute\"}"
idx_const    true  "{}"
idx_const    true  "{\"workload_class\":\"compute\"}"
mv           true  "{}"
mv           true  "{\"workload_class\":\"compute\"}"
mv_const     true  "{}"
mv_const     true  "{\"workload_class\":\"compute\"}"
snk          true  "{}"
snk          true  "{\"workload_class\":\"storage\"}"
src          true  "{}"
src          true  "{\"workload_class\":\"storage\"}"
src_progress true  "{}"
tbl          true  "{}"

# Test changing the histogram period interval.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET wallclock_lag_histogram_period_interval = '1d'

> CREATE TABLE tbl_1day (x int)
> CREATE INDEX idx_1day IN CLUSTER compute ON tbl_1day (x)
> SELECT DISTINCT
    o.name,
    l.period_end - l.period_start,
    date_trunc('day', l.period_start) = l.period_start,
    date_trunc('day', l.period_end) = l.period_end
  FROM mz_internal.mz_wallclock_global_lag_histogram l
  JOIN mz_objects o ON o.id = l.object_id
  WHERE o.name LIKE '%_1day'
idx_1day 24:00:00 true true
tbl_1day 24:00:00 true true

$ postgres-execute connection=mz_system
ALTER SYSTEM SET wallclock_lag_histogram_period_interval = '1h'

> CREATE TABLE tbl_1hour (x int)
> CREATE INDEX idx_1hour IN CLUSTER compute ON tbl_1day (x)
> SELECT DISTINCT
    o.name,
    l.period_end - l.period_start,
    date_trunc('hour', l.period_start) = l.period_start,
    date_trunc('hour', l.period_end) = l.period_end
  FROM mz_internal.mz_wallclock_global_lag_histogram l
  JOIN mz_objects o ON o.id = l.object_id
  WHERE o.name LIKE '%_1hour'
idx_1hour 01:00:00 true true
tbl_1hour 01:00:00 true true

# Test that lags of unreadable collections are NULL.

> DROP CLUSTER storage CASCADE
> DROP CLUSTER compute CASCADE
> CREATE CLUSTER storage SIZE 'scale=1,workers=1', REPLICATION FACTOR 0
> CREATE CLUSTER compute SIZE 'scale=1,workers=1'

> CREATE SOURCE src IN CLUSTER storage FROM LOAD GENERATOR counter (UP TO 100)
> CREATE INDEX idx IN CLUSTER compute ON src (counter)
> CREATE MATERIALIZED VIEW mv IN CLUSTER compute AS SELECT * FROM src
> CREATE SINK snk
  IN CLUSTER storage
  FROM mv
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT DISTINCT o.name, r.name, l.lag
  FROM mz_internal.mz_wallclock_lag_history l
  JOIN mz_objects o ON o.id = l.object_id
  LEFT JOIN mz_cluster_replicas r ON r.id = l.replica_id
  WHERE l.object_id LIKE 'u%' AND o.cluster_id IS NOT NULL
idx           r1      <null>
mv            r1      <null>
mv            <null>  <null>
snk           <null>  <null>
src           <null>  <null>

> SELECT DISTINCT o.name, l.lag_seconds
  FROM mz_internal.mz_wallclock_global_lag_histogram l
  JOIN mz_objects o ON o.id = l.object_id
  WHERE l.object_id LIKE 'u%' AND o.cluster_id IS NOT NULL
idx  <null>
mv   <null>
snk  <null>
src  <null>

> DROP CLUSTER storage CASCADE
> DROP CLUSTER compute CASCADE
