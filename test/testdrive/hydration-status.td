# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test reporting of dataflow hydration status through
# `mz_internal.mz_compute_hydration_statuses`,
# `mz_internal.mz_hydration_statuses`, and
# `mz_internal.mz_compute_operator_hydration_statuses`.
#
# Note that all of the below tests only assert that the `hydrated` flag
# eventually becomes `true`, not that it starts off as `false`. That's because
# we have no control about the hydration timing of dataflows or the update
# cadence of the hydration introspection relations, so we have no reliable way
# of ensuring that a query arrives before a dataflow has hydrated.
#
# These tests rely on testdrive's retry feature, as dataflows can take an
# unknown (but hopefully small) time to hydrate. Furthermore, the queried
# introspection relations are asynchronously updated, so DDL commands are not
# immediately reflected there.

# Durable hydration history is collected by a background sweep, which visits one
# replica per cycle. Shorten its cadence so episodes land within this test's
# retry budget.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET hydration_history_collection_interval = '1s';
# Pin retention too: CI randomizes it, and a short period would prune the
# episodes asserted below before the assertions run.
ALTER SYSTEM SET hydration_history_retention_period = '30d';
# Keep both publications of the process resource observations fresh enough to
# compare within the test's retry budget.
ALTER SYSTEM SET mz_metrics_usage_refresh_interval = '1s';
ALTER SYSTEM SET compute_prometheus_introspection_scrape_interval = '1s';

> CREATE CLUSTER test REPLICAS (hydrated_test_1 (SIZE 'scale=2,workers=2'))
> SET cluster = test

# Test that on an empty cluster only the introspection indexes show up.
# Introspection subscribes do not show up because the hydration introspection
# relations exclude transient dataflow.

> SELECT DISTINCT left(h.object_id, 2), h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  WHERE r.name LIKE 'hydrated_test%';
si true

> SELECT DISTINCT left(h.object_id, 2), h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  WHERE r.name LIKE 'hydrated_test%';
si true

# No operator-level hydration status logging for introspection dataflows.
> SELECT DISTINCT left(h.object_id, 1), h.hydrated
  FROM mz_internal.mz_compute_operator_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  WHERE r.name LIKE 'hydrated_test%';

# Test adding new compute dataflows.

> CREATE TABLE t (a int)
> CREATE INDEX idx ON t (a)
> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t
> CREATE MATERIALIZED VIEW mv_const AS SELECT 1

> SELECT o.name, r.name, h.hydrated, h.hydration_time IS NOT NULL
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx      hydrated_test_1 true true
mv       hydrated_test_1 true true
mv_const hydrated_test_1 true false

# Durable history for the objects that hydrated on this replica. Collection
# samples current state, so an object that disappears before its replica's turn is
# not recorded at all.
#
# `mv_const` is deliberately not asserted on either way. It does hydrate and its
# log row does briefly carry a complete episode, but the controller then drops the
# collection and `ExportDropped` retracts the row for good. Whether a sweep lands
# in that window is a race, so pinning either outcome makes this file flaky. It is
# an instance of the general limit, that an episode retracted before its replica's
# turn cannot be recorded, rather than a special case. `idx` and `mv` keep their
# dataflows installed, so they are stable.
#
# The recorded interval must be ordered. The stamps are aggregated over a
# replica's workers, so the ends can come from different process clocks and the
# duration carries their skew. Taking the earliest installation and latest
# hydration preserves ordering. The coalesce covers a NULL start, which the
# replica no longer produces but the column still permits.
> SELECT o.name, r.name, h.status,
         h.installed_at <= h.hydrated_at,
         coalesce(h.installed_at <= h.started_at AND h.started_at <= h.hydrated_at, true)
  FROM mz_internal.mz_object_hydration_history AS h
  JOIN mz_internal.mz_object_global_ids AS g ON g.global_id = h.object_id
  JOIN mz_catalog.mz_objects AS o ON o.id = g.id
  JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
  WHERE o.name IN ('idx', 'mv')
  ORDER BY o.name, r.name;
idx hydrated_test_1 hydrated true true
mv  hydrated_test_1 hydrated true true

# Replica history rolls overlapping object intervals into one episode and waits
# for every configured process to publish resource observations. The exact
# number of initial episodes depends on whether these sequential DDL statements
# overlapped, so assert the stable properties here.
> SELECT count(*) > 0,
         bool_and(h.status = 'hydrated' AND h.started_at <= h.finished_at),
         bool_and(h.object_count > 0)
  FROM mz_internal.mz_replica_hydration_history AS h
  JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
  WHERE r.name = 'hydrated_test_1';
true true true

# Installing an object after the replica is fully hydrated starts a new replica
# episode. Its boundaries match the same per-object aggregates used by object
# history.
> SET cluster_replica = hydrated_test_1

> SELECT count(*) > 0
  FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metrics_resource_usage';
true

$ set-from-sql var=memory-peak-before
SELECT coalesce(max(value)::text, '-1')
FROM mz_introspection.mz_cluster_replica_resource_usage
WHERE source = 'cgroup'
  AND metric = 'memory_peak'

$ set-from-sql var=disk-peak-before
SELECT coalesce(coalesce(
    max(value) FILTER (
        WHERE source = 'statvfs'
          AND metric = 'fs_used_peak'
    ),
    max(value) FILTER (
        WHERE source = 'cgroup'
          AND metric = 'swap_peak'
    )
)::text, '-1')
FROM mz_introspection.mz_cluster_replica_resource_usage

> RESET cluster_replica

> CREATE INDEX replica_episode_idx ON t (a + 1)

# This timeout bounds the complete production path: installing the targeted
# subscribe, evaluating the connected-component query, and committing through
# OCC. It is intentionally generous for debug builds but far below the
# collector's five-minute mutation timeout. Test configurations with a larger
# default can raise this bound for sanitizer overhead.
$ set-sql-timeout duration=20s

> SELECT rh.status, rh.object_count,
         rh.started_at = oh.installed_at,
         rh.finished_at = oh.hydrated_at
  FROM mz_internal.mz_replica_hydration_history AS rh
  JOIN mz_internal.mz_object_hydration_history AS oh
    ON oh.replica_id = rh.replica_id
   AND oh.installed_at = rh.started_at
  JOIN mz_internal.mz_object_global_ids AS g ON g.global_id = oh.object_id
  JOIN mz_catalog.mz_objects AS o ON o.id = g.id
  WHERE o.name = 'replica_episode_idx';
hydrated 1 true true

# The Prometheus registry and resource-usage log publish the same process
# sampler independently. A Prometheus scrape after collection can observe a
# higher process-lifetime peak, so the value is bracketed by the maximum across
# process resource logs before collection and the maximum across Prometheus
# series after it rather than required to equal the later scrape. Metric
# availability must agree.
> SET cluster_replica = hydrated_test_1

> WITH recorded AS (
      SELECT rh.peak_memory_bytes, rh.peak_disk_bytes
      FROM mz_internal.mz_replica_hydration_history AS rh
      JOIN mz_internal.mz_object_hydration_history AS oh
        ON oh.replica_id = rh.replica_id
       AND oh.installed_at = rh.started_at
      JOIN mz_internal.mz_object_global_ids AS g ON g.global_id = oh.object_id
      JOIN mz_catalog.mz_objects AS o ON o.id = g.id
      WHERE o.name = 'replica_episode_idx'
  ), prometheus AS (
      SELECT
          count(DISTINCT process_id) AS process_count,
          max(value) FILTER (
              WHERE labels -> 'source' = 'cgroup'
                AND labels -> 'metric' = 'memory_peak'
          ) AS peak_memory_bytes,
          coalesce(
              max(value) FILTER (
                  WHERE labels -> 'source' = 'statvfs'
                    AND labels -> 'metric' = 'fs_used_peak'
              ),
              max(value) FILTER (
                  WHERE labels -> 'source' = 'cgroup'
                    AND labels -> 'metric' = 'swap_peak'
              )
          ) AS peak_disk_bytes
      FROM mz_introspection.mz_cluster_prometheus_metrics
      WHERE metric_name = 'mz_metrics_resource_usage'
  )
  SELECT
      p.process_count = 2,
      CASE WHEN p.peak_memory_bytes IS NULL THEN
          r.peak_memory_bytes IS NULL AND ${memory-peak-before}::double precision = -1
      ELSE
          r.peak_memory_bytes IS NOT NULL
          AND ${memory-peak-before}::double precision <= r.peak_memory_bytes::double precision
          AND r.peak_memory_bytes::double precision <= p.peak_memory_bytes
      END,
      CASE WHEN p.peak_disk_bytes IS NULL THEN
          r.peak_disk_bytes IS NULL AND ${disk-peak-before}::double precision = -1
      ELSE
          r.peak_disk_bytes IS NOT NULL
          AND ${disk-peak-before}::double precision <= r.peak_disk_bytes::double precision
          AND r.peak_disk_bytes::double precision <= p.peak_disk_bytes
      END
  FROM recorded AS r
  CROSS JOIN prometheus AS p;
true true true

> RESET cluster_replica

$ set-sql-timeout duration=default

> DROP INDEX replica_episode_idx

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx      hydrated_test_1 true
mv       hydrated_test_1 true
mv_const hydrated_test_1 true

# `mv_const` doesn't show up because the dataflow advanced to the empty
# frontier and its replica state was cleaned up, including introspection
# logging.
> SELECT o.name, r.name, bool_and(h.hydrated)
  FROM mz_internal.mz_compute_operator_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%'
  GROUP BY o.name, r.name;
idx      hydrated_test_1 true
mv       hydrated_test_1 true

# Test adding new replicas.

> CREATE CLUSTER REPLICA test.hydrated_test_2 SIZE 'scale=1,workers=1'

> SELECT o.name, r.name, h.hydrated, h.hydration_time IS NOT NULL
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx      hydrated_test_1 true true
idx      hydrated_test_2 true true
mv       hydrated_test_1 true true
mv       hydrated_test_2 true true
mv_const hydrated_test_1 true false
mv_const hydrated_test_2 true false

> SELECT o.name, r.name, h.status
  FROM mz_internal.mz_object_hydration_history AS h
  JOIN mz_internal.mz_object_global_ids AS g ON g.global_id = h.object_id
  JOIN mz_catalog.mz_objects AS o ON o.id = g.id
  JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
  WHERE o.name IN ('idx', 'mv')
  ORDER BY o.name, r.name;
idx hydrated_test_1 hydrated
idx hydrated_test_2 hydrated
mv  hydrated_test_1 hydrated
mv  hydrated_test_2 hydrated

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx      hydrated_test_1 true
idx      hydrated_test_2 true
mv       hydrated_test_1 true
mv       hydrated_test_2 true
mv_const hydrated_test_1 true
mv_const hydrated_test_2 true

> SELECT o.name, r.name, bool_and(h.hydrated)
  FROM mz_internal.mz_compute_operator_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%'
  GROUP BY o.name, r.name;
idx      hydrated_test_1 true
idx      hydrated_test_2 true
mv       hydrated_test_1 true
mv       hydrated_test_2 true

# Test dropping replicas.

> DROP CLUSTER REPLICA test.hydrated_test_1

# History outlives the replica it describes, so dropping a replica retracts
# nothing. Repeated sweeps must also not duplicate an already recorded episode.
> SELECT count(*)
  FROM mz_internal.mz_object_hydration_history AS h
  JOIN mz_internal.mz_object_global_ids AS g ON g.global_id = h.object_id
  JOIN mz_catalog.mz_objects AS o ON o.id = g.id
  WHERE o.name IN ('idx', 'mv');
4

> SELECT o.name, r.name, h.hydrated, h.hydration_time IS NOT NULL
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx      hydrated_test_2 true true
mv       hydrated_test_2 true true
mv_const hydrated_test_2 true false

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx      hydrated_test_2 true
mv       hydrated_test_2 true
mv_const hydrated_test_2 true

> SELECT o.name, r.name, bool_and(h.hydrated)
  FROM mz_internal.mz_compute_operator_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%'
  GROUP BY o.name, r.name;
idx      hydrated_test_2 true
mv       hydrated_test_2 true

> DROP CLUSTER REPLICA test.hydrated_test_2

> SELECT o.name, r.name, h.hydrated, h.hydration_time IS NOT NULL
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

> SELECT o.name, r.name, bool_and(h.hydrated)
  FROM mz_internal.mz_compute_operator_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%'
  GROUP BY o.name, r.name;

# Test dropping dataflows.

> CREATE CLUSTER REPLICA test.hydrated_test_3 SIZE 'scale=1,workers=1'

> SELECT o.name, r.name, h.hydrated, h.hydration_time IS NOT NULL
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx      hydrated_test_3 true true
mv       hydrated_test_3 true true
mv_const hydrated_test_3 true false

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx      hydrated_test_3 true
mv       hydrated_test_3 true
mv_const hydrated_test_3 true

> SELECT o.name, r.name, bool_and(h.hydrated)
  FROM mz_internal.mz_compute_operator_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%'
  GROUP BY o.name, r.name;
idx      hydrated_test_3 true
mv       hydrated_test_3 true

> DROP INDEX idx;
> DROP MATERIALIZED VIEW mv_const;

> SELECT o.name, r.name, h.hydrated, h.hydration_time IS NOT NULL
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
mv  hydrated_test_3 true true

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
mv  hydrated_test_3 true

> SELECT o.name, r.name, bool_and(h.hydrated)
  FROM mz_internal.mz_compute_operator_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%'
  GROUP BY o.name, r.name;
mv  hydrated_test_3 true

# Test adding new storage dataflows.

> BEGIN
> CREATE SOURCE src
  IN CLUSTER test
  FROM LOAD GENERATOR auction (UP TO 100);

> CREATE TABLE accounts FROM SOURCE src (REFERENCE accounts);
> CREATE TABLE auctions FROM SOURCE src (REFERENCE auctions);
> CREATE TABLE bids FROM SOURCE src (REFERENCE bids);
> CREATE TABLE organizations FROM SOURCE src (REFERENCE organizations);
> CREATE TABLE users FROM SOURCE src (REFERENCE users);
> COMMIT

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CONNECTION csr_conn
  TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}')
> CREATE SINK snk
  IN CLUSTER test
  FROM mv
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# Webhook sources are not backed by dataflows, so they have no concept of
# hydration and shouldn't show up in mz_hydration_statuses.
> CREATE SOURCE web IN CLUSTER test FROM WEBHOOK BODY FORMAT JSON

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
mv            hydrated_test_3 true
src           hydrated_test_3 true
snk           hydrated_test_3 true

# Test dropping replicas.

> DROP CLUSTER REPLICA test.hydrated_test_3

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

# Test adding new replicas.

> CREATE CLUSTER REPLICA test.hydrated_test_4 SIZE 'scale=1,workers=1'

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
mv            hydrated_test_4 true
src           hydrated_test_4 true
snk           hydrated_test_4 true

# Test dropping dataflows.

> DROP SINK snk
> DROP SOURCE src CASCADE
> DROP MATERIALIZED VIEW mv

# Retention runs in the same sweep on the catalog server. It retracts aged-out rows
# through the same OCC path, in bounded batches. A zero retention period expires
# everything.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET hydration_history_retention_period = '0s';

> SELECT count(*) FROM mz_internal.mz_object_hydration_history;
0

> SELECT count(*) FROM mz_internal.mz_replica_hydration_history;
0

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

# Test hydration status reporting with WMR dataflows.

> CREATE MATERIALIZED VIEW mv_wmr AS
  WITH MUTUALLY RECURSIVE
      x (a int) AS (
          SELECT * FROM t
          UNION ALL
          SELECT a + 1 FROM x WHERE a < 10
      )
  SELECT * FROM x;
> CREATE MATERIALIZED VIEW mv_wmr_const AS
  WITH MUTUALLY RECURSIVE
      x (a int) AS (
          VALUES (1)
          UNION ALL
          SELECT a + 1 FROM x WHERE a < 10
      )
  SELECT * FROM x;
> CREATE MATERIALIZED VIEW mv_wmr_stuck AS
  WITH MUTUALLY RECURSIVE
      x (a int) AS (
          VALUES (1)
          UNION ALL
          SELECT a + 1 FROM x
      )
  SELECT * FROM x;

> SELECT o.name, r.name, h.hydrated, h.hydration_time IS NOT NULL
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
mv_wmr       hydrated_test_4 true  true
mv_wmr_const hydrated_test_4 true  false
mv_wmr_stuck hydrated_test_4 false false

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
mv_wmr       hydrated_test_4 true
mv_wmr_const hydrated_test_4 true
mv_wmr_stuck hydrated_test_4 false

> SELECT o.name, r.name, bool_and(h.hydrated)
  FROM mz_internal.mz_compute_operator_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%'
  GROUP BY o.name, r.name;
mv_wmr       hydrated_test_4 true
mv_wmr_stuck hydrated_test_4 false

> DROP MATERIALIZED VIEW mv_wmr
> DROP MATERIALIZED VIEW mv_wmr_const
> DROP MATERIALIZED VIEW mv_wmr_stuck

# Test that incorrectly configured sinks do _not_ show as hydrated.

> CREATE TABLE schema1 (a int)
> CREATE TABLE schema2 (a text)

> CREATE SINK snk_schema1
  IN CLUSTER test
  FROM schema1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-snk_schema1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT s.name, h.hydrated
  FROM mz_sinks s
  JOIN mz_internal.mz_hydration_statuses h ON (h.object_id = s.id)
snk_schema1 true

> CREATE SINK snk_schema2
  IN CLUSTER test
  FROM schema2
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-snk_schema1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# If we have a bug, the sink's hydration status might toggle between `false`
# and `true`, rather than always being `true`. Using retries might therefore
# cause this test to pass even if it shouldn't. We instead disable retries and
# manually check the hydration status a couple times.
$ set-max-tries max-tries=1

> SELECT s.name, h.hydrated
  FROM mz_sinks s
  JOIN mz_internal.mz_hydration_statuses h ON (h.object_id = s.id)
snk_schema1 true
snk_schema2 false

> SELECT s.name, h.hydrated
  FROM mz_sinks s
  JOIN mz_internal.mz_hydration_statuses h ON (h.object_id = s.id)
snk_schema1 true
snk_schema2 false

> SELECT s.name, h.hydrated
  FROM mz_sinks s
  JOIN mz_internal.mz_hydration_statuses h ON (h.object_id = s.id)
snk_schema1 true
snk_schema2 false

> SELECT s.name, h.hydrated
  FROM mz_sinks s
  JOIN mz_internal.mz_hydration_statuses h ON (h.object_id = s.id)
snk_schema1 true
snk_schema2 false

> SELECT s.name, h.hydrated
  FROM mz_sinks s
  JOIN mz_internal.mz_hydration_statuses h ON (h.object_id = s.id)
snk_schema1 true
snk_schema2 false
