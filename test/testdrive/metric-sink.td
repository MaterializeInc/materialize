# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# End-to-end test for CREATE METRIC SINK: verifies that the series computed by
# a metric sink's dataflow actually reach the cluster replica's Prometheus
# registry, as observed through `mz_introspection.mz_cluster_prometheus_metrics`
# (the same registry the sink's collector registers into, scraped on
# `compute_prometheus_introspection_scrape_interval`), and that DROP METRIC
# SINK removes both the user series and the sink's companion gauges.
#
# This test does not depend on the deferred `mz_metric_sinks` catalog view; it
# reads the sink's id from the `sink` label the operator stamps on its own
# companion gauges.

# A scrape has to land between a write and the assertion that observes it, so
# the default timeout is too tight here.
$ set-sql-timeout duration=60s

# `mz_cluster_prometheus_metrics` is a per-replica log source, so every read of it
# below has to name a replica. The sink runs on each replica of its cluster and
# each replica owns its own registry, so targeting one replica is also what keeps
# the series counts below independent of how many replicas `quickstart` has. `r1`
# is the default replica name, and the multi-replica testdrive configurations keep
# a replica of that name on `quickstart`.
> SET cluster_replica = r1

> CREATE TABLE t (k text NOT NULL, val double)

> INSERT INTO t VALUES ('a', 1), ('b', 2)

> CREATE VIEW v AS
  SELECT 'mz_metric_sink_test_value'::text AS metric_name,
         'gauge'::text AS metric_type,
         map_build(LIST[ROW('k', k)])::map[text=>text] AS labels,
         val AS value,
         'a metric emitted by the metric sink e2e test'::text AS help
  FROM t

# The FROM relation must expose the canonical metric-sink columns. A relation
# missing `value` (or with a wrongly-typed column) is rejected at plan time.
> CREATE VIEW bad_missing_value AS
  SELECT 'm'::text AS metric_name,
         'gauge'::text AS metric_type,
         map_build(LIST[ROW('k', k)])::map[text=>text] AS labels,
         'help'::text AS help
  FROM t

! CREATE METRIC SINK bad IN CLUSTER quickstart FROM bad_missing_value WITH (PREFIX = 'mz_metric_sink_bad_')
contains:metric sink source must expose column "value"

> CREATE VIEW bad_wrong_type AS
  SELECT 'm'::text AS metric_name,
         'gauge'::text AS metric_type,
         map_build(LIST[ROW('k', k)])::map[text=>text] AS labels,
         val::int AS value,
         'help'::text AS help
  FROM t

! CREATE METRIC SINK bad IN CLUSTER quickstart FROM bad_wrong_type WITH (PREFIX = 'mz_metric_sink_bad_')
contains:metric sink source column "value" is not of the required type

> DROP VIEW bad_missing_value

> DROP VIEW bad_wrong_type

> CREATE METRIC SINK s IN CLUSTER quickstart FROM v WITH (PREFIX = 'mz_metric_sink_e2e_')

# One family, two series: one per distinct label set.
> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value'
2

> SELECT value = 1 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value' AND labels -> 'k' = 'a'
true

> SELECT value = 2 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value' AND labels -> 'k' = 'b'
true

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value'
    AND metric_type = 'gauge'
    AND help = 'a metric emitted by the metric sink e2e test'
2

# Read the sink's id off the companion gauge it stamps with a `sink` label.
# `LIMIT 1` guards the single-row contract of `set-from-sql`: today `s` is the
# only metric sink, but a second one (user or builtin) sharing this scrape
# window would otherwise make the capture fail.
$ set-from-sql var=sink-id
SELECT labels -> 'sink' FROM mz_introspection.mz_cluster_prometheus_metrics
WHERE metric_name = 'mz_compute_metric_sink_frontier_ms'
LIMIT 1

# The companion gauges exist, are labeled with this sink's id, and read healthy
# (no errors, no skipped/conflicting/colliding series).
> SELECT value > 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_metric_sink_frontier_ms' AND labels -> 'sink' = '${sink-id}'
true

# Regression test for the compute write-frontier report (`sink_write_frontier`
# in `mz_compute::sink::metric_sink`). A metric sink writes to the process-local
# registry, not to a storage collection, but it still owns a compute write
# frontier the controller must observe. Without the report, `report_frontiers`
# logs `collection without write frontier` and skips the sink, so the controller
# never sees progress and pins the since of the relation the sink reads forever.
# A metric sink is not a storage sink, so it appears in `mz_frontiers`; its write
# frontier must advance past the value captured just after creation. Absent the
# report the frontier stays frozen at its creation `as_of` and this times out.
$ set-from-sql var=wf-before
SELECT write_frontier::text FROM mz_internal.mz_frontiers WHERE object_id = '${sink-id}'

> SELECT write_frontier > ${wf-before}::mz_timestamp
  FROM mz_internal.mz_frontiers WHERE object_id = '${sink-id}'
true

> SELECT value = 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_metric_sink_errors' AND labels -> 'sink' = '${sink-id}'
true

> SELECT value = 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_metric_sink_skipped' AND labels -> 'sink' = '${sink-id}'
true

# A null `value` does not publish a zero: the `(metric_name, labels)` series
# for 'c' is absent from the registry (a gap) while its only live value is
# null, and the sink counts it in `mz_compute_metric_sink_null_values`.
> INSERT INTO t VALUES ('c', NULL)

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value'
2

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value' AND labels -> 'k' = 'c'
0

> SELECT value = 1 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_metric_sink_null_values' AND labels -> 'sink' = '${sink-id}'
true

# The gap closes once a non-null value arrives for the same series.
> UPDATE t SET val = 3 WHERE k = 'c'

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value'
3

> SELECT value = 3 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value' AND labels -> 'k' = 'c'
true

> SELECT value = 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_metric_sink_null_values' AND labels -> 'sink' = '${sink-id}'
true

# The sink pins the relation it reads, so the view can only go with the sink.
! DROP VIEW v
contains:still depended upon by metric sink "s"

> DROP METRIC SINK s

# The user series and all of this sink's companion gauges are retracted from
# the registry once the next scrape observes the collector is gone.
> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value'
0

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE labels -> 'sink' = '${sink-id}'
0

# CASCADE takes the sink down with the relation it reads.
> CREATE METRIC SINK s_cascade IN CLUSTER quickstart FROM v WITH (PREFIX = 'mz_metric_sink_e2e_')

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value'
3

> DROP VIEW v CASCADE

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_e2e_mz_metric_sink_test_value'
0

> DROP TABLE t

# A null or empty label *value* has no representable Prometheus label set: a null has nothing to
# encode, and an empty value reads as absent to Prometheus, which would fold `{a => ''}` into `{}`.
# The sink skips the whole row and counts it in `mz_compute_metric_sink_skipped` rather than
# publish a mislabeled series. `map_build` keeps a null value (it drops only null keys), so the two
# rows reach the sink as `{a => NULL}` and `{a => ''}`, distinct identities that are each skipped.
> CREATE TABLE lt (lv text)

> INSERT INTO lt VALUES (NULL), ('')

> CREATE VIEW lv AS
  SELECT 'skip_test'::text AS metric_name,
         'gauge'::text AS metric_type,
         map_build(LIST[ROW('a', lv)])::map[text=>text] AS labels,
         1::double AS value,
         'a metric skipped by the metric sink e2e test'::text AS help
  FROM lt

> CREATE METRIC SINK s_skip IN CLUSTER quickstart FROM lv WITH (PREFIX = 'mz_metric_sink_skip_')

# Both rows are skipped and stay distinct (a null value is not the same identity as an empty one),
# so the gauge reads 2, not 1. This retrying read also waits for the scrape to observe the sink;
# `s_skip` is the only metric sink here, so no `sink`-label filter is needed.
> SELECT value FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_metric_sink_skipped'
2

# Neither row publishes a series under the sink's metric name.
> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_skip_skip_test'
0

> DROP TABLE lt CASCADE

# Replica churn drives the coordinator's curated metric-sink install and
# teardown (`install_metric_sinks` / `drop_metric_sinks`). The curated list is
# empty, so there is no curated series to assert on: what is under test is that
# both hooks run over an empty list without erroring, and that a replica added
# and removed under a live sink leaves that sink publishing on the replica that
# remains.
#
# On its own cluster, so the churn does not disturb the replica serving the rest
# of this file. `quickstart` is a managed cluster anyway, which rejects
# `CREATE CLUSTER REPLICA`, so replication factor is the way to add and remove a
# replica.
> CREATE CLUSTER churn_c (SIZE 'scale=1,workers=1')

> CREATE TABLE churn_t (k text NOT NULL, val double)

> INSERT INTO churn_t VALUES ('a', 1)

> CREATE VIEW churn_v AS
  SELECT 'value'::text AS metric_name,
         'gauge'::text AS metric_type,
         map_build(LIST[ROW('k', k)])::map[text=>text] AS labels,
         val AS value,
         'a metric emitted across replica churn'::text AS help
  FROM churn_t

> CREATE METRIC SINK s_churn IN CLUSTER churn_c FROM churn_v WITH (PREFIX = 'mz_metric_sink_churn_')

# Read `churn_c`'s registry, not `quickstart`'s: the relation is per-replica
# introspection, served by the session's active cluster.
> SET cluster = churn_c

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_churn_value'
1

> ALTER CLUSTER churn_c SET (REPLICATION FACTOR 2)

> ALTER CLUSTER churn_c SET (REPLICATION FACTOR 1)

# Nothing is asserted while both replicas are up: reading a log relation on a
# cluster with more than one replica errors as `UntargetedLogRead` unless the
# session picks one. So this reads the surviving replica's registry, after the
# churn.
> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_churn_value'
1

> RESET cluster

> DROP CLUSTER churn_c CASCADE

> DROP TABLE churn_t CASCADE
