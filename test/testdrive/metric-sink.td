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

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_metric_sink = true;

> CREATE TABLE t (k text NOT NULL, val double NOT NULL)

> INSERT INTO t VALUES ('a', 1), ('b', 2)

# `map_build` always reports a nullable result regardless of its input's
# nullability, so `coalesce` with a non-null default is required for `labels`
# to satisfy the metric sink source schema's NOT NULL requirement.
> CREATE VIEW v AS
  SELECT 'mz_metric_sink_test_value'::text AS metric_name,
         'gauge'::text AS metric_type,
         coalesce(map_build(LIST[ROW('k', k)])::map[text=>text], '{}'::map[text=>text]) AS labels,
         val AS value,
         'a metric emitted by the metric sink e2e test'::text AS help
  FROM t

> CREATE METRIC SINK s IN CLUSTER quickstart FROM v

$ set-from-sql var=sink-id
SELECT id FROM mz_catalog.mz_metric_sinks WHERE name = 's';

# One family, two series: one per distinct label set.
> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value'
2

> SELECT value = 1 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value' AND labels -> 'k' = 'a'
true

> SELECT value = 2 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value' AND labels -> 'k' = 'b'
true

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value'
    AND metric_type = 'gauge'
    AND help = 'a metric emitted by the metric sink e2e test'
2

# The companion gauges exist, are labeled with this sink's id, and read healthy
# (no errors, no skipped/conflicting/colliding series).
> SELECT value > 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_frontier_ms' AND labels -> 'sink' = '${sink-id}'
true

> SELECT value = 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_errors' AND labels -> 'sink' = '${sink-id}'
true

> SELECT value = 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_skipped' AND labels -> 'sink' = '${sink-id}'
true

> SELECT value = 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_conflicts' AND labels -> 'sink' = '${sink-id}'
true

> SELECT value = 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_collisions' AND labels -> 'sink' = '${sink-id}'
true

> DROP METRIC SINK s

# The catalog row is gone immediately (durable transaction).
> SELECT count(*) FROM mz_catalog.mz_metric_sinks
0

# The user series and all of this sink's companion gauges are retracted from
# the registry once the next scrape observes the collector is gone.
> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value'
0

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE labels -> 'sink' = '${sink-id}'
0

> DROP VIEW v

> DROP TABLE t

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_metric_sink;
