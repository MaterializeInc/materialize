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

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_metric_sink = true;

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

! CREATE METRIC SINK bad IN CLUSTER quickstart FROM bad_missing_value
contains:metric sink source must expose column "value"

> CREATE VIEW bad_wrong_type AS
  SELECT 'm'::text AS metric_name,
         'gauge'::text AS metric_type,
         map_build(LIST[ROW('k', k)])::map[text=>text] AS labels,
         val::int AS value,
         'help'::text AS help
  FROM t

! CREATE METRIC SINK bad IN CLUSTER quickstart FROM bad_wrong_type
contains:metric sink source column "value" is not of the required type

> DROP VIEW bad_missing_value

> DROP VIEW bad_wrong_type

> CREATE METRIC SINK s IN CLUSTER quickstart FROM v

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

# Read the sink's id off the companion gauge it stamps with a `sink` label.
$ set-from-sql var=sink-id
SELECT labels -> 'sink' FROM mz_introspection.mz_cluster_prometheus_metrics
WHERE metric_name = 'mz_metric_sink_frontier_ms'

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

# A null `value` does not publish a zero: the `(metric_name, labels)` series
# for 'c' is absent from the registry (a gap) while its only live value is
# null, and the sink counts it in `mz_metric_sink_null_values`.
> INSERT INTO t VALUES ('c', NULL)

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value'
2

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value' AND labels -> 'k' = 'c'
0

> SELECT value = 1 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_null_values' AND labels -> 'sink' = '${sink-id}'
true

# The gap closes once a non-null value arrives for the same series.
> UPDATE t SET val = 3 WHERE k = 'c'

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value'
3

> SELECT value = 3 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_test_value' AND labels -> 'k' = 'c'
true

> SELECT value = 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_metric_sink_null_values' AND labels -> 'sink' = '${sink-id}'
true

> DROP METRIC SINK s

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
