# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test: MV correction buffer correctly updates metrics on drop.

$ set-sql-timeout duration=60s

> CREATE CLUSTER test SIZE 'scale=1,workers=1'
> SET cluster = test

> CREATE TABLE t1 (a int)
> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t1

# Create a replacement MV with a different definition. Its correction buffer
# accumulates the diff in read-only mode, so data stays in the buffer until we
# drop the replacement.
> CREATE TABLE t2 (a int)
> CREATE REPLACEMENT MATERIALIZED VIEW rp FOR mv AS SELECT * FROM t2

> INSERT INTO t2 SELECT generate_series(1, 1000)

# Wait until the replacement's correction buffer has accumulated data.
> SELECT value >= 1000 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_persist_sink_correction_insertions_total'
true

> SELECT value < 1000 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_persist_sink_correction_deletions_total'
true

> DROP MATERIALIZED VIEW mv CASCADE

# After all correction buffers have been dropped, insertions and deletions
# should balance out.
> SELECT
    (SELECT value FROM mz_introspection.mz_cluster_prometheus_metrics
     WHERE metric_name = 'mz_persist_sink_correction_insertions_total')
    =
    (SELECT value FROM mz_introspection.mz_cluster_prometheus_metrics
     WHERE metric_name = 'mz_persist_sink_correction_deletions_total')
true

> SELECT
    (SELECT value FROM mz_introspection.mz_cluster_prometheus_metrics
     WHERE metric_name = 'mz_persist_sink_correction_capacity_increases_total')
    =
    (SELECT value FROM mz_introspection.mz_cluster_prometheus_metrics
     WHERE metric_name = 'mz_persist_sink_correction_capacity_decreases_total')
true

# Clean up.
> DROP CLUSTER test
