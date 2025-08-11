# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/database-issues/issues/3955

> CREATE CLUSTER test (SIZE 'scale=4,workers=4');
> SET cluster = test;

# In case the environment has other replicas
> SET cluster_replica = r1

> CREATE TABLE t (a int)

> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t

# Note: We rely on testdrive's retry behavior here, as it takes some time for
# the logging to catch up.

> SELECT COUNT(*)
  FROM
    mz_materialized_views AS views,
    mz_introspection.mz_compute_exports AS compute_exports,
    mz_introspection.mz_compute_frontiers AS frontiers
  WHERE
    views.name = 'mv' AND
    views.id = compute_exports.export_id AND
    compute_exports.export_id = frontiers.export_id AND
    time > 0
1

> DROP CLUSTER test CASCADE;
