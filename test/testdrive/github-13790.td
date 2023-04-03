# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/13790

# The contents of the introspection tables depend on the replica size
$ skip-if
SELECT '${arg.default-replica-size}' != '4-4'

# In case the environment has other replicas
> SET cluster_replica = r1

> CREATE TABLE t (a int)

> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t

# Note: We rely on testdrive's retry behavior here, as it takes some time for
# the logging to catch up.

> SELECT COUNT(*)
  FROM
    mz_materialized_views AS views,
    mz_internal.mz_compute_exports_per_worker AS compute_exports,
    mz_internal.mz_compute_frontiers_per_worker AS frontiers
  WHERE
    views.name = 'mv' AND
    views.id = compute_exports.export_id AND
    compute_exports.export_id = frontiers.export_id AND
    time > 0
16
