# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/database-issues/issues/4315

> CREATE CLUSTER c REPLICAS (r (SIZE '2-2'))

$ set-from-sql var=now
SELECT mz_now()::text

# Note: We rely on testdrive's retry behavior here, as it takes some time for
# the first frontiers message to arrive.

> SELECT bool_and(f.write_frontier > ${now})
  FROM mz_cluster_replicas r, mz_cluster_replica_frontiers f
  WHERE r.id = f.replica_id AND r.name = 'r'
true
