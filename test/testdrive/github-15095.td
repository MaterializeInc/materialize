# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/15095

> CREATE CLUSTER c REPLICAS (r (SIZE '2-2'))

# Note: We rely on testdrive's retry behavior here, as it takes some time for
# the first replica heartbeat to arrive.

> SELECT count(*)
  FROM mz_cluster_replicas r, mz_internal.mz_cluster_replica_heartbeats h
  WHERE r.id = h.replica_id AND r.name = 'r'
1
