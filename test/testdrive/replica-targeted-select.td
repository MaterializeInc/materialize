# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that replica-targeted selects return results from the target replica.
#
# This test works by querying introspection sources, which are the only compute
# collections that can have different contents between replicas. Specifically,
# we look at the distinct `worker_id`s present in `mz_worker_compute_frontiers`
# for replicas with different worker counts. We know that
# `mz_worker_compute_frontiers` always contains entries, because it tracks the
# frontiers of introspection sources as well.
#
# This test relies on testdrive's retry feature, as introspection data might
# not be immediately available.

> CREATE CLUSTER test REPLICAS (
      r1 (SIZE '1'),
      r2 (SIZE '2'),
      r4 (SIZE '4')
  )

> SET cluster = test

> SET cluster_replica = r1

> SELECT DISTINCT worker_id
  FROM mz_internal.mz_worker_compute_frontiers
  ORDER BY worker_id
0

> SET cluster_replica = r2

> SELECT DISTINCT worker_id
  FROM mz_internal.mz_worker_compute_frontiers
  ORDER BY worker_id
0
1

> SET cluster_replica = r4

> SELECT DISTINCT worker_id
  FROM mz_internal.mz_worker_compute_frontiers
  ORDER BY worker_id
0
1
2
3

# Clean up.
> DROP CLUSTER test
