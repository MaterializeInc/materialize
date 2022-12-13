# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CLUSTER foo REPLICAS (size_1 (SIZE '1'), size_32 (SIZE '32'), size_2_2 (SIZE '2-2'))
> CREATE CLUSTER bar REPLICAS ()
> CREATE CLUSTER xyzzy REPLICAS (size_4 (SIZE '4'))

> SELECT c.name, r.name, u.process_id, u.cpu_percent, u.memory_percent
  FROM mz_clusters c
  JOIN mz_cluster_replicas r ON r.cluster_id = c.id
  JOIN mz_internal.mz_cluster_replica_utilization u ON r.id = u.replica_id
  WHERE c.name IN ('foo', 'bar', 'xyzzy')
  ORDER BY c.name, r.name, process_id
foo size_1 0 0 0
foo size_2_2 0 0 0
foo size_2_2 1 0 0
foo size_32 0 0 0
xyzzy size_4 0 0 0

> DROP CLUSTER foo
> DROP CLUSTER bar
> DROP CLUSTER xyzzy
