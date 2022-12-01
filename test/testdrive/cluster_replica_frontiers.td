# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test mz_internal.cluster_replica_frontiers

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

> CREATE TABLE t1 (a int)

> INSERT INTO t1 VALUES (1)

> CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t1

# Make sure compute controller has the frontier
> SELECT * FROM mv1
1

> SELECT mz_clusters.name, mz_cluster_replicas.name FROM
  mz_catalog.mz_objects AS obj, mz_internal.mz_cluster_replica_frontiers AS frontiers,
  mz_cluster_replicas, mz_clusters
  WHERE obj.name = 'mv1' AND frontiers.export_id = obj.id AND
  replica_id = mz_cluster_replicas.id AND cluster_id = mz_clusters.id
  ORDER BY mz_cluster_replicas.name;
default r1

> CREATE CLUSTER REPLICA default.r2 SIZE '2';

> SELECT mz_clusters.name, mz_cluster_replicas.name, time > 0 FROM
  mz_catalog.mz_objects AS obj, mz_internal.mz_cluster_replica_frontiers AS frontiers,
  mz_cluster_replicas, mz_clusters
  WHERE obj.name = 'mv1' AND frontiers.export_id = obj.id AND
  replica_id = mz_cluster_replicas.id AND cluster_id = mz_clusters.id
  ORDER BY mz_cluster_replicas.name;
default r1 true
default r2 true


> DROP CLUSTER REPLICA default.r2;

> SELECT mz_clusters.name, mz_cluster_replicas.name FROM
  mz_catalog.mz_objects AS obj, mz_internal.mz_cluster_replica_frontiers AS frontiers,
  mz_cluster_replicas, mz_clusters
  WHERE obj.name = 'mv1' AND frontiers.export_id = obj.id AND
  replica_id = mz_cluster_replicas.id AND cluster_id = mz_clusters.id
  ORDER BY mz_cluster_replicas.name;
default r1
