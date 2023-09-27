# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test moving various objects between clusters

> CREATE TABLE alter_mv_set_cluster_table (f1 INTEGER);

> CREATE CLUSTER alter_mv_set_cluster_cluster1 REPLICAS (replica1 (SIZE '1'));

> CREATE MATERIALIZED VIEW alter_mv_set_cluster_view1 AS SELECT f1 + 1 AS f1 FROM alter_mv_set_cluster_table;
> CREATE DEFAULT INDEX ON alter_mv_set_cluster_view1;
> INSERT INTO alter_mv_set_cluster_table VALUES (1);
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_alter_set_cluster = true

> CREATE MATERIALIZED VIEW alter_mv_set_cluster_view2 AS SELECT f1 + 1 AS f1 FROM alter_mv_set_cluster_view1;

> CREATE DEFAULT INDEX ON alter_mv_set_cluster_view2;

> ALTER MATERIALIZED VIEW alter_mv_set_cluster_view1 SET CLUSTER alter_mv_set_cluster_cluster1;

> INSERT INTO alter_mv_set_cluster_table VALUES (2);

> CREATE CLUSTER alter_mv_set_cluster_cluster2 REPLICAS (replica1 (SIZE '1'));

> CREATE MATERIALIZED VIEW alter_mv_set_cluster_view3 AS SELECT f1 + 1 AS f1 FROM alter_mv_set_cluster_view2;

> CREATE DEFAULT INDEX ON alter_mv_set_cluster_view3;

> ALTER MATERIALIZED VIEW alter_mv_set_cluster_view1 SET CLUSTER alter_mv_set_cluster_cluster2;

> ALTER MATERIALIZED VIEW alter_mv_set_cluster_view2 SET CLUSTER alter_mv_set_cluster_cluster2;

# TODO(#21956): Remove once fixed
> DROP MATERIALIZED VIEW alter_mv_set_cluster_view3
> DROP MATERIALIZED VIEW alter_mv_set_cluster_view2
> DROP MATERIALIZED VIEW alter_mv_set_cluster_view1;

# Cleanup
# TODO(#21956): Add CASCADE once fixed and explicit drop removed
> DROP CLUSTER alter_mv_set_cluster_cluster2, alter_mv_set_cluster_cluster1
