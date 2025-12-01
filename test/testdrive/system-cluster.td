# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default replicas=1
$ set-arg-default default-replica-size=scale=1,workers=1

# The expected number of rows in system tables depends on the number of replicas
$ skip-if
SELECT ${arg.replicas} > 1;

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
> SELECT name FROM (SHOW CLUSTERS)
mz_analytics
mz_system
mz_catalog_server
quickstart
mz_probe
mz_support

$ postgres-execute connection=mz_system
> select managed from mz_catalog.mz_clusters where id like 's%';
true
true
true
true
true

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_rbac_checks TO false

! DROP CLUSTER mz_system CASCADE
contains:must be owner of CLUSTER mz_system

! DROP CLUSTER mz_catalog_server CASCADE
contains:must be owner of CLUSTER mz_catalog_server

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_rbac_checks TO true

! DROP CLUSTER mz_joe CASCADE
contains:unknown cluster 'mz_joe'

! CREATE CLUSTER mz_joe REPLICAS (r1 (SIZE 'scale=1,workers=1'))
contains:cluster name "mz_joe" is reserved

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_rbac_checks TO false

! CREATE CLUSTER REPLICA mz_system.r2 SIZE 'scale=1,workers=1';
contains: cannot modify managed cluster mz_system

! DROP CLUSTER REPLICA mz_system.r1;
contains:must be owner of CLUSTER REPLICA mz_system.r1

! ALTER CLUSTER mz_system SET (SIZE='2');
contains: must be owner of CLUSTER mz_system

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_rbac_checks TO true

> CREATE MATERIALIZED VIEW mv AS SELECT AVG(50)

> SET CLUSTER TO mz_catalog_server

> SHOW MATERIALIZED VIEWS
mv quickstart "" <null>

#! CREATE MATERIALIZED VIEW mv1 AS SELECT MIN(1)
#contains:system cluster 'mz_catalog_server' cannot be modified

> SET CLUSTER TO mz_system

# Query gets automatically run on mz_catalog_server, despite mz_system being set
> SHOW MATERIALIZED VIEWS
mv quickstart "" <null>

#! CREATE MATERIALIZED VIEW mv1 AS SELECT MIN(1)
#contains:must be owner of CLUSTER mz_system

> SET CLUSTER TO quickstart

> CREATE TABLE temp (a INT)

> INSERT INTO temp VALUES (1), (2)

$ postgres-execute connection=mz_system
SET CLUSTER TO mz_system
INSERT INTO temp SELECT * FROM temp

> SELECT * FROM temp
1
2
1
2

# Ready is false in the process orchestrator, but true in K8s
$ set-regex match=true|false replacement=<TRUE_OR_FALSE>

> SHOW CLUSTER REPLICAS WHERE cluster = 'mz_system'
mz_system r1 bootstrap <TRUE_OR_FALSE> ""

> SELECT COUNT(*) FROM mz_catalog.mz_cluster_replicas as r, mz_catalog.mz_clusters as c WHERE r.cluster_id = c.id and c.name = 'mz_support';
0

! ALTER CLUSTER mz_support SET (replication factor 1, size 'scale=1,workers=2');
contains: must be owner of CLUSTER mz_support

$ postgres-execute connection=postgres://mz_support@${testdrive.materialize-internal-sql-addr}/materializeI
ALTER CLUSTER mz_support SET (replication factor 0, SIZE 'scale=1,workers=1');

$ unset-regex

> select name, introspection_debugging from mz_catalog.mz_clusters where name IN ('quickstart', 'mz_support');
name        introspection_debugging
------------------------
quickstart  false
mz_support  false

! ALTER CLUSTER quickstart SET (introspection debugging = TRUE);
contains: must be owner of CLUSTER quickstart

! ALTER CLUSTER mz_support SET (introspection debugging = TRUE);
contains: must be owner of CLUSTER mz_support

$ postgres-execute connection=postgres://mz_support@${testdrive.materialize-internal-sql-addr}/materializeI
ALTER CLUSTER mz_support SET (introspection debugging = TRUE);

> select name, introspection_debugging from mz_catalog.mz_clusters where name IN ('quickstart', 'mz_support');
quickstart false
mz_support true

$ postgres-execute connection=postgres://mz_support@${testdrive.materialize-internal-sql-addr}/materializeI
ALTER CLUSTER mz_support SET (introspection interval = '10ms');

> select name, introspection_debugging, introspection_interval from mz_catalog.mz_clusters where name = 'mz_support';
mz_support true "00:00:00.01"

$ postgres-execute connection=postgres://mz_support@${testdrive.materialize-internal-sql-addr}/materializeI
ALTER CLUSTER mz_support SET (introspection interval = '0s');

> select name, introspection_debugging, introspection_interval from mz_catalog.mz_clusters where name = 'mz_support';
mz_support true <null>
