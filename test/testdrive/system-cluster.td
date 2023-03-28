# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

> SHOW CLUSTERS
mz_system
mz_introspection
default

! DROP CLUSTER mz_system CASCADE
contains:system cluster 'mz_system' cannot be modified

! DROP CLUSTER mz_introspection CASCADE
contains:system cluster 'mz_introspection' cannot be modified

! DROP CLUSTER mz_joe CASCADE
contains:unknown cluster 'mz_joe'

! CREATE CLUSTER mz_joe REPLICAS (r1 (size '1'))
contains:cluster name "mz_joe" is reserved

! CREATE CLUSTER REPLICA mz_system.r2 SIZE '1';
contains:system cluster 'mz_system' cannot be modified

! DROP CLUSTER REPLICA mz_system.r1
contains:system cluster 'mz_system' cannot be modified

> CREATE MATERIALIZED VIEW mv AS SELECT AVG(50)

> SET CLUSTER TO mz_introspection

> SHOW MATERIALIZED VIEWS
mv default

! CREATE MATERIALIZED VIEW mv1 AS SELECT MIN(1)
contains:system cluster 'mz_introspection' cannot be modified

> SET CLUSTER TO mz_system

# Query gets automatically run on mz_introspection, despite mz_system being set
> SHOW MATERIALIZED VIEWS
mv default

! CREATE MATERIALIZED VIEW mv1 AS SELECT MIN(1)
contains:system cluster 'mz_system' cannot be modified

> SET CLUSTER TO default

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
mz_system r1 1 <TRUE_OR_FALSE>

$ postgres-execute connection=mz_system
DROP CLUSTER REPLICA mz_system.r1

> SHOW CLUSTER REPLICAS WHERE cluster = 'mz_system'

$ postgres-execute connection=mz_system
CREATE CLUSTER REPLICA mz_system.r1 SIZE '${arg.default-replica-size}';

> SHOW CLUSTER REPLICAS WHERE cluster = 'mz_system'
mz_system r1 ${arg.default-replica-size} <TRUE_OR_FALSE>

$ unset-regex
