# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-sql-addr-internal}

! ALTER SYSTEM SET max_tables TO 42
contains:unauthorized: only user 'mz_system' is allowed to execute 'ALTER SYSTEM ...'

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_tables = 2

> SHOW max_tables
2

> CREATE TABLE t1 (a INT)

> CREATE TABLE t2 (a INT)

! CREATE TABLE t3 (a INT)
contains:Table resource limit of 2 cannot be exceeded. Current amount is 2.

> DROP TABLE t2;

> CREATE TABLE t3 (a INT)

! CREATE TABLE t4 (a INT)
contains:Table resource limit of 2 cannot be exceeded. Current amount is 2.


$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_tables = 25

> SHOW max_tables
25

> CREATE TABLE t4 (a INT)

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_objects_per_schema = 2

> SHOW max_objects_per_schema
2

! CREATE TABLE t4 (a INT)
contains:Objects per schema resource limit of 2 cannot be exceeded. Current amount is 3.

! CREATE VIEW v as SELECT 1
contains:Objects per schema resource limit of 2 cannot be exceeded. Current amount is 3.

! CREATE INDEX ind on t1 (a)
contains:Objects per schema resource limit of 2 cannot be exceeded. Current amount is 3.

! CREATE TYPE t AS (a float8)
contains:Objects per schema resource limit of 2 cannot be exceeded. Current amount is 3.

! CREATE TABLE t4 (a INT)
contains:Objects per schema resource limit of 2 cannot be exceeded. Current amount is 3.

> CREATE SCHEMA s1;

> CREATE TABLE s1.t4 (a INT)

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_objects_per_schema = 20

> SHOW max_objects_per_schema
20

> CREATE VIEW v as SELECT 1

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_clusters = 3

> SHOW max_clusters
3

> CREATE CLUSTER c1 REPLICAS (r1 (size '1'))

> CREATE CLUSTER c2 REPLICAS (r (SIZE '1'));

! CREATE CLUSTER c3 REPLICAS (r (SIZE '1'));
contains:Cluster resource limit of 3 cannot be exceeded. Current amount is 3.

> DROP CLUSTER c2 CASCADE;

> CREATE CLUSTER c3 REPLICAS (r (SIZE '1'));

! CREATE CLUSTER c4 REPLICAS (r (SIZE '1'));
contains:Cluster resource limit of 3 cannot be exceeded. Current amount is 3.


$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_clusters = 4

> SHOW max_clusters
4

> CREATE CLUSTER c4 REPLICAS (r (SIZE '1'));

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_replicas_per_cluster = 1

> SHOW max_replicas_per_cluster
1

! CREATE CLUSTER REPLICA c1.r2 SIZE '1'
contains:Replicas per cluster resource limit of 1 cannot be exceeded. Current amount is 1.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_replicas_per_cluster = 100

> SHOW max_replicas_per_cluster
100

#> CREATE CLUSTER REPLICA c1.r2 SIZE '1'

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_databases = 1

> SHOW max_databases
1

! CREATE DATABASE d1
contains:Database resource limit of 1 cannot be exceeded. Current amount is 1.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_databases = 42

> SHOW max_databases
42

> CREATE DATABASE d1

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_schemas_per_database = 2

> SHOW max_schemas_per_database
2

! CREATE SCHEMA s2
contains:Schemas per database resource limit of 2 cannot be exceeded. Current amount is 2.

> CREATE SCHEMA d1.s2

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_schemas_per_database = 3

> SHOW max_schemas_per_database
3

> CREATE SCHEMA s2

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_roles = 2;

> SHOW max_roles
2

> CREATE ROLE joe LOGIN SUPERUSER

! CREATE ROLE mike LOGIN SUPERUSER
contains:Role resource limit of 2 cannot be exceeded. Current amount is 2.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_roles = 3;

> SHOW max_roles
3

> CREATE ROLE mike LOGIN SUPERUSER

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_secrets = 1

> SHOW max_secrets
1

> CREATE SECRET secret AS 'secure_password'

! CREATE SECRET password AS 'pass'
contains:Secret resource limit of 1 cannot be exceeded. Current amount is 1.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_secrets = 50000

> SHOW max_secrets
50000

> CREATE SECRET password AS 'pass'

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_materialized_views = 1

> SHOW max_materialized_views
1

> CREATE MATERIALIZED VIEW mv1 AS SELECT 1

! CREATE MATERIALIZED VIEW mv2 AS SELECT 2
contains:Materialized view resource limit of 1 cannot be exceeded. Current amount is 1.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_materialized_views = 2

> SHOW max_materialized_views
2

> CREATE MATERIALIZED VIEW mv2 AS SELECT 2

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_sources = 667

> SHOW max_sources
667

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_sinks = 44

> SHOW max_sinks
44

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_tables = 0

> DROP TABLE t1

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET max_tables

> SHOW max_tables
25

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET ALL

> SHOW max_databases
1000

> SHOW max_schemas_per_database
1000

> DROP CLUSTER c1 CASCADE

> DROP CLUSTER c3 CASCADE

> DROP CLUSTER c4 CASCADE
