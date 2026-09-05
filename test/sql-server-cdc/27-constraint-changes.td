# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Dropping a UNIQUE constraint upstream invalidates the key that Materialize
# recorded for the table at purification time. The optimizer trusts that key for
# distinct elimination and join cardinality, so continuing to ingest rows that no
# longer satisfy it produces silently wrong results. The source must instead
# report an incompatible schema change.
#
# NOTE: the constraint backing the index a capture instance was enabled on cannot
# be dropped at all. SQL Server rejects that with error 22979, so a table whose
# only key is its PRIMARY KEY is protected upstream. A secondary UNIQUE
# constraint is not, which is what this test exercises.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_27_connection CASCADE
> CREATE CONNECTION sql_server_test_27_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

$ sql-server-execute name=sql-server
CREATE TABLE t27_keys (f1 INT NOT NULL CONSTRAINT t27_pk_key PRIMARY KEY, f2 INT NOT NULL CONSTRAINT t27_unique_key UNIQUE, f3 INT);
INSERT INTO t27_keys VALUES (1, 10, 100), (2, 20, 200);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't27_keys', @role_name = 'SA', @supports_net_changes = 0;

> BEGIN;
> CREATE SOURCE t27_source FROM SQL SERVER CONNECTION sql_server_test_27_connection;
> CREATE TABLE t27_keys FROM SOURCE t27_source (REFERENCE t27_keys);
> COMMIT;

> SELECT create_sql LIKE '%PRIMARY KEY ("f1")%' FROM mz_tables WHERE name = 't27_keys';
true

> SELECT create_sql LIKE '%UNIQUE NULLS NOT DISTINCT ("f2")%' FROM mz_tables WHERE name = 't27_keys';
true

> SELECT * FROM t27_keys;
1 10 100
2 20 200

# Drop the upstream UNIQUE constraint and write a row that it would have
# rejected. `f2` is no longer a key, so `SELECT DISTINCT f2` can no longer be
# answered by reading `f2` alone.

$ sql-server-execute name=sql-server
ALTER TABLE t27_keys DROP CONSTRAINT t27_unique_key;
INSERT INTO t27_keys VALUES (3, 10, 300);

# ---- This is invalid!!! ----
> SELECT DISTINCT f2 FROM t27_keys ORDER BY f2;
10
10
20

! SELECT * FROM t27_keys;
contains:Incompatible schema change

> DROP SOURCE t27_source CASCADE;
