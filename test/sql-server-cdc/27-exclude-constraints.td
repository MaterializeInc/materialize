# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# EXCLUDE CONSTRAINTS / EXCLUDE ALL CONSTRAINTS: excluded upstream constraints
# are not recorded as Materialize keys. NOTE: SQL Server does not allow
# dropping a PRIMARY KEY while CDC is enabled, and upstream ALTER COLUMN still
# errors the table regardless of these options, so this test covers key
# recording and nullability only.
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = true

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
CREATE TABLE t27_uniq (id INT NOT NULL, wallet VARCHAR(64) NOT NULL, CONSTRAINT t27_pk PRIMARY KEY (id), CONSTRAINT t27_uq_wallet UNIQUE (wallet));
INSERT INTO t27_uniq VALUES (1, 'a'), (2, 'b');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't27_uniq', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE t27_all (id INT NOT NULL, email VARCHAR(64) NOT NULL, CONSTRAINT t27_all_pk PRIMARY KEY (id));
INSERT INTO t27_all VALUES (1, 'a@b.c');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't27_all', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SOURCE t27_source FROM SQL SERVER CONNECTION sql_server_test_27_connection;

#
# Validation errors
#

! CREATE TABLE t27_uniq FROM SOURCE t27_source (REFERENCE t27_uniq)
  WITH (EXCLUDE CONSTRAINTS ('nope'));
contains:EXCLUDE CONSTRAINTS refers to constraints that do not exist on table dbo.t27_uniq

! CREATE TABLE t27_uniq FROM SOURCE t27_source (REFERENCE t27_uniq)
  WITH (EXCLUDE CONSTRAINTS ('t27_uq_wallet'), EXCLUDE ALL CONSTRAINTS);
contains:EXCLUDE ALL CONSTRAINTS cannot be combined with EXCLUDE CONSTRAINTS

#
# Excluding a named UNIQUE constraint: the PRIMARY KEY survives, the excluded
# constraint is not recorded as a key.
#

> CREATE TABLE t27_uniq FROM SOURCE t27_source (REFERENCE t27_uniq)
  WITH (EXCLUDE CONSTRAINTS ('t27_uq_wallet'));

> SELECT * FROM t27_uniq;
1 a
2 b

> CREATE DEFAULT INDEX ON t27_uniq;
> SELECT key FROM (SHOW INDEXES ON t27_uniq);
{id}

> SELECT create_sql LIKE '%EXCLUDE CONSTRAINTS%t27_uq_wallet%' FROM (SHOW CREATE TABLE t27_uniq);
true

#
# EXCLUDE ALL CONSTRAINTS: no keys, every column nullable.
#

> CREATE TABLE t27_all FROM SOURCE t27_source (REFERENCE t27_all)
  WITH (EXCLUDE ALL CONSTRAINTS);

> CREATE DEFAULT INDEX ON t27_all;
> SELECT key FROM (SHOW INDEXES ON t27_all);
{id,email}

> SELECT name, nullable FROM mz_columns WHERE id = (SELECT id FROM mz_tables WHERE name = 't27_all');
id true
email true

> SELECT * FROM t27_all;
1 a@b.c

# Replication still works on both tables.
$ sql-server-execute name=sql-server
INSERT INTO t27_uniq VALUES (3, 'c');
INSERT INTO t27_all VALUES (2, 'd@e.f');

> SELECT * FROM t27_uniq;
1 a
2 b
3 c

> SELECT * FROM t27_all;
1 a@b.c
2 d@e.f

#
# An empty exclusion list behaves as if the option were omitted.
#

> CREATE TABLE t27_uniq_empty FROM SOURCE t27_source (REFERENCE t27_uniq)
  WITH (EXCLUDE CONSTRAINTS ());

> CREATE DEFAULT INDEX ON t27_uniq_empty;
> SELECT key FROM (SHOW INDEXES ON t27_uniq_empty);
{id}

> DROP TABLE t27_uniq_empty;

#
# Feature flag: the options are rejected when disabled.
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = false

! CREATE TABLE t27_flagged FROM SOURCE t27_source (REFERENCE t27_uniq)
  WITH (EXCLUDE ALL CONSTRAINTS);
contains:not available

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = true

> DROP SOURCE t27_source CASCADE;
