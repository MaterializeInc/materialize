# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Setup SQL Server state.
#
# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1_privileges (c1 int, c2 int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1_privileges', @role_name = 'SA', @supports_net_changes = 0;

CREATE LOGIN authorization_user_column_perms WITH PASSWORD = '${arg.default-sql-server-password}';
CREATE USER authorization_user_column_perms FOR LOGIN authorization_user_column_perms;
ALTER ROLE db_datareader ADD MEMBER authorization_user_column_perms;

DENY SELECT ON OBJECT::dbo.t1_privileges(c2) TO authorization_user_column_perms;

CREATE LOGIN authorization_user_column_perms_capture_instance WITH PASSWORD = '${arg.default-sql-server-password}';
CREATE USER authorization_user_column_perms_capture_instance FOR LOGIN authorization_user_column_perms_capture_instance;
ALTER ROLE db_datareader ADD MEMBER authorization_user_column_perms_capture_instance;

DENY SELECT ON OBJECT::cdc.dbo_t1_privileges_ct(__$start_lsn) TO authorization_user_column_perms_capture_instance;

CREATE LOGIN authorization_user_table_perms WITH PASSWORD = '${arg.default-sql-server-password}';
CREATE USER authorization_user_table_perms FOR LOGIN authorization_user_table_perms;
ALTER ROLE db_datareader ADD MEMBER authorization_user_table_perms;
DENY SELECT ON OBJECT::dbo.t1_privileges TO authorization_user_table_perms;

USE master;
GRANT VIEW SERVER STATE TO authorization_user_column_perms;
GRANT VIEW SERVER STATE TO authorization_user_column_perms_capture_instance;
GRANT VIEW SERVER STATE TO authorization_user_table_perms;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_privileges_connection_table TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER 'authorization_user_table_perms',
    PASSWORD = SECRET sql_server_pass
  );

# The reference is resolved when the table is created, not when the source is,
# so the privilege errors surface on `CREATE TABLE .. FROM SOURCE`.
> CREATE SOURCE t1_privileges_src_table
  FROM SQL SERVER CONNECTION sql_server_privileges_connection_table;

! CREATE TABLE t1_privileges_failure FROM SOURCE t1_privileges_src_table (REFERENCE dbo.t1_privileges);
contains:reference to dbo.t1_privileges not found in source

> CREATE CONNECTION sql_server_privileges_connection_column TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER 'authorization_user_column_perms',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE t1_privileges_src_column
  FROM SQL SERVER CONNECTION sql_server_privileges_connection_column;

# Column exclusion has no effect on rejection for column level permissions.
! CREATE TABLE t1_privileges_failure FROM SOURCE t1_privileges_src_column (REFERENCE dbo.t1_privileges)
  WITH (EXCLUDE COLUMNS (c2));
contains:insufficient permissions for tables [dbo.t1_privileges] or capture instances []

> CREATE CONNECTION sql_server_privileges_connection_capture_instance TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER 'authorization_user_column_perms_capture_instance',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE t1_privileges_src_capture_instance
  FROM SQL SERVER CONNECTION sql_server_privileges_connection_capture_instance;

! CREATE TABLE t1_privileges_failure FROM SOURCE t1_privileges_src_capture_instance (REFERENCE dbo.t1_privileges);
contains:insufficient permissions for tables [] or capture instances [dbo_t1_privileges]

# A user who loses SELECT on a source table after the Materialize table was
# created must stall the source with a transient error, not error the table
# permanently, so that restoring the permission resumes ingestion.
#
# The generated change function returns one row with every column NULL when the
# caller fails its permission check, which covers a user outside the capture
# instance's gating role as well as a member who lost SELECT on the table. The
# change read fails on that row, so the source stalls before anything else in
# the poll runs. The user joins the gating role so only the denied SELECT is
# under test.
$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t2_privileges (id INT NOT NULL CONSTRAINT t2_privileges_pk PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT t2_privileges_uq UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2_privileges', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t2_privileges VALUES (1, 'a');
CREATE LOGIN authorization_user_revoked WITH PASSWORD = '${arg.default-sql-server-password}';
CREATE USER authorization_user_revoked FOR LOGIN authorization_user_revoked;
ALTER ROLE db_datareader ADD MEMBER authorization_user_revoked;
ALTER ROLE SA ADD MEMBER authorization_user_revoked;
USE master;
GRANT VIEW SERVER STATE TO authorization_user_revoked;

> CREATE CONNECTION sql_server_privileges_connection_revoked TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER 'authorization_user_revoked',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE t2_privileges_src
  FROM SQL SERVER CONNECTION sql_server_privileges_connection_revoked;

> CREATE TABLE t2_privileges FROM SOURCE t2_privileges_src (REFERENCE dbo.t2_privileges);

> SELECT * FROM t2_privileges;
1 a

$ sql-server-execute name=sql-server
USE test;
DENY SELECT ON OBJECT::dbo.t2_privileges TO authorization_user_revoked;

# Revoked access does not surface as an empty result that would need its own
# handling: the change read errors, and the source intentionally enters a retry
# loop until access is restored.
> SELECT status, error ILIKE '%required column ''__$start_lsn'': got null value%'
  FROM mz_internal.mz_source_statuses
  WHERE name = 't2_privileges_src';
stalled true

$ sql-server-execute name=sql-server
USE test;
REVOKE SELECT ON OBJECT::dbo.t2_privileges TO authorization_user_revoked;
INSERT INTO t2_privileges VALUES (2, 'b');

> SELECT * FROM t2_privileges;
1 a
2 b

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 't2_privileges_src';
running
