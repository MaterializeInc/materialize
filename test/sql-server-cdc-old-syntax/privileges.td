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

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_privileges_connection_table TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER 'authorization_user_table_perms',
    PASSWORD = SECRET sql_server_pass
  );

! CREATE SOURCE t1_privileges_sql_server_failure
  FROM SQL SERVER CONNECTION sql_server_privileges_connection_table
  FOR TABLES (dbo.t1_privileges);
contains:reference to dbo.t1_privileges not found in source

> CREATE CONNECTION sql_server_privileges_connection_column TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER 'authorization_user_column_perms',
    PASSWORD = SECRET sql_server_pass
  );

# Column exclusion has no effect on rejection for column level permissions.
! CREATE SOURCE t1_privileges_sql_server_failure
  FROM SQL SERVER CONNECTION sql_server_privileges_connection_column (
    EXCLUDE COLUMNS (dbo.t1_privileges.c2)
  )
  FOR TABLES (dbo.t1_privileges);
contains:insufficient permissions for tables [dbo.t1_privileges] or capture instances []

> CREATE CONNECTION sql_server_privileges_connection_capture_instance TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER 'authorization_user_column_perms_capture_instance',
    PASSWORD = SECRET sql_server_pass
  );

! CREATE SOURCE t1_privileges_sql_server_failure
  FROM SQL SERVER CONNECTION sql_server_privileges_connection_capture_instance
  FOR TABLES (dbo.t1_privileges);
contains:insufficient permissions for tables [] or capture instances [dbo_t1_privileges]