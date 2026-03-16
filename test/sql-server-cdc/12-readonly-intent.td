# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the READONLY INTENT connection option for SQL Server connections.

# Setup SQL Server state.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t_readonly (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't_readonly', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t_readonly VALUES ('a', 'hello'), ('b', 'world');

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

# Test creating a connection with READONLY INTENT.
# On a standalone SQL Server (no Availability Group), the ReadOnly intent is
# accepted but ignored — both reads and writes succeed. This test validates
# that the option is properly threaded through parsing, planning, and connection
# establishment.

> CREATE CONNECTION readonly_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass,
    READONLY INTENT = true
  );

> VALIDATE CONNECTION readonly_conn;

> SELECT name, type from mz_connections WHERE name = 'readonly_conn';
name            type
--------------------------
readonly_conn   sql-server

# Verify we can create a source and read data through a READONLY INTENT connection.

> CREATE SOURCE readonly_source
  FROM SQL SERVER CONNECTION readonly_conn
  FOR TABLES (dbo.t_readonly);

# On a standalone server (not in an AG), is_primary_replica should be NULL.
> SELECT is_primary_replica FROM mz_internal.mz_sql_server_sources
  WHERE id = (SELECT id FROM mz_sources WHERE name = 'readonly_source');
<null>

> SELECT * FROM t_readonly;
a hello
b world

# Verify replication works — insert new data and check it propagates.
$ sql-server-execute name=sql-server
INSERT INTO t_readonly VALUES ('c', 'readonly test');

> SELECT * FROM t_readonly;
a hello
b world
c "readonly test"

# Clean up.
> DROP SOURCE readonly_source CASCADE;
> DROP CONNECTION readonly_conn;
