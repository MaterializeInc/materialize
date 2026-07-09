# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Core CDC happy path against Azure SQL Database: TLS, snapshot + streaming, and
# capture-instance selection. This is the end-to-end exercise of the
# engine-edition gating, since a successful CREATE SOURCE requires the SQL
# Server Agent check to be skipped for Azure SQL Database.

$ sql-server-connect name=sql-server
server=tcp:${arg.azure-sql-host},${arg.azure-sql-port};Encrypt=true;TrustServerCertificate=true;User ID={${arg.azure-sql-user}};Password={${arg.azure-sql-password}};Database=${arg.azure-sql-database}

$ sql-server-execute name=sql-server
CREATE TABLE mz_azure_t1 (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'mz_azure_t1', @role_name = NULL, @supports_net_changes = 0;
INSERT INTO mz_azure_t1 VALUES ('a', 'hello world'), ('b', 'foobar'), ('c', 'anotha one');

> CREATE SECRET IF NOT EXISTS azure_sql_pass AS '${arg.azure-sql-password}'

# Azure SQL Database mandates encryption. Validate the trust-all path...
> CREATE CONNECTION azure_conn TO SQL SERVER (
    HOST '${arg.azure-sql-host}',
    PORT ${arg.azure-sql-port},
    DATABASE '${arg.azure-sql-database}',
    USER '${arg.azure-sql-user}',
    PASSWORD = SECRET azure_sql_pass,
    SSL MODE required
  );

> VALIDATE CONNECTION azure_conn;

# ...and the path that verifies Azure's certificate against the system CA roots.
> CREATE CONNECTION azure_conn_verify TO SQL SERVER (
    HOST '${arg.azure-sql-host}',
    PORT ${arg.azure-sql-port},
    DATABASE '${arg.azure-sql-database}',
    USER '${arg.azure-sql-user}',
    PASSWORD = SECRET azure_sql_pass,
    SSL MODE verify
  );

> VALIDATE CONNECTION azure_conn_verify;

> CREATE SOURCE mz_azure_src
  FROM SQL SERVER CONNECTION azure_conn;

> CREATE TABLE mz_azure_t1 FROM SOURCE mz_azure_src (REFERENCE dbo.mz_azure_t1);

> SELECT * FROM mz_azure_t1;
a "hello world"
b foobar
c "anotha one"

# Streaming: insert, update, and delete should all propagate.
$ sql-server-execute name=sql-server
INSERT INTO mz_azure_t1 VALUES ('d', 'streamed');
UPDATE mz_azure_t1 SET val_col = 'updated' WHERE key_col = 'a';
DELETE FROM mz_azure_t1 WHERE key_col = 'b';

> SELECT * FROM mz_azure_t1;
a updated
c "anotha one"
d streamed

# Capture-instance selection: when a table has multiple capture instances,
# Materialize selects the one with the most recent create_date. We drop a column
# before creating the second (newer) instance, so picking the newest is
# observable in the projected schema.
$ sql-server-execute name=sql-server
CREATE TABLE mz_azure_ci (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));
INSERT INTO mz_azure_ci VALUES ('a', 'first'), ('b', 'second');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'mz_azure_ci', @role_name = NULL, @supports_net_changes = 0, @capture_instance = 'zebra';
ALTER TABLE mz_azure_ci DROP COLUMN val_col;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'mz_azure_ci', @role_name = NULL, @supports_net_changes = 0, @capture_instance = 'apple';

> CREATE SOURCE mz_azure_ci_src
  FROM SQL SERVER CONNECTION azure_conn;

> CREATE TABLE mz_azure_ci FROM SOURCE mz_azure_ci_src (REFERENCE dbo.mz_azure_ci);

# The newest instance ('apple') reflects the dropped column, so only key_col
# is present.
> SELECT * FROM mz_azure_ci;
a
b
