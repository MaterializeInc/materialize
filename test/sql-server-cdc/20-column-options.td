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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
DROP DATABASE IF EXISTS test_column_options;
CREATE DATABASE test_column_options;
USE test_column_options;

EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test_column_options SET ALLOW_SNAPSHOT_ISOLATION ON;

CREATE TABLE t1_columns (c1 decimal(20, 10), c2 time, c3 money, c4 varbinary(100));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1_columns', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1_columns VALUES (1.444889, '12:00:00', '$100.99', 0x1100AB);

CREATE TABLE t2_columns (c1 varchar(256));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2_columns', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t2_columns VALUES ('invisible');

CREATE TABLE dummy (data int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dummy', @role_name = 'SA', @supports_net_changes = 0;


> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_columns_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test_column_options,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE t1_columns_sql_server
  FROM SQL SERVER CONNECTION sql_server_columns_connection (
    TEXT COLUMNS (dbo.t1_columns.c1, dbo.t1_columns.c2, dbo.t1_columns.c4),
    EXCLUDE COLUMNS (dbo.t1_columns.c3)
  )
  FOR TABLES (dbo.t1_columns);

> SHOW CREATE SOURCE t1_columns_sql_server;
materialize.public.t1_columns_sql_server "CREATE SOURCE materialize.public.t1_columns_sql_server\nIN CLUSTER quickstart\nFROM\n    SQL SERVER CONNECTION materialize.public.sql_server_columns_connection (TEXT COLUMNS = (dbo.t1_columns.c1, dbo.t1_columns.c2, dbo.t1_columns.c4), EXCLUDE COLUMNS = (dbo.t1_columns.c3))\nFOR TABLES (test_column_options.dbo.t1_columns AS materialize.public.t1_columns)\nEXPOSE PROGRESS AS materialize.public.t1_columns_sql_server_progress;"

> SHOW COLUMNS FROM t1_columns;
c1 true text ""
c2 true text ""
c4 true text ""

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 't1_columns_sql_server' AND ss.snapshot_records_staged > 0;
t1_columns_sql_server true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);

> SELECT c1, c2, c4 FROM t1_columns;
1.4448890000 12:00:00 EQCr

$ sql-server-execute name=sql-server
INSERT INTO t1_columns VALUES (2.000001, '01:59:32.99901', '$0.89', 0x1122AABBCC00DD), (NULL, NULL, '$99.99', NULL);

> SELECT c1, c2, c4 FROM t1_columns;
1.4448890000 12:00:00 EQCr
2.0000010000 01:59:32.999010 ESKqu8wA3Q==
<null> <null> <null>

! CREATE SOURCE t2_columns_sql_server
  FROM SQL SERVER CONNECTION sql_server_columns_connection (
    EXCLUDE COLUMNS (dbo.t2_columns.c1)
  )
  FOR TABLES (dbo.t2_columns);
contains:Table t2_columns had all columns excluded

> DROP SOURCE t1_columns_sql_server CASCADE;
