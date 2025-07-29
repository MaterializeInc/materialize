# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# These goal of these tests is to exercise what isn't supported in the SQL
# Server source and that we properly handle these scenarios.

# Setup SQL Server state.
#
# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
DROP DATABASE IF EXISTS test_50;
CREATE DATABASE test_50;
USE test_50;

EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test_50 SET ALLOW_SNAPSHOT_ISOLATION ON;

CREATE TABLE table_text (good varchar(128), bad text);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_text', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_text VALUES ('i work', 'i do not'), ('maybe me', 'not so much');

CREATE TABLE table_image (good int, bad image);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_image', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_image VALUES (42, NULL);

CREATE TABLE table_geography (good int, bad geography);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_geography', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_geography VALUES (42, NULL);

CREATE TABLE table_geometry (good int, bad geometry);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_geometry', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_geometry VALUES (42, NULL);

CREATE TABLE table_varchar_max (good int, bad varchar(max));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_varchar_max', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_varchar_max VALUES (42, NULL);

CREATE TABLE dummy (data int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dummy', @role_name = 'SA', @supports_net_changes = 0;

EXEC sys.sp_cdc_change_job @pollinginterval = 1;

# SQL server delay because it's terrible for testing and the initial_lsn selection
# may happen faster than CDC manages to process the above updates
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;

> CREATE CONNECTION sql_server_test_50_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test_50,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

! CREATE SOURCE table_text_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_text);
contains: SQL SERVER source validation: column dbo.table_text.bad of type text is not supported

> CREATE SOURCE table_text_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection (
    EXCLUDE COLUMNS (dbo.table_text.bad)
  )
  FOR TABLES (dbo.table_text);

> SHOW COLUMNS FROM table_text;
good true "character varying" ""

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'table_text_source' AND ss.snapshot_records_staged > 0;
table_text_source true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);

> SELECT * FROM table_text;
"i work"
"maybe me"

> DROP SOURCE table_text_source CASCADE;

! CREATE SOURCE table_image_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_image);
contains: SQL SERVER source validation: column dbo.table_image.bad of type image is not supported

! CREATE SOURCE table_geography_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_geography);
contains: SQL SERVER source validation: column dbo.table_geography.bad of type geography is not supported

! CREATE SOURCE table_geometry_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_geometry);
contains: SQL SERVER source validation: column dbo.table_geometry.bad of type geometry is not supported

! CREATE SOURCE table_varchar_max_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_varchar_max);
contains: SQL SERVER source validation: column dbo.table_varchar_max.bad of type varchar is not supported


## same tests for source tables

# Creating a source with no subsources should succeed
> CREATE SOURCE ms_src
  FROM SQL SERVER CONNECTION sql_server_test_50_connection;

! CREATE TABLE table_text
  FROM SOURCE ms_src (REFERENCE dbo.table_text);
contains: SQL SERVER source validation: column dbo.table_text.bad of type text is not supported

> CREATE TABLE table_text
  FROM SOURCE ms_src (REFERENCE dbo.table_text)
  WITH (
    EXCLUDE COLUMNS (bad)
  );

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'ms_src' AND ss.snapshot_records_staged > 0;
ms_src true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);

> SHOW COLUMNS FROM table_text;
good true "character varying" ""

> SELECT * FROM table_text;
"i work"
"maybe me"

! CREATE TABLE table_image
  FROM SOURCE ms_src (REFERENCE dbo.table_image);
contains: SQL SERVER source validation: column dbo.table_image.bad of type image is not supported

! CREATE TABLE table_geography
  FROM SOURCE ms_src (REFERENCE dbo.table_geography);
contains: SQL SERVER source validation: column dbo.table_geography.bad of type geography is not supported

! CREATE TABLE table_geometry
  FROM SOURCE ms_src (REFERENCE dbo.table_geometry);
contains: SQL SERVER source validation: column dbo.table_geometry.bad of type geometry is not supported

! CREATE TABLE table_varchar_max
  FROM SOURCE ms_src (REFERENCE dbo.table_varchar_max);
contains: SQL SERVER source validation: column dbo.table_varchar_max.bad of type varchar is not supported

> DROP SOURCE ms_src CASCADE;
