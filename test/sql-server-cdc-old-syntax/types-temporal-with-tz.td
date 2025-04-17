# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the temporal data types with time zone
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'test') BEGIN ALTER DATABASE test SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE test; END;
CREATE DATABASE test;
USE test;
EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

# Insert data pre-snapshot
$ sql-server-execute name=sql-server
CREATE TABLE time_table (f1 TIME WITH TIME ZONE);
INSERT INTO time_table VALUES ('11:11:11.123456-09');
INSERT INTO time_table VALUES ('11:11:11.123456+09');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'time_table', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE timestamp_table (f1 TIMESTAMPTZ);
INSERT INTO timestamp_table VALUES ('2011-11-11 11:11:11.123456-09');
INSERT INTO timestamp_table VALUES ('2011-11-11 11:11:11.123456+09');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'timestamp_table', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SOURCE mz_source_timestamp
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM timestamp_table;
true

# Insert the same data post-snapshot
$ sql-server-execute name=sql-server
INSERT INTO time_table SELECT * FROM time_table;
INSERT INTO timestamp_table SELECT * FROM timestamp_table;

> SELECT pg_typeof(f1) FROM timestamp_table LIMIT 1;
"timestamp with time zone"

> SELECT * FROM timestamp_table;
"2011-11-11 02:11:11.123456 UTC"
"2011-11-11 02:11:11.123456 UTC"
"2011-11-11 20:11:11.123456 UTC"
"2011-11-11 20:11:11.123456 UTC"

> DROP SOURCE mz_source_timestamp CASCADE;

# Mz does not support TIME WITH TIME ZOINE
! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;
contains:type "pg_catalog.timetz" does not exist
