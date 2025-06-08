# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test various variants of INT IDENTITY(1,1)
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
CREATE TABLE t1 (f1 SMALLINT IDENTITY(1,1), f2 SERIAL, f3 BIGSERIAL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1 VALUES (32767, 2147483647, 9223372036854775807);

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ sql-server-execute name=sql-server
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1), type_name(f2), type_name(f3) FROM t1 LIMIT 1;
smallint integer bigint

> SELECT * FROM t1;
32767 2147483647 9223372036854775807
32767 2147483647 9223372036854775807
