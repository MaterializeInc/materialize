# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that we can ingest unchanged toasted values
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


# Insert data pre-snapshot by generating 16kB of uncompressible data to force
# TOASTed storage by concatenating 1024 random MD5 hashes, each being 128bit
$ sql-server-execute name=sql-server
CREATE TABLE t1 (a int, b varchar(128));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

WITH numbers AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM numbers WHERE n + 1 <= 1024) INSERT INTO t1 SELECT 1, STRING_AGG(CONVERT(VARCHAR(32), HASHBYTES('MD5', NEWID()), 2), '') FROM numbers OPTION (MAXRECURSION 0);
WITH numbers AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM numbers WHERE n + 1 <= 1024) INSERT INTO t1 SELECT 2, STRING_AGG(CONVERT(VARCHAR(32), HASHBYTES('MD5', NEWID()), 2), '') FROM numbers OPTION (MAXRECURSION 0);

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT a, length(b) FROM t1;
1 32768
2 32768

# Update the rows without touching the TOASTed column
$ sql-server-execute name=sql-server
UPDATE t1 SET a = 3;

> SELECT a, length(b) FROM t1;
3 32768
3 32768
