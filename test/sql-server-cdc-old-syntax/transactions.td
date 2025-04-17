# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that transactions work properly
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

$ postgres-connect name=conn url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn
CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES (3);
INSERT INTO t1 VALUES (4);

CREATE TABLE t2 (a INT);

CREATE TABLE t3 (a INT);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't3', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT * FROM t1;
3
4

$ postgres-execute connection=conn
BEGIN;
DELETE FROM t1;
INSERT INTO t1 VALUES (5);

> SELECT * FROM t1;
3
4

$ postgres-execute connection=conn
COMMIT;
BEGIN;

> SELECT * FROM t1;
5

$ postgres-execute connection=conn
INSERT INTO t1 VALUES (6);
COMMIT;
BEGIN;

INSERT INTO t2 VALUES (20);
INSERT INTO t3 VALUES (300);
COMMIT;
BEGIN;

DELETE FROM t1;
ROLLBACK;
BEGIN;

UPDATE t1 SET a = a + 8;
DELETE FROM t3 WHERE a = 300;
INSERT INTO t2 SELECT * FROM t1;
UPDATE t1 SET a = 100 WHERE a = 14;
COMMIT;
BEGIN;

UPDATE t2 SET a = a + 10;
INSERT INTO t3 VALUES (500), (600);
INSERT INTO t2 VALUES (44);
DELETE FROM t3 WHERE a = 500;
COMMIT;
BEGIN;

INSERT INTO t2 VALUES (99);

> SELECT * FROM t1;
13
100

> SELECT * FROM t2;
30
23
24
44

> SELECT * FROM t3;
600

$ postgres-execute connection=conn
COMMIT;
BEGIN;

> SELECT * FROM t2;
30
23
24
44
99
