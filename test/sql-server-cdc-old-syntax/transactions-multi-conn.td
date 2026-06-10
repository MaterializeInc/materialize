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
USE test;

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


$ sql-server-execute name=sql-server

$ sql-server-connect name=sql-server-1
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}
$ sql-server-connect name=sql-server-2
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}
$ sql-server-connect name=sql-server-3
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server-1
USE test;
CREATE TABLE t1 (a INT);
CREATE TABLE t2 (a INT);
CREATE TABLE t3 (a INT);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't3', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT count(*) FROM t1;
0

> SELECT count(*) FROM t2;
0

> SELECT count(*) FROM t3;
0

$ sql-server-execute name=sql-server-1
USE test;
BEGIN TRANSACTION;
INSERT INTO t1 VALUES (1000);
INSERT INTO t2 VALUES (1000);
INSERT INTO t3 VALUES (1000);

$ sql-server-execute name=sql-server-2
USE test;
BEGIN TRANSACTION;
INSERT INTO t1 VALUES (2000);
INSERT INTO t2 VALUES (2000);
INSERT INTO t3 VALUES (2000);

$ sql-server-execute name=sql-server-3
USE test;
BEGIN TRANSACTION;
INSERT INTO t1 VALUES (3000);
INSERT INTO t2 VALUES (3000);
INSERT INTO t3 VALUES (3000);

$ sql-server-execute name=sql-server-1
COMMIT TRANSACTION;
BEGIN TRANSACTION;

$ sql-server-execute name=sql-server-3
COMMIT TRANSACTION;
BEGIN TRANSACTION;

> SELECT * FROM t1;
1000
3000

> SELECT * FROM t2;
1000
3000

> SELECT * FROM t3;
1000
3000

$ sql-server-execute name=sql-server-2
COMMIT TRANSACTION;
BEGIN TRANSACTION;

> SELECT * FROM t1;
1000
2000
3000

# delete and insert statements cannot be done in multiple transactions on the same table even with fine-grained where condition

$ sql-server-execute name=sql-server-1
INSERT INTO t1 VALUES (1001)
INSERT INTO t2 VALUES (1001)
INSERT INTO t3 VALUES (1001)

$ sql-server-execute name=sql-server-2
INSERT INTO t1 VALUES (2001)
INSERT INTO t2 VALUES (2001)
INSERT INTO t3 VALUES (2001)

$ sql-server-execute name=sql-server-3
INSERT INTO t1 VALUES (3001)
INSERT INTO t2 VALUES (3001)
INSERT INTO t3 VALUES (3001)

$ sql-server-execute name=sql-server-1
COMMIT TRANSACTION;
BEGIN TRANSACTION;

$ sql-server-execute name=sql-server-3
COMMIT TRANSACTION;
BEGIN TRANSACTION;

> SELECT * FROM t1;
1000
1001
2000
3000
3001

> SELECT * FROM t2;
1000
1001
2000
3000
3001

> SELECT * FROM t3;
1000
1001
2000
3000
3001

$ sql-server-execute name=sql-server-2
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO t1 VALUES (2002);

$ sql-server-execute name=sql-server-1
INSERT INTO t1 VALUES (1002);
DELETE FROM t2 WHERE a = 2000;

$ sql-server-execute name=sql-server-2
DELETE FROM t3 WHERE a = 2000;

$ sql-server-execute name=sql-server-1
COMMIT TRANSACTION;
BEGIN TRANSACTION;

> SELECT * FROM t1;
1000
1001
1002
2000
2001
3000
3001

> SELECT * FROM t2;
1000
1001
2001
3000
3001

> SELECT * FROM t3;
1000
1001
2000
2001
3000
3001

$ sql-server-execute name=sql-server-2
COMMIT TRANSACTION;
BEGIN TRANSACTION;

> SELECT * FROM t1;
1000
1001
1002
2000
2001
2002
3000
3001

> SELECT * FROM t2;
1000
1001
2001
3000
3001

> SELECT * FROM t3;
1000
1001
2001
3000
3001
