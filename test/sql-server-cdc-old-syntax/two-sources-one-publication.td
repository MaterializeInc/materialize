# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that two sources reading from one capture instance is OK
#

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

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1 (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t1 VALUES (1);

CREATE TABLE t2 (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t2 VALUES (5);

> DROP SCHEMA IF EXISTS schema1

> CREATE SCHEMA schema1

> CREATE SOURCE mz_source1
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1 AS t1_1, t2 AS t2_1);

> CREATE SOURCE mz_source2
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1 AS t1_2, t2 AS t2_2);

> SELECT * FROM t1_1;
1

> SELECT * FROM t2_1;
5

> SELECT * FROM t1_2;
1

> SELECT * FROM t2_2;
5

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES (2);
INSERT INTO t1 VALUES (3);
INSERT INTO t2 VALUES (6);
INSERT INTO t2 VALUES (7);

> SELECT * FROM t1_1;
1
2
3

> SELECT * FROM t2_1;
5
6
7

> SELECT * FROM t1_2;
1
2
3

> SELECT * FROM t2_2;
5
6
7

> DROP SCHEMA schema1 CASCADE;
