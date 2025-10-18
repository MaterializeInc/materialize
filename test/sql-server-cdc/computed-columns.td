# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1 (a int, b int, np as (a*b), p as (a+b) persisted);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1 VALUES (2,2), (3,3);

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );



> CREATE SOURCE t1_src FROM SQL SERVER CONNECTION sql_server_conn;

! CREATE TABLE t1 FROM SOURCE t1_src (REFERENCE dbo.t1);
regex:column \S+ of type \S+ is not supported
detail:column is computed

# excluding the columns allows successful configuration
> CREATE TABLE t1 FROM SOURCE t1_src (REFERENCE dbo.t1) WITH (EXCLUDE COLUMNS = (np, p));

> SELECT * FROM t1;
2 2
3 3

$ sql-server-execute name=sql-server
DELETE FROM t1 WHERE a = 3;
UPDATE t1 SET b = 5 WHERE a = 2;

> SELECT * FROM t1;
2 5
