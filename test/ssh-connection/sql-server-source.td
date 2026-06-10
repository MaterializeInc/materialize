# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a SQL Server source using SSH

> CREATE SECRET sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    DATABASE 'test',
    USER '${arg.default-sql-server-user}',
    PASSWORD SECRET sql_server_pass,
    SSH TUNNEL thancred
  );

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1 (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t1 VALUES (1), (2);

> BEGIN
> CREATE SOURCE sql_server_source FROM SQL SERVER CONNECTION sql_server_conn;
> CREATE TABLE t1 FROM SOURCE sql_server_source (REFERENCE t1);
> COMMIT

> SELECT COUNT(*) FROM t1;
2

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES (3);

> SELECT * FROM t1;
1
2
3
