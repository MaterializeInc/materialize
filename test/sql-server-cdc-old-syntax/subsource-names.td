# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that subsources are created in the source schema if provided
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
CREATE SCHEMA subsource_test;
CREATE TABLE subsource_test.t1 (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'subsource_test', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO subsource_test.t1 VALUES (1);

CREATE TABLE subsource_test.t2 (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'subsource_test', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO subsource_test.t2 VALUES (5);

> CREATE SCHEMA a;
> CREATE SOURCE a.mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1);

> SELECT * FROM a.t1;
1

> CREATE SCHEMA another;
> CREATE SOURCE another.mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1, t2);

> SELECT * FROM another.t1;
1

> SELECT * FROM another.t2;
5

> CREATE SCHEMA foo;
> CREATE SCHEMA bar;
> CREATE SOURCE foo.mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1 as table1, t2 as bar.table2);

! SELECT * FROM table1;
contains: unknown

# table1 gets created in source schema foo because it doesn't have any prefix
> SELECT * FROM foo.table1;
1

! SELECT * FROM foo.table2;
contains: unknown

# table2 gets created in mentioned bar because it does have a prefix
> SELECT * FROM bar.table2;
5

> CREATE SCHEMA baz;
> CREATE SOURCE baz.mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR SCHEMAS (subsource_test);

> SELECT * FROM baz.t1;
1

> SELECT * FROM baz.t2;
5