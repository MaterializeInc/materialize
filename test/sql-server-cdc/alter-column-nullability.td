# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Changing a replicated column's nullability upstream. Adding NOT NULL keeps
# replicating, since every value still fits the nullable column Materialize
# recorded. Dropping NOT NULL or changing the type stalls the table.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE foo (id INT PRIMARY KEY, name VARCHAR(16));
CREATE TABLE bar (id INT PRIMARY KEY, code VARCHAR(16) NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'bar', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO foo VALUES (1, 'a');
INSERT INTO bar VALUES (1, 'x');

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE sql_server_src FROM SQL SERVER CONNECTION sql_server_conn;
> CREATE TABLE foo FROM SOURCE sql_server_src (REFERENCE dbo.foo);
> CREATE TABLE bar FROM SOURCE sql_server_src (REFERENCE dbo.bar);

> SELECT * FROM foo;
1 a

> SELECT * FROM bar;
1 x

$ sql-server-execute name=sql-server
ALTER TABLE foo ALTER COLUMN name VARCHAR(16) NOT NULL;
INSERT INTO foo VALUES (2, 'b');

> SELECT * FROM foo;
1 a
2 b

$ sql-server-execute name=sql-server
ALTER TABLE foo ALTER COLUMN name VARCHAR(32) NOT NULL;
INSERT INTO foo VALUES (3, 'c');
ALTER TABLE bar ALTER COLUMN code VARCHAR(16) NULL;
INSERT INTO bar VALUES (2, NULL);

! SELECT * FROM foo;
contains:Incompatible schema change

! SELECT * FROM bar;
contains:Incompatible schema change

> DROP SOURCE sql_server_src CASCADE;
