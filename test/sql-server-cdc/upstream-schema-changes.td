# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Perform various schema updates to the upstream table

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE foo (id BIGINT IDENTITY(1,1), name VARCHAR(16), value VARCHAR(32));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO foo VALUES ('a', 'apple'), ('b', 'banana');


> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE sql_server_src
  FROM SQL SERVER CONNECTION sql_server_conn;
> CREATE TABLE foo1 FROM SOURCE sql_server_src (REFERENCE foo);

> SELECT * FROM foo1;
1 a apple
2 b banana

$ sql-server-execute name=sql-server
ALTER TABLE foo ADD meta_col VARCHAR(32);
INSERT INTO foo VALUES ('c', 'cherry', 'wild');

> SELECT * FROM foo1;
1 a apple
2 b banana
3 c cherry

$ sql-server-execute name=sql-server
ALTER TABLE foo ALTER COLUMN meta_col VARCHAR(64);
INSERT INTO foo VALUES ('d', 'date', 'ajwa');

! SELECT * FROM foo1;
contains:Incompatible schema change

> DROP TABLE foo1;

# The upstream table `foo` was initially created without the `meta_col` column, as was the CDC
# table.  In the current state, the upstream table now has the `meta_col` column, but the CDC table
# does not.  Source creation must fail in this case as the snapshot and table description would
# contain this column, but the CDC events would not.  Rewinding to the initial LSN could result in
# negative accumulations.


! CREATE TABLE foo2 FROM SOURCE sql_server_src (REFERENCE dbo.foo);
contains:missing columns

> CREATE TABLE foo2 FROM SOURCE sql_server_src (REFERENCE dbo.foo) WITH (EXCLUDE COLUMNS = (meta_col));
> SELECT * FROM foo2;
1 a apple
2 b banana
3 c cherry
4 d date

$ sql-server-execute name=sql-server
ALTER TABLE foo DROP COLUMN id;

# DROP COLUMN stalls the source
! SELECT * FROM foo2;
contains:Incompatible schema change

> DROP TABLE foo2;

# The id column was dropped, so it exists in the CDC table, but not in the upstream.
# The SQL Server source decodes rows using column name instead of index, and since the id column
# will not exist in the table description, it will not be read from the CDC rows.
> CREATE TABLE foo3 FROM SOURCE sql_server_src (REFERENCE dbo.foo) WITH (EXCLUDE COLUMNS = (meta_col));
> SELECT * FROM foo3;
a apple
b banana
c cherry
d date

> DROP TABLE foo3;

$ sql-server-execute name=sql-server
USE test;
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'foo', @capture_instance = 'all';
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @supports_net_changes = 0;

> CREATE TABLE foo4 FROM SOURCE sql_server_src (REFERENCE dbo.foo);
> SELECT * FROM foo4;
a apple <null>
b banana <null>
c cherry wild
d date ajwa

$ sql-server-execute name=sql-server
INSERT INTO foo VALUES ('e', 'elderberry', 'montypython');

> SELECT * FROM foo4;
a apple <null>
b banana <null>
c cherry wild
d date ajwa
e elderberry montypython

> DROP SOURCE sql_server_src CASCADE;
