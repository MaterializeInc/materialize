# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Setup SQL Server state.
#
# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE foo (id int, name varchar(max), skip int, other nvarchar(max));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = NULL, @capture_instance = 'foo_cap', @supports_net_changes = 0;
INSERT INTO foo VALUES (1, 'a', 0, 'a'), (2, 'b', 0, 'b'), (3, NULL, 0, NULL);

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE ms_src
  FROM SQL SERVER CONNECTION sql_server_test_connection;
> CREATE TABLE foo FROM SOURCE ms_src (REFERENCE foo);

> SELECT * FROM foo;
1 a 0 a
2 b 0 b
3 <null> 0 <null>

$ sql-server-execute name=sql-server
UPDATE foo SET skip = 1 where id = 2;
UPDATE foo SET name = 'c', other = 'c' where id = 3;

> SELECT * FROM foo;
1 a 0 a
2 b 1 b
3 c 0 c


## Test CDC when `max repl text` size is decreased after data is inserted.  This configuration
## prevents inserts/updates for writing data to the source table if the field length exceeds the
## current setting, so we focus on ensuring updates/deletes to existing

$ sql-server-execute name=sql-server
INSERT INTO foo VALUES (4, 'abcdefghijklmnopqrstuvwxyz', 0, 'abcdefghijklmnopqrstuvwxyz');
INSERT INTO foo VALUES (5, 'abcdefghijklmnopqrstuvwxyz', 0, 'abcdefghijklmnopqrstuvwxyz');
INSERT INTO foo VALUES (6, 'abcdefghijklmnopqrstuvwxyz', 0, 'abcdefghijklmnopqrstuvwxyz');

> SELECT * FROM foo;
1 a 0 a
2 b 1 b
3 c 0 c
4 abcdefghijklmnopqrstuvwxyz 0 abcdefghijklmnopqrstuvwxyz
5 abcdefghijklmnopqrstuvwxyz 0 abcdefghijklmnopqrstuvwxyz
6 abcdefghijklmnopqrstuvwxyz 0 abcdefghijklmnopqrstuvwxyz

## can update rows that have been written with fields larger than `max repl text size`

# Update LOD (Large Object Data)
$ sql-server-execute name=sql-server
EXEC sp_configure 'max text repl size', 20;
RECONFIGURE;
UPDATE foo SET name = 'd', other = 'd' WHERE id = 4;

> SELECT * FROM foo;
1 a 0 a
2 b 1 b
3 c 0 c
4 d 0 d
5 abcdefghijklmnopqrstuvwxyz 0 abcdefghijklmnopqrstuvwxyz
6 abcdefghijklmnopqrstuvwxyz 0 abcdefghijklmnopqrstuvwxyz

# Update non-LOD column
$ sql-server-execute name=sql-server
UPDATE foo SET skip = 1 WHERE id = 5;

> SELECT * FROM foo;
1 a 0 a
2 b 1 b
3 c 0 c
4 d 0 d
5 abcdefghijklmnopqrstuvwxyz 1 abcdefghijklmnopqrstuvwxyz
6 abcdefghijklmnopqrstuvwxyz 0 abcdefghijklmnopqrstuvwxyz

## can delete rows that contain LOD columns
$ sql-server-execute name=sql-server
DELETE FROM foo WHERE id = 6;

> SELECT * FROM foo;
1 a 0 a
2 b 1 b
3 c 0 c
4 d 0 d
5 abcdefghijklmnopqrstuvwxyz 1 abcdefghijklmnopqrstuvwxyz

## Ensure that update mask indexing works correctly when a column exists in the source table
## but is excluded from CDC.

> DROP TABLE foo;

$ sql-server-execute name=sql-server
DELETE FROM foo;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = NULL, @capture_instance = 'foo_cap1', @captured_column_list = 'id, name, other', @supports_net_changes = 0;

INSERT INTO foo VALUES (1, 'a', 0, 'a'), (2, 'b', 0, 'b'), (3, NULL, 0, NULL);

> CREATE TABLE foo FROM SOURCE ms_src (REFERENCE dbo.foo) WITH (EXCLUDE COLUMNS = (skip));

> SELECT * FROM foo;
1 a a
2 b b
3 <null> <null>

$ sql-server-execute name=sql-server
UPDATE foo SET id = 222 where id = 2;
UPDATE foo SET name = 'c', other = 'c' where id = 3;
UPDATE foo SET name = null, other = null where id = 1;

> SELECT * FROM foo;
1 <null> <null>
222 b b
3 c c

# The current CDC stream processes up to 64 events in a batch. This test will create 63 INSERTS
# followed by an update. The UPDATE will generate 2 rows.  The first will be in the batch with
# 63 inserts, the second will be in its own batch.

$ sql-server-execute name=sql-server
WITH Tally(n) AS (SELECT 10 UNION ALL SELECT n + 1 FROM Tally WHERE n < 63 ) INSERT INTO foo SELECT n, 'z', 0, 'z' FROM Tally OPTION (MAXRECURSION 63);
UPDATE foo SET id = 2 WHERE id = 222;

> SELECT * FROM foo;
1 <null> <null>
2 b b
3 c c
10 z z
11 z z
12 z z
13 z z
14 z z
15 z z
16 z z
17 z z
18 z z
19 z z
20 z z
21 z z
22 z z
23 z z
24 z z
25 z z
26 z z
27 z z
28 z z
29 z z
30 z z
31 z z
32 z z
33 z z
34 z z
35 z z
36 z z
37 z z
38 z z
39 z z
40 z z
41 z z
42 z z
43 z z
44 z z
45 z z
46 z z
47 z z
48 z z
49 z z
50 z z
51 z z
52 z z
53 z z
54 z z
55 z z
56 z z
57 z z
58 z z
59 z z
60 z z
61 z z
62 z z
63 z z
