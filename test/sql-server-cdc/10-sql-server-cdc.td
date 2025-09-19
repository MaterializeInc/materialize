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
CREATE TABLE t1_pk (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1_pk', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1_pk VALUES ('a', 'hello world'), ('b', 'foobar'), ('c', 'anotha one');

CREATE TABLE t2_no_cdc (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));

CREATE TABLE t3_text (value VARCHAR(100));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't3_text', @role_name = 'SA', @supports_net_changes = 0;

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

> SELECT name, type from mz_connections WHERE name = 'sql_server_test_connection';
name                         type
---------------------------------------
sql_server_test_connection   sql-server


# Create a SQL Server Source.

> CREATE SOURCE t1_pk_sql_server
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SHOW SOURCES
dummy subsource quickstart ""
t1_pk subsource quickstart ""
t1_pk_sql_server sql-server quickstart ""
t1_pk_sql_server_progress progress <null> ""
t3_text subsource quickstart ""

> SELECT schema_name, table_name FROM mz_internal.mz_sql_server_source_tables;
dbo dummy
dbo t1_pk
dbo t3_text

$ set-from-sql var=source-id
SELECT id FROM mz_sources WHERE name = 't1_pk_sql_server';

$ set-from-sql var=subsource-id
SELECT id FROM mz_sources WHERE name = 't1_pk';

> SELECT * FROM t1_pk;
a "hello world"
b "foobar"
c "anotha one"

$ sql-server-execute name=sql-server
UPDATE t1_pk SET val_col = 'I am an updated value' WHERE key_col = 'a';

> SELECT offset_known IS NULL, offset_committed IS NULL FROM mz_internal.mz_source_statistics WHERE id = '${subsource-id}';
false false

> SELECT snapshot_records_staged, snapshot_records_known
  FROM mz_internal.mz_source_statistics
  WHERE id = '${subsource-id}';
3 3

> SELECT * FROM t1_pk;
a "I am an updated value"
b "foobar"
c "anotha one"

$ sql-server-execute name=sql-server
DELETE t1_pk WHERE key_col = 'a';

> SELECT * FROM t1_pk;
b "foobar"
c "anotha one"

$ sql-server-execute name=sql-server
INSERT INTO t1_pk VALUES ('😊', 'lets see what happens');

# The test setup uses a UTF8 collation, which allows VARCHAR to store emojis.
> SELECT * FROM t1_pk;
b "foobar"
c "anotha one"
😊 "lets see what happens"


# Insert a lot of data upstream.

$ sql-server-execute name=sql-server
WITH Tally(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM Tally WHERE n < 1000) INSERT INTO t3_text (value) SELECT 'a longer string that will be a bit of data, cool ' + CAST(n AS VARCHAR) FROM Tally OPTION (MAXRECURSION 1000);

> SELECT COUNT(*) FROM t3_text;
1000

$ sql-server-execute name=sql-server
INSERT INTO t3_text (value) SELECT value FROM t3_text;
INSERT INTO t3_text (value) SELECT value FROM t3_text;
INSERT INTO t3_text (value) SELECT value FROM t3_text;
INSERT INTO t3_text (value) SELECT value FROM t3_text;
INSERT INTO t3_text (value) SELECT value FROM t3_text;

> SELECT COUNT(*) FROM t3_text;
32000

> DROP SOURCE t1_pk_sql_server CASCADE;
