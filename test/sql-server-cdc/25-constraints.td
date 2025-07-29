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
DROP DATABASE IF EXISTS test_25;
CREATE DATABASE test_25;
USE test_25;

EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test_25 SET ALLOW_SNAPSHOT_ISOLATION ON;

CREATE TABLE t25_pk (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024) NOT NULL, extra VARCHAR(200));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't25_pk', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t25_pk VALUES ('a', 'hello world', NULL), ('b', 'foobar', 'apple'), ('c', 'anotha one', 'orange');

CREATE TABLE t25_pk2 (id VARCHAR(20), seq_no VARCHAR(1024), extra VARCHAR(200), PRIMARY KEY(id, seq_no));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't25_pk2', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t25_pk2 VALUES ('i am an ID', '1000', 'uhhmmmm');

CREATE TABLE dummy (data int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dummy', @role_name = 'SA', @supports_net_changes = 0;

EXEC sys.sp_cdc_change_job @pollinginterval = 1;

# SQL server delay because it's terrible for testing and the initial_lsn selection
# may happen faster than CDC manages to process the above updates
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;

> CREATE CONNECTION sql_server_test_25_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test_25,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE t25_pk_sql_server
  FROM SQL SERVER CONNECTION sql_server_test_25_connection
  FOR ALL TABLES;

> SELECT name, regexp_replace(create_sql, '[us]\d+|__.*__[A-F0-9]+|DETAILS = ''[a-f0-9]+''', '<VAR>', 'g') as sql FROM mz_sources WHERE name IN ('t25_pk', 't25_pk2');
t25_pk "CREATE SUBSOURCE \"materialize\".\"public\".\"t25_pk\" (\"key_col\" [<VAR> AS \"pg_catalog\".\"varchar\"](20) NOT NULL, \"val_col\" [<VAR> AS \"pg_catalog\".\"varchar\"](1024) NOT NULL, \"extra\" [<VAR> AS \"pg_catalog\".\"varchar\"](200), CONSTRAINT \"PK<VAR>\" PRIMARY KEY (\"key_col\")) OF SOURCE [<VAR> AS \"materialize\".\"public\".\"t25_pk_sql_server\"] WITH (EXTERNAL REFERENCE = \"test_25\".\"dbo\".\"t25_pk\", <VAR>)"
t25_pk2 "CREATE SUBSOURCE \"materialize\".\"public\".\"t25_pk2\" (\"id\" [<VAR> AS \"pg_catalog\".\"varchar\"](20) NOT NULL, \"seq_no\" [<VAR> AS \"pg_catalog\".\"varchar\"](1024) NOT NULL, \"extra\" [<VAR> AS \"pg_catalog\".\"varchar\"](200), CONSTRAINT \"PK<VAR>\" PRIMARY KEY (\"id\", \"seq_no\")) OF SOURCE [<VAR> AS \"materialize\".\"public\".\"t25_pk_sql_server\"] WITH (EXTERNAL REFERENCE = \"test_25\".\"dbo\".\"t25_pk2\", <VAR>)"

> SHOW COLUMNS FROM t25_pk
key_col false "character varying" ""
val_col false "character varying" ""
extra true "character varying" ""

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 't25_pk_sql_server' AND ss.snapshot_records_staged > 0;
t25_pk_sql_server true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);


> SELECT * FROM t25_pk;
a "hello world" <null>
b foobar apple
c "anotha one" orange

> DROP SOURCE t25_pk_sql_server CASCADE;
