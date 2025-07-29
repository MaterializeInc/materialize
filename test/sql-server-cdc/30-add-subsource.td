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
DROP DATABASE IF EXISTS test_30;
CREATE DATABASE test_30;
USE test_30;

EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test_30 SET ALLOW_SNAPSHOT_ISOLATION ON;

CREATE TABLE t1_add_subsource (val VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1_add_subsource', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1_add_subsource VALUES ('a'), ('b'), (NULL), ('c');

CREATE TABLE t2_add_subsource (val VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2_add_subsource', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t2_add_subsource VALUES ('100'), ('200'), (NULL), ('300');

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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET log_filter = 'mz_sql=debug,info';

> CREATE CONNECTION IF NOT EXISTS sql_server_test_add_subsource_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test_30,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

# Create a SQL Server Source.

> CREATE SOURCE my_source_add_subsources
  FROM SQL SERVER CONNECTION sql_server_test_add_subsource_conn
  FOR TABLES (dbo.t1_add_subsource);

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'my_source_add_subsources' AND ss.snapshot_records_staged > 0;
my_source_add_subsources true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);


> SELECT * FROM t1_add_subsource;
a
b
c
<null>

> ALTER SOURCE my_source_add_subsources ADD SUBSOURCE dbo.t2_add_subsource;

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'my_source_add_subsources' AND ss.snapshot_records_staged > 0;
my_source_add_subsources true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);

> SELECT * FROM t2_add_subsource;
100
200
300
<null>

# Add some more data to ensure the replication continues.

$ sql-server-execute name=sql-server
INSERT INTO t1_add_subsource VALUES ('d');
INSERT INTO t2_add_subsource VALUES ('400');

> SELECT * FROM t1_add_subsource;
a
b
c
d
<null>

> SELECT * FROM t2_add_subsource;
100
200
300
400
<null>

$ sql-server-execute name=sql-server
INSERT INTO t1_add_subsource VALUES ('e');
INSERT INTO t2_add_subsource VALUES ('500');

> DROP SOURCE t1_add_subsource;

> SELECT * FROM t2_add_subsource;
100
200
300
400
500
<null>

> ALTER SOURCE my_source_add_subsources ADD SUBSOURCE dbo.t1_add_subsource;

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'my_source_add_subsources' AND ss.snapshot_records_staged > 0;
my_source_add_subsources true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);


> SELECT * FROM t1_add_subsource;
a
b
c
d
e
<null>

$ sql-server-execute name=sql-server
INSERT INTO t1_add_subsource VALUES ('f');

> SELECT * FROM t1_add_subsource;
a
b
c
d
e
f
<null>

> DROP SOURCE my_source_add_subsources CASCADE;
