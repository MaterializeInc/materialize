# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that a publication WITH (publish = 'XXX') option is handled
# correctly.
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'test') BEGIN ALTER DATABASE test SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE test; END;
CREATE DATABASE test;
USE test;
EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON;

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
CREATE TABLE t1 (f1 INTEGER PRIMARY KEY);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

CREATE PUBLICATION mz_source_insert FOR TABLE t1 WITH ( publish = 'insert') ;
CREATE PUBLICATION mz_source_update FOR TABLE t1 WITH ( publish = 'update') ;
CREATE PUBLICATION mz_source_delete FOR TABLE t1 WITH ( publish = 'delete') ;

> CREATE SOURCE mz_source_insert
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1 AS t1_insert);

> CREATE SOURCE mz_source_update
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1 AS t1_update);

> CREATE SOURCE mz_source_delete
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1 AS t1_delete);

# Make sure that the above sources are fully instantiated
# (that is, we have finished absorbing their snapshots),
# so that the DML statements below are sent as actual replication
# events post-snapshot.
#
# Currently there is no good way to do this other than sleeping.
# <https://github.com/MaterializeInc/database-issues/issues/4266> tracks
# making this better
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s
> SELECT COUNT(*) FROM mz_source_insert;
0
> SELECT COUNT(*) FROM mz_source_update;
0
> SELECT COUNT(*) FROM mz_source_delete;
0

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES (1);
UPDATE t1 SET f1 = 2 WHERE f1 = 1;
DELETE FROM t1;

> SELECT * FROM t1_insert;
1

! SELECT * FROM t1_update;
contains:Invalid data in source, saw retractions (1) for row that does not exist

! SELECT * FROM t1_delete;
contains:Invalid data in source, saw retractions (1) for row that does not exist
