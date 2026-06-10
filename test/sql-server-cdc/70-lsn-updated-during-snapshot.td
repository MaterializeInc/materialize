# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that if we snapshot while writes are happening we will not lose data.
# We want to guarantee that there were writes to t2 while t0 and t1 are being
# snapshotted so we write 3 new rows every 10ms to t2.
# We then halt the job writing rows to t2. Count the number of rows in SQL Server
# then assert that the final number of rows in t2 are the same.
#
# Thus we verify that t2 picks the correct LSN to rewind from after snapshotting.
#
# Note: we chose 10ms as that was found to be the lowest interval that SQL Server would
# reliably execute a job. We are also assuming that t3 and t4 will take > 10ms to snapshot
# thereby forcing the max LSN seen in the t3 and t4 snapshots to be higher than t2.
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

$ sql-server-execute name=sql-server
CREATE TABLE t1 (cat VARCHAR(1024));
CREATE TABLE t2 (cat VARCHAR(1024));
CREATE TABLE t3 (cat VARCHAR(1024));
CREATE TABLE t4 (cat VARCHAR(1024));

$ sql-server-execute name=sql-server split-lines=false
# TODO(ptravers): rewrite to insert rows with an included counter so we can assert in a rollup
# that the counter included in the rows represents the sequence An = An-1 + 1.
CREATE OR ALTER PROCEDURE dbo.FastTicker AS
BEGIN
    SET NOCOUNT ON;
    WHILE 1 = 1
    BEGIN
        INSERT INTO t2 VALUES ('peanut'), ('minnie'), ('mischa'), ('archie');
        WAITFOR DELAY '00:00:00:010';
    END
END;

$ sql-server-execute name=sql-server
USE msdb;
EXEC sp_add_job @job_name = N'FastTicker', @enabled = 1;
EXEC sp_add_jobstep @job_name = N'FastTicker', @step_name = N'run', @subsystem = N'TSQL', @database_name = N'test', @command = N'EXEC dbo.FastTicker;';
EXEC sp_add_jobserver @job_name = N'FastTicker';
EXEC msdb.dbo.sp_start_job N'FastTicker';
# Wait for 10 seconds to ensure the job has started and that we are writing as we snapshot.
WAITFOR DELAY '00:00:10';

$ sql-server-execute name=sql-server
USE test;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't3', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't4', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1 VALUES ('peanut'), ('minnie'), ('mischa'), ('archie');
WITH Tally(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM Tally WHERE n < 5000) INSERT INTO t1 (cat) SELECT 'cat' + CAST(n AS VARCHAR) FROM Tally OPTION (MAXRECURSION 5000);

INSERT INTO t2 VALUES ('peanut'), ('minnie'), ('mischa'), ('archie');
WITH Tally(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM Tally WHERE n < 5000) INSERT INTO t2 (cat) SELECT 'cat' + CAST(n AS VARCHAR) FROM Tally OPTION (MAXRECURSION 5000);

INSERT INTO t3 VALUES ('peanut'), ('minnie'), ('mischa'), ('archie');
WITH Tally(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM Tally WHERE n < 5000) INSERT INTO t3 (cat) SELECT 'cat' + CAST(n AS VARCHAR) FROM Tally OPTION (MAXRECURSION 5000);

INSERT INTO t4 VALUES ('peanut'), ('minnie'), ('mischa'), ('archie');
WITH Tally(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM Tally WHERE n < 5000) INSERT INTO t4 (cat) SELECT 'cat' + CAST(n AS VARCHAR) FROM Tally OPTION (MAXRECURSION 5000);

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

> CREATE SOURCE sql_server
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

$ sql-server-execute name=sql-server split-lines=false
USE msdb;
EXEC msdb.dbo.sp_stop_job N'FastTicker';

# Wait for 5 seconds to ensure the job has stopped and that we stopped writing to t2.
# We want to ensure that the count we take below is after the job has stopped to guarantee
# that replication hasn't lost rows but then appeared to catch up due to new rows being
# written.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="5s"


$ sql-server-set-from-sql name=sql-server var=count
USE test
SELECT CAST(COUNT(*) AS NVARCHAR(128)) FROM t2;

> SELECT COUNT(*) = ${count} FROM t2;
true

> SELECT COUNT(*) FROM t1;
5004

> SELECT COUNT(*) FROM t3;
5004

> SELECT COUNT(*) FROM t4;
5004
