# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Two hydrated tables, with a SQL Server Agent job inserting into t1 so the
# exports' committed LSNs keep moving independently.

$ set-sql-timeout duration=60s

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1 (id INT IDENTITY PRIMARY KEY, v VARCHAR(20));
CREATE TABLE t2 (id INT IDENTITY PRIMARY KEY, v VARCHAR(20));
CREATE TABLE t3 (id INT IDENTITY PRIMARY KEY, v VARCHAR(20));
CREATE TABLE t4 (id INT IDENTITY PRIMARY KEY, v VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't3', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't4', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t1 (v) VALUES ('x');
INSERT INTO t2 (v) VALUES ('y');
INSERT INTO t3 (v) VALUES ('z');
INSERT INTO t4 (v) VALUES ('w');

$ sql-server-execute name=sql-server split-lines=false
CREATE OR ALTER PROCEDURE dbo.Inserter AS
BEGIN
    SET NOCOUNT ON;
    WHILE 1 = 1
    BEGIN
        INSERT INTO dbo.t1 (v) VALUES ('tick');
        WAITFOR DELAY '00:00:00.020';
    END
END;

$ sql-server-execute name=sql-server
USE msdb;
EXEC sp_add_job @job_name = N'Inserter', @enabled = 1;
EXEC sp_add_jobstep @job_name = N'Inserter', @step_name = N'run', @subsystem = N'TSQL', @database_name = N'test', @command = N'EXEC dbo.Inserter;';
EXEC sp_add_jobserver @job_name = N'Inserter';
EXEC msdb.dbo.sp_start_job N'Inserter';

> CREATE SECRET mspass AS '${arg.default-sql-server-password}'
> CREATE CONNECTION msconn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET mspass
  )

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION msconn
  WITH (TIMESTAMP INTERVAL '50ms')
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE dbo.t1)
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE dbo.t2)

> SELECT count(*) FROM t2
1

> SELECT bool_and(u.offset_committed > 0)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('t1', 't2')
true
