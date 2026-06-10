# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server split-lines=false
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'test')
BEGIN
    ALTER DATABASE test SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE test;
END;

$ sql-server-execute name=sql-server split-lines=false
USE msdb;
IF EXISTS (
    SELECT 1
    FROM msdb.dbo.sysjobs
    WHERE name = N'DummyTicker'
)
BEGIN
    EXEC sp_delete_job @job_name = N'DummyTicker';
END;

$ sql-server-execute name=sql-server
CREATE DATABASE test COLLATE Latin1_General_100_CI_AI_SC_UTF8;
USE test;

EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON;

CREATE TABLE dummy (t datetime);
INSERT INTO dummy VALUES (CURRENT_TIMESTAMP);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dummy', @role_name = 'SA', @supports_net_changes = 0;

$ sql-server-execute name=sql-server split-lines=false
CREATE OR ALTER PROCEDURE dbo.DummyTicker AS
BEGIN
    SET NOCOUNT ON;
    WHILE 1 = 1
    BEGIN
        UPDATE dbo.dummy SET t = CURRENT_TIMESTAMP;
        WAITFOR DELAY '00:00:01';
    END
END;

$ sql-server-execute name=sql-server
USE msdb;
EXEC sp_add_job @job_name = N'DummyTicker', @enabled = 1;
EXEC sp_add_jobstep @job_name = N'DummyTicker', @step_name = N'run', @subsystem = N'TSQL', @database_name = N'test', @command = N'EXEC dbo.DummyTicker;';
EXEC sp_add_jobserver @job_name = N'DummyTicker';
EXEC msdb.dbo.sp_start_job N'DummyTicker';
