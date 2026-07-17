# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Setup for the Azure SQL Database happy-path workflow.
#
# Unlike the on-prem setup, this operates inside the database named in the
# connection (no CREATE DATABASE / USE, no msdb, no SQL Server Agent job). The
# provided account must be a member of db_owner so it can enable CDC and manage
# the test objects.

# A serverless database that has auto-paused reports error 40613 ("not
# currently available") on the first connection while it resumes, which takes
# up to a few minutes. Retry steadily until it comes online.
$ sql-server-connect name=sql-server retry-initial-backoff=10 retry-clamp-backoff=30 retry-max-duration=240
server=tcp:${arg.azure-sql-host},${arg.azure-sql-port};Encrypt=true;TrustServerCertificate=true;User ID={${arg.azure-sql-user}};Password={${arg.azure-sql-password}};Database=${arg.azure-sql-database}

# Drop objects left over from a previous run so the workflow is idempotent
# against a persistent database.
$ sql-server-execute name=sql-server split-lines=false
IF OBJECT_ID('dbo.mz_azure_t1', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.mz_azure_t1'))
        EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'mz_azure_t1', @capture_instance = 'all';
    DROP TABLE dbo.mz_azure_t1;
END

$ sql-server-execute name=sql-server split-lines=false
IF OBJECT_ID('dbo.mz_azure_ci', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.mz_azure_ci'))
        EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'mz_azure_ci', @capture_instance = 'all';
    DROP TABLE dbo.mz_azure_ci;
END

$ sql-server-execute name=sql-server split-lines=false
IF OBJECT_ID('dbo.mz_azure_priv', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.mz_azure_priv'))
        EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'mz_azure_priv', @capture_instance = 'all';
    DROP TABLE dbo.mz_azure_priv;
END

$ sql-server-execute name=sql-server split-lines=false
IF OBJECT_ID('dbo.mz_azure_ticker', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.mz_azure_ticker'))
        EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'mz_azure_ticker', @capture_instance = 'all';
    DROP TABLE dbo.mz_azure_ticker;
END

$ sql-server-execute name=sql-server
DROP TABLE IF EXISTS dbo.mz_azure_ticker_control;

# Drop the provisioned users before the role so the role has no members.
$ sql-server-execute name=sql-server split-lines=false
DROP USER IF EXISTS mz_ci_denied;
DROP USER IF EXISTS mz_ci_materialize;
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'mz_ci_role' AND type = 'R')
    DROP ROLE mz_ci_role;

# Enable CDC at the database level. On Azure SQL Database the capture is driven
# by an internal scheduler, so no SQL Server Agent job is required.
$ sql-server-execute name=sql-server split-lines=false
IF (SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) = 0
    EXEC sys.sp_cdc_enable_db;

$ sql-server-execute name=sql-server
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;

# A CDC-enabled table that a background ticker constantly updates, so the
# database-wide max LSN keeps advancing. Without this the source stalls at
# startup waiting for the LSN to move (Azure SQL Database has no SQL Server
# Agent to run the equivalent of the on-prem DummyTicker job). The `keep_running`
# flag lets the workflow stop the ticker loop cleanly.
$ sql-server-execute name=sql-server
CREATE TABLE mz_azure_ticker (t DATETIME2);
INSERT INTO mz_azure_ticker VALUES (SYSUTCDATETIME());
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'mz_azure_ticker', @role_name = NULL, @supports_net_changes = 0;
CREATE TABLE mz_azure_ticker_control (keep_running BIT NOT NULL);
INSERT INTO mz_azure_ticker_control VALUES (1);
