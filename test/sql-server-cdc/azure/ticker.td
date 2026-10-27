# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Background ticker: continuously update a CDC-enabled table so the
# database-wide max LSN keeps advancing, standing in for the on-prem DummyTicker
# SQL Server Agent job (Azure SQL Database has no Agent). Runs in its own
# testdrive process until the workflow flips `keep_running` off, with a bounded
# iteration count as a fallback so it always terminates on its own.

$ sql-server-connect name=ticker
server=tcp:${arg.azure-sql-host},${arg.azure-sql-port};Encrypt=true;TrustServerCertificate=true;User ID={${arg.azure-sql-user}};Password={${arg.azure-sql-password}};Database=${arg.azure-sql-database}

$ sql-server-execute name=ticker split-lines=false
DECLARE @i INT = 0;
WHILE (SELECT keep_running FROM dbo.mz_azure_ticker_control) = 1 AND @i < 900
BEGIN
    UPDATE dbo.mz_azure_ticker SET t = SYSUTCDATETIME();
    WAITFOR DELAY '00:00:01';
    SET @i += 1;
END
