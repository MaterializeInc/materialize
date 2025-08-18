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
    DECLARE @kill varchar(8000) = '';
    SELECT @kill = @kill + 'KILL ' + CONVERT(varchar(5), session_id) + ';'
    FROM sys.dm_exec_sessions
    WHERE database_id = DB_ID('test')
        AND session_id != @@SPID
        AND session_id > 50  -- Exclude system processes (typically <= 50)
        AND is_user_process = 1;

    IF LEN(@kill) > 0
    BEGIN
        EXEC(@kill);
        WAITFOR DELAY '00:00:02';
    END;

    DECLARE @attempts int = 0;
    DECLARE @success bit = 0;

    WHILE @attempts < 3 AND @success = 0
    BEGIN
        BEGIN TRY
            ALTER DATABASE test SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            RESTORE DATABASE test FROM DISK = N'${arg.backup-directory}/test.bak' WITH MOVE N'test' TO N'${arg.backup-directory}/test.mdf', REPLACE, STATS = 5;
            ALTER DATABASE test SET MULTI_USER;
            SET @success = 1;
        END TRY
        BEGIN CATCH
            SET @attempts = @attempts + 1;
            WAITFOR DELAY '00:00:05';
        END CATCH
    END;
END;
