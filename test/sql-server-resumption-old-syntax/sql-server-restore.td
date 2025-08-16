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
    RESTORE DATABASE test FROM DISK = N'${arg.backup-directory}/test.bak' WITH MOVE N'test' TO N'${arg.backup-directory}/test.mdf', REPLACE, STATS = 5
END;

DECLARE @counter int = 0;
WHILE @counter < 30
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.databases 
        WHERE name = 'test' 
            AND state_desc = 'ONLINE' 
            AND user_access_desc = 'MULTI_USER'
    )
    BEGIN
        -- Try to access the database
        USE test;
        SELECT 'Database is ready' as status;
        BREAK;
    END
    
    SET @counter = @counter + 1;
    WAITFOR DELAY '00:00:01';
END;
