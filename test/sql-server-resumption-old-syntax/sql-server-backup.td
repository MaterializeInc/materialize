# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

# A full BACKUP DATABASE is an online operation, so there is no need to put the
# database into SINGLE_USER mode. Doing so used to race with Materialize's CDC
# source reconnecting and claiming the single-user slot, which intermittently
# failed with "Database 'test' is already open and can only have one user at a
# time." (error 924). Backing up while the database stays in MULTI_USER mode
# avoids that race entirely.
$ sql-server-execute name=sql-server split-lines=false
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'test')
BEGIN
    BACKUP DATABASE test TO DISK = N'${arg.backup-directory}/test.bak';
END;
