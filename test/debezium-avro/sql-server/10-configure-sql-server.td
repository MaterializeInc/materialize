# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# It seems that there is a race condition on SQL Server startup that does not allow CDC to be enabled immediately. The following error is returned instead:
# > 'Could not update the metadata that indicates database test is enabled for Change Data Capture.
# > The failure occurred when executing the command 'sp_addrolemember 'db_owner', 'cdc''.
# > The error returned was 1205: 'Transaction (Process ID 52) was deadlocked on lock resources with another process and has been chosen as the deadlock victim.
# > Rerun the transaction.'. Use the action and error to determine the cause of the failure and resubmit the request.'
# > on server 67f36c260951 executing sys.sp_cdc_enable_db_internal on line 205 (code: 22830, state: 1, class: 16)
#

> SELECT mz_internal.mz_sleep(10);
<null>

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ sql-server-execute name=sql-server
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
EXEC sys.sp_cdc_enable_db;

# I could not get the Debezium signal mechanism to work -- Debezium throws a NullPointerException
# Therefore, we are forced to populate all tables first as we can not add new tables to the replication stream post-factum

# CREATE TABLE debezium_signal (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL);
# EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'debezium_signal', @role_name = 'SA', @supports_net_changes = 0
