# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE transaction_table1 (f1 VARCHAR(20) PRIMARY KEY);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'transaction_table1', @role_name = 'SA', @supports_net_changes = 0;
CREATE TABLE transaction_table2 (f1 VARCHAR(20) PRIMARY KEY);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'transaction_table2', @role_name = 'SA', @supports_net_changes = 0;

# Unlike Postgres, SQL server requires that each line is a fully-formed transaction
# so we can not have interleaved transaction using the current testdrive capabilities
$ sql-server-execute name=sql-server
USE test;
BEGIN TRANSACTION INSERT INTO transaction_table1 VALUES ('t1-trans1-event1'); INSERT INTO transaction_table2 VALUES ('t2-trans1-event1'); INSERT INTO transaction_table1 VALUES ('t1-trans1-event2'); INSERT INTO transaction_table2 VALUES ('t2-trans1-event2'); COMMIT;
BEGIN TRANSACTION INSERT INTO transaction_table1 VALUES ('t1-trans2-event1'); INSERT INTO transaction_table2 VALUES ('t2-trans2-event1'); INSERT INTO transaction_table1 VALUES ('t1-trans2-event2'); INSERT INTO transaction_table2 VALUES ('t2-trans2-event2'); COMMIT;
