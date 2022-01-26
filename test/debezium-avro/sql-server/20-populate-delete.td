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
CREATE TABLE delete_table_pk (key_col VARCHAR(20) PRIMARY KEY, value_col VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'delete_table_pk', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE delete_table_nopk (key_col VARCHAR(20), value_col VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'delete_table_nopk', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO delete_table_pk VALUES ('key1', 'value1');
INSERT INTO delete_table_pk VALUES ('key2', 'value2');

INSERT INTO delete_table_nopk VALUES ('key1', 'value1');
INSERT INTO delete_table_nopk VALUES ('key2', 'value2');
