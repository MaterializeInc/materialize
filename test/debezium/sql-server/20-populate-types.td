# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test some fringe types specific to SQL Server
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE types_table (decimal_col DECIMAL(5,2), datetimeoffset_col DATETIMEOFFSET(4), datetime2_col DATETIME2(7), smalldatetime_col SMALLDATETIME, bit_col BIT, pk INTEGER PRIMARY KEY);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'types_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO types_table VALUES (123.45, '12-10-25 12:32:10 +01:00', '2010-11-12 13:14:15.1234567', '11:12:13', 1, 0);
