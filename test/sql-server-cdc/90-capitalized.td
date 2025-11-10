# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Setup SQL Server state.
#
# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE T1 (C1 decimal(20, 10), C2 time, C3 money, C4 varbinary(100));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'T1', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO T1 VALUES (1.444889, '12:00:00', '$100.99', 0x1100AB);

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_columns_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE "T1_sql_server"
  FROM SQL SERVER CONNECTION sql_server_columns_connection (
    TEXT COLUMNS (dbo."T1"."C1", dbo."T1"."C2", dbo."T1"."C4"),
    EXCLUDE COLUMNS (dbo."T1"."C3")
  )
  FOR TABLES (dbo."T1");

> SHOW CREATE SOURCE "T1_sql_server";
materialize.public.T1_sql_server "CREATE SOURCE materialize.public.\"T1_sql_server\"\nIN CLUSTER quickstart\nFROM\n    SQL SERVER CONNECTION materialize.public.sql_server_columns_connection (TEXT COLUMNS = (dbo.\"T1\".\"C1\", dbo.\"T1\".\"C2\", dbo.\"T1\".\"C4\"), EXCLUDE COLUMNS = (dbo.\"T1\".\"C3\"))\nFOR TABLES (test.dbo.\"T1\" AS materialize.public.\"T1\")\nEXPOSE PROGRESS AS materialize.public.\"\"\"T1_sql_server\"\"_progress\";"

> SHOW COLUMNS FROM "T1";
C1 true text ""
C2 true text ""
C4 true text ""

> SELECT "C1", "C2", "C4" FROM "T1";
1.4448890000 12:00:00 EQCr
