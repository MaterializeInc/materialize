# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Make sure MZ is escaping identifiers

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE [quote ident] ([i]]d] int, [it's] int, ["it is"] int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'quote ident', @role_name = 'SA', @capture_instance = 'w[]t', @supports_net_changes = 0;

INSERT INTO [quote ident] VALUES (1,1,1), (2,2,2), (3,3,3);

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE mz_src FROM SQL SERVER CONNECTION sql_server_conn;

> CREATE TABLE "quote ident" FROM SOURCE mz_src (REFERENCE dbo."quote ident");

> SELECT * FROM "quote ident"
1 1 1
2 2 2
3 3 3

$ sql-server-execute name=sql-server
DELETE FROM [quote ident] WHERE [i]]d] = 3;
UPDATE [quote ident] SET [it's] = 5 WHERE [i]]d] = 2;

> SELECT * FROM "quote ident";
1 1 1
2 5 2
