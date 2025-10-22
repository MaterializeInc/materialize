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

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE sneaky (id int, ts_start datetime2 generated always as row start hidden, ts_end datetime2 generated always as row end hidden, period for system_time (ts_start, ts_end));

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sneaky', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO sneaky VALUES (1), (2), (2), (3), (3), (3)

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE sneaky_src FROM SQL SERVER CONNECTION sql_server_conn;

> CREATE TABLE sneaky FROM SOURCE sneaky_src (REFERENCE dbo.sneaky);

> SELECT id, count(*) FROM sneaky GROUP BY id;
1 1
2 2
3 3

# excluding the columns allows successful configuration
> CREATE TABLE sneaky_ex FROM SOURCE sneaky_src (REFERENCE dbo.sneaky) WITH (EXCLUDE COLUMNS = (ts_end), TEXT COLUMNS = (ts_start));

> SELECT id, count(*) FROM sneaky_ex GROUP BY id;
1 1
2 2
3 3

$ sql-server-execute name=sql-server
DELETE FROM sneaky WHERE id = 3;
UPDATE sneaky SET id = 5 WHERE id = 2;

> SELECT id, count(*) FROM sneaky GROUP BY id;
1 1
5 2

> SELECT id, count(*) FROM sneaky_ex GROUP BY id;
1 1
5 2
