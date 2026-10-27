# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test: ALTER SOURCE ADD SUBSOURCE silently drops existing
# TEXT/EXCLUDE COLUMNS on SQL Server sources due to 2-part vs 3-part
# name mismatch (schema.table vs database.schema.table).

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1_retain (f1 INTEGER, f2 decimal(20, 10), f3 money);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1_retain', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t1_retain VALUES (1, 1.5, '$10.50');
CREATE TABLE t2_retain (val VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2_retain', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t2_retain VALUES ('hello');

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_retain_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE retain_src
  FROM SQL SERVER CONNECTION sql_server_retain_conn (
    TEXT COLUMNS (dbo.t1_retain.f2),
    EXCLUDE COLUMNS (dbo.t1_retain.f3)
  )
  FOR TABLES (dbo.t1_retain);

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE retain_src);
dbo.t1_retain.f2

# Add a second table. This must not drop the existing column options.
> ALTER SOURCE retain_src ADD SUBSOURCE dbo.t2_retain;

# TEXT COLUMNS and EXCLUDE COLUMNS for t1 must survive the ALTER.
> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE retain_src);
dbo.t1_retain.f2

> SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE retain_src);
dbo.t1_retain.f3

> DROP SOURCE retain_src CASCADE;
