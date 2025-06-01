# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'test') BEGIN ALTER DATABASE test SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE test; END;
CREATE DATABASE test;
USE test;
EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;


$ sql-server-execute name=sql-server
CREATE TABLE t (f1 INT);
INSERT INTO t VALUES (1);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE other.t (f1 INT);
INSERT INTO other.t VALUES (1);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'other.t', @role_name = 'SA', @supports_net_changes = 0;

! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;
contains:multiple subsources would be named t
detail:referenced tables with duplicate name: postgres.other.t, postgres.public.t

! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (public.t AS x, public.t as Y);
contains:multiple subsources refer to table postgres.public.t
detail: subsources referencing table: x, y

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR SCHEMAS (other);

> SHOW sources
 mz_source          postgres  quickstart  ""
 mz_source_progress progress  <null>      ""
 t                  subsource quickstart  ""
