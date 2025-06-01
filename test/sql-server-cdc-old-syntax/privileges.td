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
CREATE USER priv WITH PASSWORD 'priv';
ALTER USER priv WITH replication;

CREATE SCHEMA other;
CREATE TABLE other.s (a int);
GRANT ALL PRIVILEGES ON SCHEMA other TO priv;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA other TO priv;

CREATE TABLE public.t (a int);
GRANT ALL PRIVILEGES ON SCHEMA public TO priv;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO priv;

REVOKE CONNECT ON DATABASE postgres FROM public;

#
# CONNECT error
#

! CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  )
contains:permission denied for database "postgres"

#
# USAGE error
#

$ sql-server-execute name=sql-server
GRANT CONNECT ON DATABASE postgres TO public;
REVOKE USAGE ON SCHEMA other FROM priv;

! CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  )

! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR SCHEMAS(public, other);
contains:insufficient privileges
detail:user priv lacks USAGE privileges for schemas other

#
# SELECT errors
#

$ sql-server-execute name=sql-server
GRANT ALL PRIVILEGES ON SCHEMA other TO priv;
REVOKE SELECT ON TABLE other.s FROM priv;

! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR SCHEMAS(public, other);
contains:insufficient privileges
detail:user priv lacks SELECT privileges for tables other.s

$ sql-server-execute name=sql-server
CREATE TABLE "select" (a INT);
REVOKE SELECT ON public.select FROM priv;
CREATE TABLE """select""" (a INT);
REVOKE SELECT ON public."""select""" FROM priv;

! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR SCHEMAS(public);
contains:insufficient privileges
detail:user priv lacks SELECT privileges for tables public."""select""", public."select"

$ sql-server-execute name=sql-server
DROP SCHEMA IF EXISTS other CASCADE;
