# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# ENUM supported only as TEXT
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

# Insert data pre-snapshot
$ sql-server-execute name=sql-server
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TYPE enum1 AS ENUM ('val1', 'val2');
CREATE TABLE enum_type ( f1 enum1);
INSERT INTO enum_type VALUES ('val1'), ('val2');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'enum_type', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', TEXT COLUMNS (enum_type.f1))
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM enum_type;
true

# Insert the same data post-snapshot
$ sql-server-execute name=sql-server
INSERT INTO enum_type SELECT * FROM enum_type;

> SELECT pg_typeof(f1) FROM enum_type LIMIT 1;
text

> SELECT * FROM enum_type;
val1
val1
val2
val2
