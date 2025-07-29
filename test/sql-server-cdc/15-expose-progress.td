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
DROP DATABASE IF EXISTS test_15;
CREATE DATABASE test_15;
USE test_15;

EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test_15 SET ALLOW_SNAPSHOT_ISOLATION ON;

CREATE TABLE t15_pk (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't15_pk', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t15_pk VALUES ('a', 'hello world'), ('b', 'foobar'), ('c', 'anotha one');

$ sql-server-stop-capture-jobs name=sql-server

$ sql-server-run-cdc-scan name=sql-server

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;

> CREATE CONNECTION sql_server_test_15_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test_15,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

# Create a SQL Server Source.

> CREATE SOURCE t15_pk_sql_server
  FROM SQL SERVER CONNECTION sql_server_test_15_connection
  FOR ALL TABLES
  EXPOSE PROGRESS AS t15_renamed_progress_relation;

> SELECT name FROM mz_sources WHERE name LIKE 't15%';
t15_pk
t15_pk_sql_server
t15_renamed_progress_relation

> SHOW COLUMNS FROM t15_renamed_progress_relation;
lsn true bytea ""
