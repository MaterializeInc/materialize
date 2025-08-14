# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that any replication slot can only be materialized once
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;

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


# Insert data pre-snapshot
$ sql-server-execute name=sql-server
CREATE TABLE t1 (id INT IDENTITY(1,1) PRIMARY KEY, f1 BIT);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE t2 (id INT IDENTITY(1,1) PRIMARY KEY, t1_id INT REFERENCES t1(id), name VARCHAR(256));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1(f1) VALUES ('true'),('false');

INSERT INTO t2(t1_id, name) VALUES (1, 'example');

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> CREATE MATERIALIZED VIEW t1_mat AS
  SELECT * FROM t1

> SELECT id, f1 FROM t1_mat;
1 true
2 false

> CREATE MATERIALIZED VIEW t1_mat_dupe AS
  SELECT * FROM t1

> DROP MATERIALIZED VIEW t1_mat;

> DROP SOURCE mz_source CASCADE;

# verify that dropping things allows recreation

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> CREATE MATERIALIZED VIEW joiner AS
  SELECT t2.id, t1.f1, t2.name
  FROM t1
  JOIN t2
  ON   t1.id = t2.t1_id

> SELECT * FROM joiner;
1 true example