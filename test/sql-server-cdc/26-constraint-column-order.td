# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_26_connection CASCADE
> CREATE CONNECTION sql_server_test_26_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

$ sql-server-execute name=sql-server
CREATE TABLE t26_pk_order (a_col INT NOT NULL, val INT, z_col INT NOT NULL, PRIMARY KEY (z_col, a_col));
INSERT INTO t26_pk_order VALUES (1, 10, 100);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't26_pk_order', @role_name = 'SA', @supports_net_changes = 0;

> BEGIN;
> CREATE SOURCE t26_source FROM SQL SERVER CONNECTION sql_server_test_26_connection;
> CREATE TABLE t26_pk_order FROM SOURCE t26_source (REFERENCE t26_pk_order);
> COMMIT;

> SELECT create_sql LIKE '%PRIMARY KEY ("z_col", "a_col")%' FROM mz_tables WHERE name = 't26_pk_order';
true

> SELECT * FROM t26_pk_order;
1 10 100
