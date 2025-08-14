# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that identically-named tables in two source schemas can be
# successfully disambiguated and replicated
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

$ sql-server-execute name=sql-server
DROP SCHEMA IF EXISTS schema1;
CREATE SCHEMA schema1;

CREATE TABLE schema1.t1 (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'schema1', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO schema1.t1 VALUES (1);

DROP SCHEMA IF EXISTS schema2;
CREATE SCHEMA schema2;

CREATE TABLE schema2.t1 (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'schema2', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO schema2.t1 VALUES (2);

$ sql-server-execute name=sql-server
INSERT INTO schema1.t1 SELECT * FROM schema1.t1;
INSERT INTO schema2.t1 SELECT * FROM schema2.t1;

! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;
contains:multiple subsources would be named t1

! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1);
contains:reference t1 is ambiguous, consider specifying an additional layer of qualification

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (schema1.t1);

> SELECT * FROM t1;
1
1

> DROP SOURCE mz_source CASCADE;

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (schema1.t1 AS t1_1, schema2.t1 AS t1_2);

> SELECT * FROM t1_1;
1
1

> SELECT * FROM t1_2;
2
2
