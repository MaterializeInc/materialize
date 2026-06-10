# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that two capture instances can replicate the same table
#

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


$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1 (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'cap1';

INSERT INTO t1 VALUES (1);

> DROP SCHEMA IF EXISTS schema1

> CREATE SCHEMA schema1

> CREATE SOURCE mz_source1
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1 AS t1_1);

# we add a column to validate that the t1_2 is using cap2, cap1 will not see this column.
$ sql-server-execute name=sql-server
ALTER TABLE t1 ADD f2 INTEGER;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'cap2';

> CREATE SOURCE mz_source2
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1 AS t1_2);

> SELECT * FROM t1_1;
1

> SELECT * FROM t1_2;
1 <null>

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES (2, 2);

> SELECT * FROM t1_1;
1
2

> SELECT * FROM t1_2;
1 <null>
2 2

> DROP SCHEMA schema1 CASCADE;
