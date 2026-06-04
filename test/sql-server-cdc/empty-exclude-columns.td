# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for ss-159: an empty `EXCLUDE COLUMNS = ()` on a
# `CREATE TABLE .. FROM SOURCE` statement is treated as though the option
# were omitted and must succeed.
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE foo (id INT PRIMARY KEY, name VARCHAR(1024));
INSERT INTO foo VALUES (1, 'one'), (2, 'two');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION ssconn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE s FROM SQL SERVER CONNECTION ssconn;

> CREATE TABLE foo FROM SOURCE s (REFERENCE dbo.foo) WITH (EXCLUDE COLUMNS = ());

> SELECT * FROM foo;
1 one
2 two

> DROP SOURCE s CASCADE;
