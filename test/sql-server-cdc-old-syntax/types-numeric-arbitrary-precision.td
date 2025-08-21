# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Arbitrary-precision NUMERIC supported only as TEXT
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
CREATE TABLE t1 (f1 NUMERIC(30,29));

INSERT INTO t1 VALUES (NULL), ('0.0000000000000000000000000001');

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE t2 (f1 NUMERIC(30,0));

INSERT INTO t2 VALUES (NULL), ('-111111111111111111111111111111');

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ sql-server-execute name=sql-server
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1) FROM t1 LIMIT 1;
"numeric"

> SELECT * FROM t1;
<null>
<null>
0.0000000000000000000000000001
0.0000000000000000000000000001

> SELECT COUNT(*) > 0 FROM t2;
true

# Insert the same data post-snapshot
$ sql-server-execute name=sql-server
INSERT INTO t2 SELECT * FROM t2;

> SELECT pg_typeof(f1) FROM t2 LIMIT 1;
"numeric"

> SELECT * FROM t2;
<null>
<null>
-111111111111111111111111111111
-111111111111111111111111111111
