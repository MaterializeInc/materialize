
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Validate that when multiple capture instances exist for a table,
# we select the one with the highest LSN (most recent CDC tracking).
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1 (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));

INSERT INTO t1 VALUES ('a', 'hello world'), ('b', 'foobar'), ('c', 'anotha one');

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'first';

# Increase start_lsn for next capture instance.
INSERT INTO t1 VALUES ('d', 'after cdc'), ('e', 'also after'), ('f', 'one more');

# Drop column before creating second capture instance to test that we pick the
# capture instance with the higher LSN (second) which has the updated schema.
ALTER TABLE t1 DROP COLUMN val_col;

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'second';

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

> CREATE SOURCE t1_pk_sql_server
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES(t1);

> SELECT * FROM t1;
a
b
c
d
e
f

$ sql-server-execute name=sql-server
USE test;
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 't1', @capture_instance = 'first';

INSERT INTO t1 VALUES ('g'), ('h'), ('i');

> SELECT * FROM t1;
a
b
c
d
e
f
g
h
i
