# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that a snapshot whose table lock times out stalls the source and is retried until the
# upstream writer holding the lock commits. An open transaction on t2 blocks the shared lock
# that the snapshot takes on every table it reads.
#
# The writer's UPDATE matches no row. It holds an intent exclusive lock on t2 until the
# transaction ends, which blocks the snapshot's shared table lock. An uncommitted row would
# instead block the row count that the snapshot takes before it locks the tables.
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET sql_server_snapshot_lock_wait_timeout = '1s'

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

$ sql-server-execute name=sql-server
CREATE TABLE t1 (id int);
CREATE TABLE t2 (id int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t1 VALUES (1);
INSERT INTO t2 VALUES (1);

$ sql-server-connect name=writer
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

$ sql-server-execute name=writer
BEGIN TRANSACTION;
UPDATE t2 SET id = id WHERE id = 2;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE lock_source
  FROM SQL SERVER CONNECTION sql_server_test_connection;

> BEGIN
> CREATE TABLE t1 FROM SOURCE lock_source (REFERENCE dbo.t1);
> CREATE TABLE t2 FROM SOURCE lock_source (REFERENCE dbo.t2);
> COMMIT

> SELECT status, error LIKE '%Lock request time out period exceeded%'
  FROM mz_internal.mz_source_statuses
  WHERE name = 'lock_source';
stalled true

$ sql-server-execute name=writer
COMMIT TRANSACTION;
INSERT INTO t2 VALUES (2);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'lock_source';
running

> SELECT * FROM t1;
1

> SELECT * FROM t2;
1
2

> DROP SOURCE lock_source CASCADE;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET sql_server_snapshot_lock_wait_timeout
