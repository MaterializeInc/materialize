# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


# Validates that SQL Server returns an error if CDC events are cleaned up before
# being processsed.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1 (pk INT IDENTITY(1,1) PRIMARY KEY, f2 varchar(2048));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t1 VALUES ('apple');

> CREATE CLUSTER storage REPLICAS (r1 (SIZE 'scale=1,workers=1'))

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (t1);

# Wait for the initial snapshot to be ingested
> SELECT * FROM t1
1 apple

# Stop ingestion by dropping the replica
> DROP CLUSTER REPLICA storage.r1;

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES ('bottle');
INSERT INTO t1 VALUES ('cork');

# give CDC chance to progress the max LSN before we clear the table
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

# Cleanup the change events
$ sql-server-execute name=sql-server split-lines=false
DECLARE @max_lsn binary(10) = sys.fn_cdc_get_max_lsn();
EXEC sys.sp_cdc_cleanup_change_table
    @capture_instance = N'dbo_t1',
    @low_water_mark = @max_lsn;

# Resume the ingestion by adding a replica to the cluster.
> CREATE CLUSTER REPLICA storage.r1 SIZE = 'scale=1,workers=1';

! SELECT * FROM t1;
contains:Overcompacted

> DROP SOURCE mz_source CASCADE;
