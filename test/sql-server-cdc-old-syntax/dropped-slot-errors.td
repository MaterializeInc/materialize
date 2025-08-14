# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

$ sql-server-execute name=sql-server
CREATE TABLE t1 (pk INT IDENTITY(1,1) PRIMARY KEY, f2 varchar(128));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

> CREATE CLUSTER storage REPLICAS (r1 (SIZE 'scale=1,workers=1'))

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES

# Wait for the initial snapshot to be ingested
> SELECT * FROM t1

# Stop ingestion by dropping the replica
> DROP CLUSTER REPLICA storage.r1;

# Now drop the replication slot
$ sql-server-execute name=sql-server
EXEC sys.sp_cdc_disable_db;

# Resume the ingestion by adding a replica to the cluster.
> CREATE CLUSTER REPLICA storage.r1 SIZE 'scale=1,workers=1';

> SELECT error ~~ 'sql-server: into results: Token error: ''Invalid object name ''cdc.lsn_time_mapping''.'' on server % executing  on line 1 (code: %, state: %, class: %)' FROM mz_internal.mz_source_statuses WHERE name = 't1';
true