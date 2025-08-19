# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

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
CREATE TABLE t1 (pk INT IDENTITY(1,1) PRIMARY KEY, f2 BIGINT);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
WITH numbers AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM numbers WHERE x + 1 <= 10000) INSERT INTO t1 (f2) SELECT x FROM numbers OPTION (MAXRECURSION 0);

# Create a cluster with no replicas so that we have time to submit queries at the minimum frontier.
> CREATE CLUSTER storage REPLICAS ()

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES
  WITH (RETAIN HISTORY = FOR '365000 days');

# Grab a cursor at timestamp 0.
> BEGIN
> DECLARE c CURSOR FOR SELECT 1, COUNT(*) FROM t1 AS OF 0

# Start ingestion by adding a replica to the cluster. We must do this from a
# different connection to not disturbe the transaction we're in.
$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
CREATE CLUSTER REPLICA storage.r1 SIZE = 'scale=1,workers=1';

# Verify that at timestamp 0 there were no rows replicated due to the fact that there was no data at timestamp 0.
> FETCH 1 c WITH (timeout = '1d');
1 0

> COMMIT

# Verify that the data does appear as the frontier advances to include the data that was snapshotted.
> SELECT COUNT(*) FROM t1;
10000
