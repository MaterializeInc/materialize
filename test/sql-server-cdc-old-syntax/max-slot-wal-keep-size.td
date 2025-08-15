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
CREATE TABLE t1 (pk INT IDENTITY(1,1) PRIMARY KEY, f2 varchar(2048));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

> CREATE CLUSTER storage REPLICAS (r1 (SIZE 'scale=1,workers=1'))

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

# Wait for the initial snapshot to be ingested
> SELECT * FROM t1

# Stop ingestion by dropping the replica
> DROP CLUSTER REPLICA storage.r1;

# Now generate more than 10MB of WAL data so that the slot gets invalidated
$set count=200000
$ sql-server-execute name=sql-server
WITH numbers AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM numbers WHERE x + 1 <= ${count}) INSERT INTO t1 (f2) SELECT REPLICATE('0123456789abcdef', 64) FROM numbers OPTION (MAXRECURSION 0);
UPDATE t1 SET f2 = CONCAT(f2, pk);
UPDATE t1 SET f2 = CONCAT(f2, pk);

# Resume the ingestion by adding a replica to the cluster.
> CREATE CLUSTER REPLICA storage.r1 SIZE = 'scale=1,workers=1';

$ set-sql-timeout duration=60s
> SELECT count(*) FROM t1;
200000
