# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Hold the first runtime connection open and verify that the ingestion-lag
# calculation remains valid before `offset_known` is initialized.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t73 (f1 VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't73', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t73 VALUES ('one');

$ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
{
  "name": "sql-server",
  "listen": "0.0.0.0:1433",
  "upstream": "sql-server:1433",
  "enabled": true
}

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000;
ALTER SYSTEM SET storage_statistics_interval = 2000;

> CREATE CLUSTER storage REPLICAS ()
> CREATE SECRET pass AS '${arg.default-sql-server-password}'
> CREATE CONNECTION conn TO SQL SERVER (
    HOST 'toxiproxy',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET pass
  )
> CREATE SOURCE s IN CLUSTER storage
  FROM SQL SERVER CONNECTION conn
  FOR TABLES (dbo.t73)

# Stall the first connection past several statistics intervals.
$ http-request method=POST url=http://toxiproxy:8474/proxies/sql-server/toxics content-type=application/json
{
  "name": "startup-delay",
  "type": "latency",
  "attributes": { "latency": 30000 }
}

> CREATE CLUSTER REPLICA storage.r1 SIZE = 'scale=1,workers=1'

# The correct state remains zero, so there is no nonzero value to poll for.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="5s"

$ set-sql-timeout duration=10s

> SELECT u.offset_known - u.offset_committed
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 's'
0
