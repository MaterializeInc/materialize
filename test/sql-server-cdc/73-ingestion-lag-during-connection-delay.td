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

# Snapshot before stalling anything. Only a source that has durably committed
# has a resumption LSN above the minimum, and only then does the seeded
# `offset_committed` differ from the seeded `offset_known`.
> CREATE CLUSTER REPLICA storage.r1 SIZE = 'scale=1,workers=1'

> SELECT * FROM t73
one

> DROP CLUSTER REPLICA storage.r1

# Stall the reconnect. Establishing a connection takes several round trips, so
# the replica below stays stalled for well over a minute.
$ http-request method=POST url=http://toxiproxy:8474/proxies/sql-server/toxics content-type=application/json
{
  "name": "startup-delay",
  "type": "latency",
  "attributes": { "latency": 30000 }
}

> CREATE CLUSTER REPLICA storage.r1 SIZE = 'scale=1,workers=1'

$ set-from-sql var=replica_id
SELECT cr.id
  FROM mz_clusters c JOIN mz_cluster_replicas cr ON c.id = cr.cluster_id
  WHERE c.name = 'storage' AND cr.name = 'r1'

# The persist sink publishes this row without touching the upstream, so it
# appears during the stall.
> SELECT COUNT(*)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 's' AND u.replica_id = '${replica_id}'
1

# `force` is required: without it the timeout is raised to the default, and
# retrying past the stall would read the settled post-connect state instead.
$ set-sql-timeout duration=5s force=true

> SELECT u.offset_known >= u.offset_committed
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 's' AND u.replica_id = '${replica_id}'
true

$ set-sql-timeout duration=default force=true

> DROP SOURCE s CASCADE
> DROP CLUSTER storage CASCADE
> DROP CONNECTION conn
> DROP SECRET pass

$ http-request method=DELETE url=http://toxiproxy:8474/proxies/sql-server
