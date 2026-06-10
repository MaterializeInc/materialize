# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test progress statistics
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Datbase=test

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
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000
ALTER SYSTEM SET max_credit_consumption_rate = 2000

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1 (f1 VARCHAR(128));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t1 VALUES ('one');
INSERT INTO t1 VALUES ('two');

> CREATE CLUSTER stats_cluster SIZE 'scale=1,workers=1'

> CREATE SOURCE mz_source
  IN CLUSTER stats_cluster
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES ("t1")

> SELECT COUNT(*) > 0 FROM t1;
true

> SELECT
    s.name,
    SUM(u.offset_committed) > 0,
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 2 2

$ set-from-sql var=previous-offset-committed
SELECT
    (SUM(u.offset_committed))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source')

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES ('three');

> SELECT
    s.name,
    SUM(u.offset_committed) > ${previous-offset-committed},
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 2 2

$ set-from-sql var=pre-restart-offset-committed
SELECT
    (SUM(u.offset_committed))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source')

# capture the current replica_id for the source statistics
$ set-regex match=u\d+ replacement="<REPLICAID>"
> SELECT cr.id
  FROM
    mz_clusters c,
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics u,
    mz_sources s
  WHERE
    c.name = 'stats_cluster' AND c.id = cr.cluster_id AND cr.id = u.replica_id
    AND s.name IN ('mz_source') AND u.id = s.id
  ORDER BY cr.id
  LIMIT 1
<REPLICAID>

$ set-from-sql var=old_replica_id
SELECT cr.id
  FROM
    mz_clusters c,
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics u,
    mz_sources s
  WHERE
    c.name = 'stats_cluster' AND c.id = cr.cluster_id AND cr.id = u.replica_id
    AND s.name IN ('mz_source') AND u.id = s.id
  ORDER BY cr.id
  LIMIT 1

$ unset-regex

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 0)

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES ('four');

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 1)

# capture the new replica_id
$ set-regex match=u\d+ replacement="<REPLICAID>"
> SELECT cr.id
  FROM
    mz_clusters c,
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics u,
    mz_sources s
  WHERE
    c.name = 'stats_cluster' AND c.id = cr.cluster_id AND cr.id = u.replica_id
    AND s.name IN ('mz_source') AND u.id = s.id
  ORDER BY cr.id
  LIMIT 1
<REPLICAID>

$ set-from-sql var=replica_id
SELECT cr.id
  FROM
    mz_clusters c,
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics u,
    mz_sources s
  WHERE
    c.name = 'stats_cluster' AND c.id = cr.cluster_id AND cr.id = u.replica_id
    AND s.name IN ('mz_source') AND u.id = s.id
  ORDER BY cr.id
  LIMIT 1

$ unset-regex

# Ensure the snapshot stats stay there, and don't change for the original replica
# and the new replica has no snapshot stats as no snapshots needed.
> SELECT
    s.name,
    u.replica_id,
    SUM(u.offset_committed) > ${pre-restart-offset-committed},
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name, u.replica_id
  ORDER BY s.name, u.replica_id
mz_source "${old_replica_id}" false true 2 2
mz_source "${replica_id}" true true <null> <null>

$ sql-server-execute name=sql-server
CREATE TABLE alt (f1 VARCHAR(128));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alt', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alt VALUES ('one');

> ALTER SOURCE mz_source ADD SUBSOURCE alt;

> SELECT COUNT(*) > 0 FROM alt;
true

# Ensure snapshot stats are overridden when we add a new table
> SELECT
     s.name,
     SUM(u.snapshot_records_known),
     SUM(u.snapshot_records_staged)
   FROM mz_sources s
   JOIN mz_internal.mz_source_statistics u ON s.id = u.id
   WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
   GROUP BY s.name
   ORDER BY s.name
 mz_source 1 1

# Ensure subsource stats show up, and then are removed when we drop subsources.
> SELECT
    s.name,
    SUM(u.updates_committed) > 0
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('alt') AND u.replica_id = '${replica_id}'
  GROUP BY s.name
  ORDER BY s.name
alt true

> DROP SOURCE alt;

> SELECT
    count(*)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('alt') AND u.replica_id = '${replica_id}'
0
