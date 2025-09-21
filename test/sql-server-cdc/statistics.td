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
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
DROP DATABASE IF EXISTS test_stats;
CREATE DATABASE test_stats;
USE test_stats;

EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test_stats SET ALLOW_SNAPSHOT_ISOLATION ON;

CREATE TABLE t1 (f1 VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1 VALUES ('one');

CREATE TABLE dummy (data int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dummy', @role_name = 'SA', @supports_net_changes = 0;




$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;
ALTER SYSTEM SET storage_statistics_collection_interval = 1000;
ALTER SYSTEM SET storage_statistics_interval = 2000;

> CREATE SECRET IF NOT EXISTS mspass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION msconn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test_stats,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET mspass
  );


> CREATE CLUSTER stats_cluster SIZE '${arg.default-replica-size}'
> CREATE SOURCE mz_source IN CLUSTER stats_cluster FROM SQL SERVER CONNECTION msconn;
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE dbo.t1);


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

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'mz_source'
    AND ss.replica_id = '${replica_id}'
    AND ss.snapshot_records_staged > 0;
mz_source true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);

> SELECT COUNT(*) > 0 FROM t1;
true


# NOTE: we make sure that we have stats for a replica, otherwise the
# set-from-sql below might fail because it doesn't do retries when a row is
# missing.
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

# Find the replica that is running the source, so that the stats query can be
# very specific and not be confused by querying stats from multiple or older
# replicas.
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

> SELECT
    s.name,
    SUM(u.offset_committed) > 0,
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source', 't1') AND u.replica_id = '${replica_id}'
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 1 1
t1 true true 1 1

$ set-from-sql var=previous-offset-committed
SELECT
    (u.offset_committed)::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES ('two');

> SELECT
    s.name,
    SUM(u.offset_committed) > 0,
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source', 't1') AND u.replica_id = '${replica_id}'
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 1 1
t1 true true 1 1

$ set-from-sql var=pre-restart-offset-committed
SELECT
    (u.offset_committed)::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 0)

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES ('three');

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 1)

# Ensure the snapshot stats stay there for the old replica, and don't change.
> SELECT
    s.name,
    SUM(u.offset_committed) > 0,
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source', 't1') AND u.replica_id = '${replica_id}'
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 1 1
t1 true true 1 1

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

# The new replica will have different stats, because it never did a snapshot
# and didn't read messages.
> SELECT
    s.name,
    SUM(u.offset_committed) > ${pre-restart-offset-committed},
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
  GROUP BY s.name
mz_source true true <null> <null>


$ sql-server-execute name=sql-server
CREATE TABLE t2 (f1 VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t2 VALUES ('one');
INSERT INTO t2 VALUES ('two');


> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE dbo.t2);

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'mz_source'
    AND ss.replica_id = '${replica_id}'
    AND ss.snapshot_records_staged > 0;
mz_source true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);

> SELECT COUNT(*) > 0 FROM t2;
true

> SELECT
    s.name,
    SUM(u.offset_committed) > 0,
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source', 't1', 't2') AND u.replica_id = '${replica_id}'
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 2 2
t1 true true 0 0
t2 true true 2 2

# Ensure subsource stats show up, and then are removed when we drop subsources.
> SELECT
    t.name,
    SUM(u.updates_committed) > 0
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('t1', 't2') AND u.replica_id = '${replica_id}'
  GROUP BY t.name
  ORDER BY t.name
t1 true
t2 true

> DROP TABLE t2;

> SELECT
    t.name, count(*)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('t1', 't2') AND u.replica_id = '${replica_id}'
  GROUP BY t.name
  ORDER BY t.name
t1 1

> DROP CLUSTER stats_cluster CASCADE
