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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;

CREATE TABLE t1 (f1 TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES ('one');
INSERT INTO t1 VALUES ('two');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE CLUSTER stats_cluster SIZE '${arg.default-replica-size}'

> CREATE SOURCE mz_source
  IN CLUSTER stats_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES ("t1")

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
    mz_internal.mz_source_statistics_raw u,
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
    mz_internal.mz_source_statistics_raw u,
    mz_sources s
  WHERE
    c.name = 'stats_cluster' AND c.id = cr.cluster_id AND cr.id = u.replica_id
    AND s.name IN ('mz_source') AND u.id = s.id
  ORDER BY cr.id
  LIMIT 1

> SELECT
    s.name,
    u.offset_committed > 0,
    u.offset_known >= u.offset_committed,
    u.snapshot_records_known,
    u.snapshot_records_staged
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
mz_source true true 2 2

$ set-from-sql var=previous-offset-committed
SELECT
    (u.offset_committed)::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'


$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES ('three');


> SELECT
    s.name,
    u.offset_committed > ${previous-offset-committed},
    u.offset_known >= u.offset_committed,
    u.snapshot_records_known,
    u.snapshot_records_staged
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
mz_source true true 2 2

$ set-from-sql var=pre-restart-offset-committed
SELECT
    (u.offset_committed)::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 0)

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES ('four');

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 1)

# Ensure the snapshot stats stay there for the old replica, and don't change.
> SELECT
    s.name,
    u.offset_committed >= ${pre-restart-offset-committed},
    u.offset_known >= u.offset_committed,
    u.snapshot_records_known,
    u.snapshot_records_staged
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
mz_source true true 2 2

$ set-regex match=u\d+ replacement="<REPLICAID>"
> SELECT cr.id
  FROM
    mz_clusters c,
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics_raw u,
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
    mz_internal.mz_source_statistics_raw u,
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
    u.offset_committed > ${pre-restart-offset-committed},
    u.offset_known >= u.offset_committed,
    u.snapshot_records_known,
    u.snapshot_records_staged
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
mz_source true true <null> <null>


$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alt (f1 TEXT);
ALTER TABLE alt REPLICA IDENTITY FULL;
INSERT INTO alt VALUES ('one');

> ALTER SOURCE mz_source ADD SUBSOURCE alt;

> SELECT COUNT(*) > 0 FROM alt;
true

# When we add a table we snapshot that table, so now we will have snapshot stats.
>[version>=14900] SELECT
    s.name,
    u.snapshot_records_known,
    u.snapshot_records_staged
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
mz_source 1 1

>[version<14900] SELECT
    s.name,
    u.snapshot_records_known,
    u.snapshot_records_staged
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
mz_source 1 1

# Ensure subsource stats show up, and then are removed when we drop subsources.
> SELECT
    s.name,
    u.updates_committed > 0
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('alt') AND u.replica_id = '${replica_id}'
alt true

> DROP SOURCE alt;

> SELECT
    count(*)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('alt')
0

# TODO(guswynn): consider adding an envd restart test for pg statistics, not just kafka ones like in test/cluster.
