# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test replicates a bug:
#
# If we don't set the "offset_committed" statistic until we start processing new replication events,
# it's possible we'll leave "offset_committed" at the default value of zero for too long.
# This confuses the ingestion lag calculation in the UI, causing it to yield erroneously high values.
#
# Instead, we should set "offset_committed" to the dataflow's "resume_lsn" as soon as the responsible worker starts up.
# This is a more accurate representation of how much catching up the replica has to do.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

# Load up a table with a lot of data.
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE t0 (f1 INTEGER, f2 TEXT);
ALTER TABLE t0 REPLICA IDENTITY FULL;

INSERT INTO t0 SELECT i, repeat(md5(i::text), 1000) FROM generate_series(1, 10000) AS i;

# Configure Materialize

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
$ postgres-execute connection=mz_system
ALTER SYSTEM SET storage_statistics_collection_interval = 100
ALTER SYSTEM SET storage_statistics_interval = 200

> BEGIN
> CREATE SOURCE mz_source
  FROM POSTGRES
  CONNECTION pg
  (PUBLICATION 'mz_source');

> CREATE TABLE t0 FROM SOURCE mz_source (REFERENCE t0);
> COMMIT

# Wait for statistics to be populated, then find the active replica for the source.
> SELECT COUNT(*)
  FROM
    mz_clusters c,
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics u,
    mz_sources s
  WHERE
    c.id = cr.cluster_id AND cr.id = u.replica_id
    AND s.name IN ('mz_source') AND u.id = s.id
1

$ set-from-sql var=replica_id
SELECT cr.id
  FROM
    mz_clusters c,
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics u,
    mz_sources s
  WHERE
    c.id = cr.cluster_id AND cr.id = u.replica_id
    AND s.name IN ('mz_source') AND u.id = s.id
  ORDER BY cr.id
  LIMIT 1

# Expect to see nonzero offset_committed before we finish processing the snapshot (i.e. while snapshot_committed=false).
# We need retries because offset_committed is initialized to 0.
# But limit retries so we don't wait forever--once snapshot_committed=true, it will never become false again.
$ set-max-tries max-tries=100
> SELECT
    s.name,
    u.offset_committed > 0,
    u.snapshot_committed
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('mz_source') AND u.replica_id = '${replica_id}'
mz_source true false
