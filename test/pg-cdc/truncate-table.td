# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test TRUNCATE -- source will error out
#

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

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

ANALYZE;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> BEGIN
> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
> COMMIT

> SELECT * FROM t1;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE t1;

# Note: truncating the upstream table will not corrupt the source in Materialize
# for Postgres version 10. For all versions > 10, users will see the following error
# "source table got truncated."
#
# See the Postgres docs for more information:
# https://www.postgresql.org/docs/10/logical-replication-restrictions.html
! SELECT * FROM t1;
contains:table was truncated

#
# Test that a truncated table stays dead across an ingestion dataflow restart.
#
# While the dataflow is running, a TRUNCATE drops the table from the in-memory
# table map, so every later change to it is ignored. The map is rebuilt from the
# catalog when the dataflow restarts, which brings the table back as a live
# output resuming from the current replication position. Rows inserted upstream
# while the table was stalled were never ingested, so their later deletes are
# written as retractions of rows that do not exist.
#
# Those retractions cannot be read back directly: the shard also holds the
# permanent "table was truncated" error, so every peek on it fails on that error
# instead. A downstream MIN/MAX reduce does see them, because the ok stream keeps
# flowing regardless of the error stream, and it pushes an error of its own into
# the error stream. Counting the reduce's errors is therefore the observation
# point.

$ set-sql-timeout duration=120s

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE t_trunc (f1 INTEGER);
ALTER TABLE t_trunc REPLICA IDENTITY FULL;
INSERT INTO t_trunc VALUES (1);

CREATE TABLE t_barrier (f1 INTEGER);
ALTER TABLE t_barrier REPLICA IDENTITY FULL;
INSERT INTO t_barrier VALUES (1);

CREATE PUBLICATION mz_source_restart FOR TABLE t_trunc, t_barrier;

> CREATE CLUSTER restart_cluster SIZE '${arg.default-replica-size}'

> BEGIN
> CREATE SOURCE mz_source_restart
  IN CLUSTER restart_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_restart');

> CREATE TABLE t_trunc FROM SOURCE mz_source_restart (REFERENCE t_trunc);
> CREATE TABLE t_barrier FROM SOURCE mz_source_restart (REFERENCE t_barrier);
> COMMIT

> SELECT * FROM t_trunc;
1

> SELECT * FROM t_barrier;
1

# Hydrates on clean data before the truncate. One replica, because the error
# counts are reported per replica and a second one only adds a lagging copy of
# the same numbers.
> CREATE CLUSTER compute_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 1

> CREATE MATERIALIZED VIEW mv_minmax
  IN CLUSTER compute_cluster
  AS SELECT max(f1) AS m FROM t_trunc

> SELECT * FROM mv_minmax;
1

# Truncate and reload upstream. The reloaded rows are dropped on the floor: the
# running dataflow has already forgotten the table.
$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE t_trunc;
INSERT INTO t_trunc VALUES (100), (200);

! SELECT * FROM t_trunc;
contains:table was truncated

# Wait for the truncate error to propagate into the view, then record how many
# errors it has. How many the reduce plan produces for one definite error is not
# the point, so keep the baseline rather than a literal: what matters is that it
# does not grow later.
> SELECT SUM(c.count) > 0
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  WHERE o.name = 'mv_minmax'
true

$ set-from-sql var=baseline_errors
SELECT SUM(c.count)::text
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  WHERE o.name = 'mv_minmax'

# Restart the ingestion dataflow without touching the truncated table. Dropping
# any other subsource of the same source has the same effect.
> ALTER CLUSTER restart_cluster SET (REPLICATION FACTOR 0)

> ALTER CLUSTER restart_cluster SET (REPLICATION FACTOR 1)

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 't_barrier'
running

# Delete the reloaded rows, then write to `t_barrier`. Replication is ordered,
# so once the `t_barrier` row is visible the DELETE has already been processed.
$ postgres-execute connection=postgres://postgres:postgres@postgres
DELETE FROM t_trunc;

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_barrier VALUES (2);

> SELECT * FROM t_barrier;
1
2

$ set-from-sql var=barrier_ts
SELECT mz_now()::text FROM t_barrier LIMIT 1

# Both exports share the source's remap collection and the DELETE precedes the
# `t_barrier` write in the replication stream, so a view that has produced output
# past this point has already consumed the retractions if there were any. Without
# this wait the count below would match before the view had a chance to change.
> SELECT write_frontier > ${barrier_ts}
  FROM mz_internal.mz_frontiers f
  JOIN mz_objects o ON f.object_id = o.id
  WHERE o.name = 'mv_minmax'
true

# Pre-fix the deletes reach the reduce as retractions of rows it never saw, so it
# emits further errors and this count grows past the baseline.
> SELECT SUM(c.count) = ${baseline_errors}
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  WHERE o.name = 'mv_minmax'
true
