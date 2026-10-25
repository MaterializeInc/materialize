# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Recovering a materialized view from a truncated table with a replacement.
#
# A TRUNCATE seals the ingesting table (SS-472). A materialized view that reads
# only that table seals with it, and a sealed materialized view can never be
# replaced (SQL-595): its output is final for all times. The recovery flow in
# the "Recover from an unplanned change" guide therefore only works when the
# public materialized view keeps a live input. The pattern (SS-506) is to read
# the table through a UNION ALL with an empty table: the empty table's frontier
# advances with wall-clock time, so the view's frontier keeps advancing while it
# carries the truncate error, APPLY REPLACEMENT can align on a timestamp, and the
# apply retracts the error and emits only the diff between the old and the new
# table version.
#
# `feed` is the guarded view and must stay replaceable. `feed_plain` reads the
# table directly and is the control: it seals and cannot be replaced.

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
CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
ANALYZE;
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE t1_v1 FROM SOURCE mz_source (REFERENCE t1);

# The guard has the same columns and nullability as the ingested table and
# stays empty.
> CREATE TABLE guard (id INTEGER NOT NULL, val TEXT);

> CREATE MATERIALIZED VIEW feed AS
  SELECT * FROM t1_v1 UNION ALL SELECT * FROM guard;
> CREATE MATERIALIZED VIEW feed_plain AS
  SELECT * FROM t1_v1;

> SELECT * FROM feed ORDER BY id;
1 a
2 b
3 c
4 d
5 e

> SELECT * FROM feed_plain ORDER BY id;
1 a
2 b
3 c
4 d
5 e

# Nightly reload: three rows unchanged, one changed, one removed, two added.
$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE t1;
INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd-changed'), (6, 'f'), (7, 'g');

! SELECT * FROM t1_v1;
contains:table was truncated

# Both views return the error while the table version is dead.
! SELECT * FROM feed;
contains:table was truncated

! SELECT * FROM feed_plain;
contains:table was truncated

# The table and the unguarded view are sealed. The guarded view is not.
> SELECT name, write_frontier IS NULL
  FROM mz_internal.mz_frontiers
  JOIN mz_objects ON id = object_id
  WHERE name IN ('t1_v1', 'feed', 'feed_plain')
feed false
feed_plain true
t1_v1 true

# The next table version snapshots the reloaded table off the same slot.
> CREATE TABLE t1_v2 FROM SOURCE mz_source (REFERENCE t1);

> SELECT * FROM t1_v2 ORDER BY id;
1 a
2 b
3 c
4 d-changed
6 f
7 g

# The replacement keeps the guard.
> CREATE REPLACEMENT MATERIALIZED VIEW feed_r FOR feed AS
  SELECT * FROM t1_v2 UNION ALL SELECT * FROM guard;

> SELECT hydrated
  FROM mz_internal.mz_hydration_statuses
  JOIN mz_materialized_views ON id = object_id
  WHERE name = 'feed_r'
true

> ALTER MATERIALIZED VIEW feed APPLY REPLACEMENT feed_r;

# The error is retracted and the view shows the reloaded table.
> SELECT * FROM feed ORDER BY id;
1 a
2 b
3 c
4 d-changed
6 f
7 g

> SELECT write_frontier IS NULL
  FROM mz_internal.mz_frontiers
  JOIN mz_materialized_views ON id = object_id
  WHERE name = 'feed'
false

# The unguarded view sealed with its input and cannot be replaced.
> CREATE REPLACEMENT MATERIALIZED VIEW feed_plain_r FOR feed_plain AS
  SELECT * FROM t1_v2;

! ALTER MATERIALIZED VIEW feed_plain APPLY REPLACEMENT feed_plain_r;
contains:is sealed and thus cannot be replaced

! SELECT * FROM feed_plain;
contains:table was truncated

# The dead version can be dropped once nothing live reads it.
> DROP MATERIALIZED VIEW feed_plain_r;
> DROP MATERIALIZED VIEW feed_plain;
> DROP TABLE t1_v1;

> SELECT * FROM feed ORDER BY id;
1 a
2 b
3 c
4 d-changed
6 f
7 g

> DROP SOURCE mz_source CASCADE;
