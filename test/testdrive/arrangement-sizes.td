# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the live `mz_object_arrangement_sizes` introspection source and the
# periodically-snapshotted `mz_object_arrangement_size_history` table.
#
# Relies on testdrive's retry behavior since both relations are populated
# asynchronously.

$ set-sql-timeout duration=90s
$ set-arg-default default-replica-size=scale=1,workers=1

# `arrangement_size_collection_interval` is set to 2s at bootstrap by the
# testdrive mzcompose harness so the first snapshot arrives within this
# test's timeout rather than at the 1h production default.

# Schema check.
> SELECT c.name, c.type
  FROM mz_columns c
  JOIN mz_tables t ON t.id = c.id
  JOIN mz_schemas s ON s.id = t.schema_id
  WHERE s.name = 'mz_internal'
    AND t.name = 'mz_object_arrangement_size_history'
  ORDER BY c.position
replica_id           text
object_id            text
size                 bigint
collection_timestamp "timestamp with time zone"

# Set up a cluster with two replicas and some objects.
> CREATE CLUSTER test SIZE '${arg.default-replica-size}', REPLICATION FACTOR 2
> SET cluster = test

> CREATE TABLE t (a int, b int)
> INSERT INTO t SELECT g, g % 10 FROM generate_series(1, 1000) g

> CREATE MATERIALIZED VIEW mv_agg AS
  SELECT b, COUNT(*) AS cnt FROM t GROUP BY b
> CREATE INDEX idx_mv_agg ON mv_agg (b)
> CREATE INDEX idx_t ON t (a)
# Filter-only MV: no arrangement, should not appear in the live table.
> CREATE MATERIALIZED VIEW mv_filter AS SELECT * FROM t WHERE a > 0

# Live table: each arranged object shows up on both replicas with size > 0.
> SELECT o.name, r.name, s.size > 0
  FROM mz_internal.mz_object_arrangement_sizes s
  JOIN mz_objects o ON o.id = s.object_id
  JOIN mz_cluster_replicas r ON r.id = s.replica_id
  WHERE o.name IN ('mv_agg', 'idx_mv_agg', 'idx_t')
  ORDER BY o.name, r.name
idx_mv_agg r1 true
idx_mv_agg r2 true
idx_t      r1 true
idx_t      r2 true
mv_agg     r1 true
mv_agg     r2 true

# Filter-only MV eventually drops out of the live table. During initial
# install the persist-sink pipeline can briefly register transient state
# in the raw heap/batcher tables; testdrive's automatic retry covers this.
> SELECT count(*)
  FROM mz_internal.mz_object_arrangement_sizes s
  JOIN mz_objects o ON o.id = s.object_id
  WHERE o.name = 'mv_filter'
0

# History table: at least one snapshot row per (object_id, replica_id) pair
# for each arranged object, with a recent collection_timestamp.
> SELECT o.name, r.name, count(*) > 0, bool_and(h.size > 0)
  FROM mz_internal.mz_object_arrangement_size_history h
  JOIN mz_objects o ON o.id = h.object_id
  JOIN mz_cluster_replicas r ON r.id = h.replica_id
  WHERE o.name IN ('mv_agg', 'idx_mv_agg', 'idx_t')
  GROUP BY o.name, r.name
  ORDER BY o.name, r.name
idx_mv_agg r1 true true
idx_mv_agg r2 true true
idx_t      r1 true true
idx_t      r2 true true
mv_agg     r1 true true
mv_agg     r2 true true

> SELECT bool_and(h.collection_timestamp IS NOT NULL
    AND h.collection_timestamp > now() - INTERVAL '60 seconds')
  FROM mz_internal.mz_object_arrangement_size_history h
  JOIN mz_objects o ON o.id = h.object_id
  WHERE o.name IN ('mv_agg', 'idx_mv_agg', 'idx_t')
true

# Drop test: live table retracts the dropped MV; existing history rows
# remain but no new rows should be appended for it.
$ set-from-sql var=dropped-id
SELECT id FROM mz_objects WHERE name = 'mv_agg'

> DROP MATERIALIZED VIEW mv_agg CASCADE

> SELECT count(*)
  FROM mz_internal.mz_object_arrangement_sizes
  WHERE object_id = '${dropped-id}'
0

$ set-from-sql var=last-history-ts
SELECT COALESCE(max(collection_timestamp)::text, '1970-01-01')
FROM mz_internal.mz_object_arrangement_size_history
WHERE object_id = '${dropped-id}'

> SELECT count(*)
  FROM mz_internal.mz_object_arrangement_size_history
  WHERE object_id = '${dropped-id}'
    AND collection_timestamp > '${last-history-ts}'::timestamptz
0

> DROP CLUSTER test CASCADE
