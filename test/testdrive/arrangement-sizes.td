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

# Shorten the snapshot cadence so the first snapshot arrives within
# this test's timeout rather than at the 1h production default.
$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET arrangement_size_history_collection_interval = '2s';

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
hydration_complete   boolean

# Set up a cluster with two replicas and arrangements large enough to
# clear the 10 MiB floor that `mz_object_arrangement_sizes` applies.
> CREATE CLUSTER test SIZE '${arg.default-replica-size}', REPLICATION FACTOR 2
> SET cluster = test

> CREATE TABLE t (a int, b text)
> INSERT INTO t SELECT g, repeat('x', 1024) FROM generate_series(1, 30000) g

> CREATE INDEX idx_t ON t (a)
> CREATE INDEX idx_t2 ON t ((a + 1))

# Filter-only materialized view: writes to persist but maintains no
# in-memory arrangement, so it must never appear in
# `mz_object_arrangement_sizes`.
> CREATE MATERIALIZED VIEW mv_filter AS SELECT * FROM t WHERE a > 0

# Each indexed object shows up on both replicas with size > 0.
> SELECT o.name, r.name, s.size > 0
  FROM mz_internal.mz_object_arrangement_sizes s
  JOIN mz_objects o ON o.id = s.object_id
  JOIN mz_cluster_replicas r ON r.id = s.replica_id
  WHERE o.name IN ('idx_t', 'idx_t2')
  ORDER BY o.name, r.name
idx_t  r1 true
idx_t  r2 true
idx_t2 r1 true
idx_t2 r2 true

# Filter-only MV never appears.
> SELECT count(*)
  FROM mz_internal.mz_object_arrangement_sizes s
  JOIN mz_objects o ON o.id = s.object_id
  WHERE o.name = 'mv_filter'
0

# History table: at least one snapshot row per (object_id, replica_id)
# pair, all with size > 0, and at least one row tagged
# `hydration_complete = true` for each pair once arrangements settle.
> SELECT o.name, r.name, count(*) > 0, bool_and(h.size > 0), bool_or(h.hydration_complete)
  FROM mz_internal.mz_object_arrangement_size_history h
  JOIN mz_objects o ON o.id = h.object_id
  JOIN mz_cluster_replicas r ON r.id = h.replica_id
  WHERE o.name IN ('idx_t', 'idx_t2')
  GROUP BY o.name, r.name
  ORDER BY o.name, r.name
idx_t  r1 true true true
idx_t  r2 true true true
idx_t2 r1 true true true
idx_t2 r2 true true true

> SELECT bool_and(h.collection_timestamp IS NOT NULL
    AND h.collection_timestamp > now() - INTERVAL '60 seconds')
  FROM mz_internal.mz_object_arrangement_size_history h
  JOIN mz_objects o ON o.id = h.object_id
  WHERE o.name IN ('idx_t', 'idx_t2')
true

> DROP CLUSTER test CASCADE
