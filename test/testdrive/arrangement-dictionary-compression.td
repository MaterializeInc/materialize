# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for arrangement dictionary compression (materialize#32095)
# on the paged arrange path: a batch of > 64Ki rows installs its codec
# mid-container (`STATS_THRESHOLD` in mz-row-spine), storing its first 64Ki
# rows raw. The codec's first-byte tag bitmap must still cover those raw rows:
# when two compressed batches merge, the merged codec reuses any tag absent
# from the union of the bitmaps as a dictionary tag for a popular value. If
# the bitmap under-reports, a dictionary tag collides with a raw-stored
# datum's first byte and reads silently decode it as the popular value.
#
# Setup: single worker; 70k-row batches; 1000 "rare" rows whose small keys
# sort first, pinning their tiny-string value '1' (tag StringTiny) into the
# raw region, while all other values are 200 distinct 259-char strings (tag
# StringShort) that become dictionary entries. On corruption, a rare row's v
# decodes as a 259-char filler string. In debug-assertions builds the merge
# panics the replica instead and rehydration heals the data, hence the final
# assertion that no replica went offline.

$ set-sql-timeout duration=120s

# Must precede CREATE CLUSTER: the gating flag is captured once at replica
# creation. The flag only enables honoring the per-cluster configuration, so
# the cluster below must also request compression explicitly.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_arrangement_dictionary_compression_alpha = true
ALTER SYSTEM SET enable_column_paged_batcher = true

> CREATE CLUSTER dict_compression SIZE 'scale=1,workers=1', EXPERIMENTAL ARRANGEMENT COMPRESSION = true

> SET cluster = dict_compression

> CREATE TABLE t (k bigint, v text)

> INSERT INTO t SELECT g::bigint, '1' FROM generate_series(1, 1000) g

> INSERT INTO t
  SELECT (4300000000 + g)::bigint, repeat('x', 256) || lpad((g % 200)::text, 3, '0')
  FROM generate_series(1, 69000) g

> CREATE INDEX t_dict_idx ON t (k)

> SELECT count(*) FROM t
70000

# Each single-statement insert lands as one spine batch; introducing a
# same-scale batch forces the pending lower merge to complete, so the snapshot
# batch physically merges with batch 2.
> INSERT INTO t
  SELECT (4400000000 + g)::bigint, repeat('x', 256) || lpad((g % 200)::text, 3, '0')
  FROM generate_series(1, 70000) g

> INSERT INTO t
  SELECT (4500000000 + g)::bigint, repeat('x', 256) || lpad((g % 200)::text, 3, '0')
  FROM generate_series(1, 70000) g

> INSERT INTO t
  SELECT (4600000000 + g)::bigint, repeat('x', 256) || lpad((g % 200)::text, 3, '0')
  FROM generate_series(1, 70000) g

# 4 batches went in; reaching <= 3 requires a merge, and the snapshot batch's
# merge is necessarily the first to complete. The records guard keeps this
# from passing before all batches arrived.
> SELECT s.batches <= 3
  FROM mz_introspection.mz_arrangement_sizes s
  JOIN mz_introspection.mz_dataflow_operator_dataflows d ON s.operator_id = d.id
  WHERE d.dataflow_name LIKE '%t_dict_idx%' AND s.records >= 280000
true

# The raw-stored rare rows must survive the merge.
> SELECT v FROM t WHERE k = 17
1

> SELECT count(*), count(DISTINCT v), min(length(v)), max(length(v)) FROM t
280000 201 1 259

# Catch the debug-assertions failure mode (merge panic + rehydration): the
# replica goes offline after it has already come online, then rehydrates.
# We must only count offline events that follow the replica's first online
# event. Under the Kubernetes orchestrator a freshly created replica records a
# benign offline entry while its pod is still provisioning, before it first
# reports ready, so counting all offline events would spuriously trip here.
> SELECT count(*)
  FROM mz_internal.mz_cluster_replica_status_history h
  JOIN mz_catalog.mz_cluster_replicas r ON h.replica_id = r.id
  JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id
  WHERE c.name = 'dict_compression' AND h.status = 'offline'
    AND h.occurred_at > (
      SELECT min(o.occurred_at)
      FROM mz_internal.mz_cluster_replica_status_history o
      WHERE o.replica_id = h.replica_id AND o.status = 'online'
    )
0

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_arrangement_dictionary_compression_alpha
ALTER SYSTEM RESET enable_column_paged_batcher
