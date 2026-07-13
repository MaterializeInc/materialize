# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Coalesced ("shared") batch writing in the MV sink must produce results that
# are identical to the per-worker path. Exercise it across multiple workers, with
# churn (retractions and inserts), and assert that the materialized view always
# equals the equivalent one-shot aggregation. `mv_v1` renders the async sink and
# `mv_v2` the sync sink, since the sink implementation is picked when the sink
# renders. The barrier applies to the sync sink only, and coalesces per process,
# so this covers both a single-process replica and a multi-process one, where the
# per-process batches are combined by the append operator.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET compute_mv_sink_shared_batch_max_bytes = 1048576
# The barrier makes the workers coalesce into exactly one batch per interval, which exercises the
# barrier and skip-reporting paths.
ALTER SYSTEM SET enable_compute_sync_mv_sink_shared_batches_barrier = true

# Multiple workers in one process, so the sink coalesces their parts.
> CREATE CLUSTER shared_batches SIZE 'scale=1,workers=4'

> SET cluster = shared_batches

> CREATE TABLE t (k bigint, v bigint)

> INSERT INTO t SELECT x, x * 10 FROM generate_series(1, 20000) x

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_compute_sync_mv_sink = false

> CREATE MATERIALIZED VIEW mv_v1 AS
  SELECT k % 256 AS bucket, count(*) AS cnt, sum(v) AS s
  FROM t
  GROUP BY k % 256

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_compute_sync_mv_sink = true

> CREATE MATERIALIZED VIEW mv_v2 AS
  SELECT k % 256 AS bucket, count(*) AS cnt, sum(v) AS s
  FROM t
  GROUP BY k % 256

> SELECT count(*) FROM mv_v1
256

> SELECT count(*) FROM mv_v2
256

# The view must equal the one-shot aggregation. Both directions of the
# difference must be empty.
> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv_v1)

> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv_v2)

> (SELECT bucket, cnt, s FROM mv_v1)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> (SELECT bucket, cnt, s FROM mv_v2)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

# Churn: retract half the rows and insert new keys. This spreads updates across
# all four workers within a single batch interval, which is what the shared
# batch coalesces.
> DELETE FROM t WHERE k % 2 = 0

> INSERT INTO t SELECT x, x * 10 FROM generate_series(20001, 35000) x

> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv_v1)

> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv_v2)

> (SELECT bucket, cnt, s FROM mv_v1)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> (SELECT bucket, cnt, s FROM mv_v2)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

# Odd k in 1..20000 (10000 rows) plus 20001..35000 (15000 rows).
> SELECT sum(cnt) FROM mv_v1
25000

> SELECT sum(cnt) FROM mv_v2
25000

# With a tiny threshold, workers whose updates for an interval exceed it write
# their own batch while the rest still contribute to the shared one, so one
# interval yields both kinds of batch.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET compute_mv_sink_shared_batch_max_bytes = 1024

> UPDATE t SET v = v + 1 WHERE k % 3 = 0

> INSERT INTO t VALUES (1, 1), (2, 2)

> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv_v1)

> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv_v2)

> (SELECT bucket, cnt, s FROM mv_v1)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> (SELECT bucket, cnt, s FROM mv_v2)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> SELECT sum(cnt) FROM mv_v2
25002

# With sharing disabled, every worker writes its own batch. The barrier mode of
# `mv_v2` was fixed when it rendered, so it keeps sharing.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET compute_mv_sink_shared_batch_max_bytes = 0

> DELETE FROM t WHERE k IN (1, 2)

> INSERT INTO t VALUES (1, 10), (2, 20)

> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv_v1)

> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv_v2)

> (SELECT bucket, cnt, s FROM mv_v1)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> (SELECT bucket, cnt, s FROM mv_v2)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> SELECT sum(cnt) FROM mv_v2
25001

$ postgres-execute connection=mz_system
ALTER SYSTEM SET compute_mv_sink_shared_batch_max_bytes = 1048576

# A write with no net change must not corrupt the view, including on workers
# with no data for the interval.
> DELETE FROM t WHERE false

> (SELECT bucket, cnt, s FROM mv_v1)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> (SELECT bucket, cnt, s FROM mv_v2)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> DROP MATERIALIZED VIEW mv_v1
> DROP MATERIALIZED VIEW mv_v2
> DROP TABLE t
> SET cluster = quickstart
> DROP CLUSTER shared_batches

# Multi-process replica (scale > 1): each process barriers its own workers into one
# batch, and the append operator combines the per-process batches into a single
# append. The result must still equal the one-shot aggregation.
> CREATE CLUSTER shared_batches_mp SIZE 'scale=2,workers=2'

> SET cluster = shared_batches_mp

> CREATE TABLE t (k bigint, v bigint)

> INSERT INTO t SELECT x, x * 10 FROM generate_series(1, 20000) x

> CREATE MATERIALIZED VIEW mv AS
  SELECT k % 256 AS bucket, count(*) AS cnt, sum(v) AS s
  FROM t
  GROUP BY k % 256

> DELETE FROM t WHERE k % 2 = 0

> INSERT INTO t SELECT x, x * 10 FROM generate_series(20001, 35000) x

> (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)
  EXCEPT ALL
  (SELECT bucket, cnt, s FROM mv)

> (SELECT bucket, cnt, s FROM mv)
  EXCEPT ALL
  (SELECT k % 256, count(*), sum(v) FROM t GROUP BY k % 256)

> SELECT sum(cnt) FROM mv
25000

> DROP MATERIALIZED VIEW mv
> DROP TABLE t
> SET cluster = quickstart
> DROP CLUSTER shared_batches_mp

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET compute_mv_sink_shared_batch_max_bytes
ALTER SYSTEM RESET enable_compute_sync_mv_sink
ALTER SYSTEM RESET enable_compute_sync_mv_sink_shared_batches_barrier
