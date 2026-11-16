# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# PER-152: with in-builder compaction on and a small max run len, a large
# INSERT used to panic environmentd when txn-wal rewrote its timestamps.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET persist_batch_max_runs = 2
ALTER SYSTEM SET persist_batch_max_run_len = 2
ALTER SYSTEM SET persist_blob_target_size = 1024

> CREATE TABLE t (a int, b text)

> INSERT INTO t SELECT x, repeat('x', 100) FROM generate_series(1, 5000) AS x

> SELECT count(*), sum(a) FROM t
5000 12502500

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET persist_batch_max_runs
ALTER SYSTEM RESET persist_batch_max_run_len
ALTER SYSTEM RESET persist_blob_target_size
