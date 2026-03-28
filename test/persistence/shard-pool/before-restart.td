# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that pre-opened shards from the shard pool survive a restart.
# Before restart: enable the pool, create tables (which use pre-opened shards),
# and verify data is correct.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_shard_pool_enabled = true;
ALTER SYSTEM SET storage_shard_pool_target_size = 5;

# Wait briefly for pool to replenish.
$ sleep-is-probably-flaky-i-am-okay-with-that duration=2s

> CREATE TABLE shard_pool_t1 (a INT)
> INSERT INTO shard_pool_t1 VALUES (1), (2), (3)

> CREATE TABLE shard_pool_t2 (b TEXT)
> INSERT INTO shard_pool_t2 VALUES ('hello'), ('world')

> SELECT * FROM shard_pool_t1
1
2
3

> SELECT * FROM shard_pool_t2
hello
world

# Verify unique shard IDs were allocated.
> SELECT count(DISTINCT shard_id) FROM mz_internal.mz_storage_shards WHERE object_id IN (SELECT id FROM mz_tables WHERE name LIKE 'shard_pool_%')
2
