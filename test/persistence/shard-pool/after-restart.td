# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# After restart: verify tables created with pre-opened shards still have
# their data, and that new tables can be created using the pool.
# Crash recovery should have moved any unclaimed pre-allocated shards to
# unfinalized_shards for GC.

> SELECT * FROM shard_pool_t1
1
2
3

> SELECT * FROM shard_pool_t2
hello
world

# The shard pool should still be enabled (dyncfg persists across restarts).
# Create another table to verify the pool works after restart.
> CREATE TABLE shard_pool_t3 (c INT)
> INSERT INTO shard_pool_t3 VALUES (42)

> SELECT * FROM shard_pool_t3
42

# All tables should have unique shard IDs.
> SELECT count(DISTINCT shard_id) FROM mz_internal.mz_storage_shards WHERE object_id IN (SELECT id FROM mz_tables WHERE name LIKE 'shard_pool_%')
3

# Cleanup.
> DROP TABLE shard_pool_t3
> DROP TABLE shard_pool_t2
> DROP TABLE shard_pool_t1
