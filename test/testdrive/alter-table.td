# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test for ALTER TABLE ADD COLUMN functionality.

# Note: There is also a comprehensive test in test/sqllogictest/alter-table.slt
# that covers various ALTER TABLE scenarios.
#
# This test specifically validates that shard mappings are correctly maintained
# when using ALTER TABLE ADD COLUMN followed by DROP TABLE, which previously
# could lead to negative multiplicities in builtin collections. See commit
# 2b8deec9 for details on the issue this regression test covers.

$ set-arg-default single-replica-cluster=quickstart

# First, enable the alter table feature
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_alter_table_add_column = true

# Create a simple table
> CREATE TABLE t (initial_col int)

# Insert some data
> INSERT INTO t VALUES (1), (2), (3)

# Verify initial state
> SELECT * FROM t ORDER BY initial_col
1
2
3

# Check initial shard mappings for the table. We should see one mapping.
> SELECT COUNT(*) AS shard_count FROM mz_internal.mz_storage_shards WHERE object_id = (SELECT id FROM mz_tables WHERE name = 't')
shard_count
-----------
1

$ set-from-sql var=old-shard-count
SELECT COUNT(*)::text FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%'

# Now alter the table to add a column
> ALTER TABLE t ADD COLUMN added_col text

# Verify the table structure after alteration
> SELECT * FROM t ORDER BY initial_col
initial_col added_col
-----------------------
1 <null>
2 <null>
3 <null>

# Insert data with the new column
> INSERT INTO t VALUES (4, 'test')

> SELECT * FROM t ORDER BY initial_col
initial_col added_col
-----------------------
1 <null>
2 <null>
3 <null>
4 test

# Check shard mappings after ALTER TABLE. We should now see a mapping for the
# old GlobalId of the table and one for the _new_ GlobalId of the table.
> SELECT COUNT(*) = ${old-shard-count} + 1 FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%'
true

# Now drop the table - this previously would cause negative multiplicities
# because ALTER TABLE didn't write down shard mappings but DROP TABLE would try
# to remove them.
> DROP TABLE t

# Verify that shard mappings are properly cleaned up. The shards should be back
# to where they started out.
> SELECT COUNT(*) = ${old-shard-count} - 1 FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%'
true

# Reset the feature flag
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_alter_table_add_column = false
