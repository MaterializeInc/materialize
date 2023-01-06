# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that specifying an idle arrangement merge effort has an effect.
#
# The idea is to create an arrangement with some records in it, then send retractions for all those
# records and watch what happens. Without further updates arriving at the arrangement, its batches
# should only be merged if a positive idle merge effort is configured.

> CREATE CLUSTER test REPLICAS (
    eager (SIZE '1', IDLE ARRANGEMENT MERGE EFFORT = 100),
    lazy (SIZE '1', IDLE ARRANGEMENT MERGE EFFORT = 0)
  )
> SET cluster = test

# Create the arrangement used in this test.
# Set the minimal compaction window, to ensure the most aggressive compaction.
> CREATE TABLE t (a int)
> CREATE DEFAULT INDEX ON t WITH (LOGICAL COMPACTION WINDOW = '1ms')

# Fill the arrangement with some records.
> INSERT INTO t SELECT generate_series(1, 100)

# Retract all the records from the arrangement.
> DELETE FROM t

# This is subtle: DD's idle_merge_effort is only applied to in-progress merges of batches, but
# doesn't itself cause new merges to start. Thus, we need to send another update to trigger a batch
# merge in the arrangement. We only insert a single record, which is not enough to complete a full
# merge of the relatively larger batches (unless we exert idle merge effort).
> INSERT INTO t VALUES (1)

# The arrangement should now be compacted on the eager replica, but not on the lazy one.

> SET cluster_replica = eager
> SELECT records, batches FROM mz_internal.mz_arrangement_sizes
records  batches
----------------
1        1

> SET cluster_replica = lazy
> SELECT records, batches FROM mz_internal.mz_arrangement_sizes
records  batches
----------------
201      4

# Clean up.
> DROP CLUSTER test CASCADE
