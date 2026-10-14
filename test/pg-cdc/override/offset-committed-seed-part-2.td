# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Part 2 of the offset_committed_seed workflow, run while the workflow holds
# ACCESS EXCLUSIVE locks on the primary key indexes of t2 and t4 and keeps
# inserting into t1 and t3: add a table to each source. The tables' snapshots
# block on the locks.

$ set-sql-timeout duration=60s

> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE t2);
> ALTER SOURCE mz_source_b ADD SUBSOURCE t4;

# t2 and t4 are snapshotting and stay so until the locks are released.
> SELECT o.name, bool_and(NOT u.snapshot_committed)
  FROM mz_objects o
  JOIN mz_internal.mz_source_statistics u ON o.id = u.id
  WHERE o.name IN ('t2', 't4')
  GROUP BY o.name
t2 true
t4 true
