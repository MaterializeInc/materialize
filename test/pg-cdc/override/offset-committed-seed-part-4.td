# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Part 4 of the offset_committed_seed workflow, run once the locks are
# released: the pending snapshots complete.

$ set-sql-timeout duration=120s

> SELECT count(*) FROM t2
100

> SELECT count(*) FROM t4
100

> SELECT o.name, bool_and(u.snapshot_committed)
  FROM mz_objects o
  JOIN mz_internal.mz_source_statistics u ON o.id = u.id
  WHERE o.name IN ('t2', 't4')
  GROUP BY o.name
t2 true
t4 true
