# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Part 3 of the offset_committed_seed workflow, run after Materialize restarted
# with the snapshots of t2 and t4 still pending. An ingestion makes no progress
# until its snapshot completes, so t1 and t3 cannot be read here, but the
# statistics can.

$ set-sql-timeout duration=120s

> SELECT o.name, bool_and(NOT u.snapshot_committed)
  FROM mz_objects o
  JOIN mz_internal.mz_source_statistics u ON o.id = u.id
  WHERE o.name IN ('t2', 't4')
  GROUP BY o.name
t2 true
t4 true
