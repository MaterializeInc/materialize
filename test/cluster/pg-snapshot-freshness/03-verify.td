# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The storage cluster has restarted without the failpoint, so `t_paused`'s
# snapshot re-runs to completion and every table converges. The row inserted
# while the snapshot was held appears exactly once, whether it was staged
# before the restart or picked up by the re-run snapshot.

> SELECT * FROM t_paused
100
200
300

> SELECT * FROM t_before
1
2

> SELECT * FROM t_alongside
10
20
30

> SELECT s.name, bool_and(u.snapshot_committed)
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 't_paused'
  GROUP BY s.name
t_paused true
