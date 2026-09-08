# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The `pg_snapshot_pause=return(t_paused:<millis>)` failpoint holds `t_paused`'s
# snapshot for a fixed time, so it completes in this dataflow incarnation and
# what commits is the snapshot plus the CDC the persist sink staged behind the
# held frontier. A restart would discard that staging and re-run the snapshot,
# which is what the verify phase of the sibling workflow covers.

# More CDC for the held table, spread over several timestamps. Each round waits
# for the hydrated table's row to commit, which takes at least one timestamp
# interval, so the held table's rows land at distinct times rather than in one.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_paused VALUES (301);
INSERT INTO t_before VALUES (3);

> SELECT * FROM t_before
1
2
3

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_paused VALUES (302);
INSERT INTO t_before VALUES (4);

> SELECT * FROM t_before
1
2
3
4

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_paused VALUES (303);
INSERT INTO t_before VALUES (5);

> SELECT * FROM t_before
1
2
3
4
5

# Still held.
> SELECT s.name, bool_and(u.snapshot_committed)
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 't_paused'
  GROUP BY s.name
t_paused false

# The hold lifts. The snapshot rows and every staged CDC row appear exactly once.
> SELECT * FROM t_paused
100
200
300
301
302
303

> SELECT s.name, bool_and(u.snapshot_committed)
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 't_paused'
  GROUP BY s.name
t_paused true

# And CDC after the release lands as for any hydrated table.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_paused VALUES (400);

> SELECT * FROM t_paused
100
200
300
301
302
303
400

> SELECT * FROM t_alongside
10
20
30
