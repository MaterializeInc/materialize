# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The `mysql_snapshot_pause=return(t_paused:<millis>)` failpoint holds
# `t_paused`'s snapshot for a fixed time. The snapshot then completes in this
# dataflow incarnation. What commits is the snapshot plus the CDC that the
# persist sink staged behind the held frontier. A restart would discard that
# staging and re-run the snapshot. The verify phase of the sibling workflow
# covers that case.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
USE public;

# More CDC for the held table, spread over several timestamps. Each round waits
# for the hydrated table's row to commit. That takes at least one timestamp
# interval. The held table's rows therefore land at distinct times.
$ mysql-execute name=mysql
INSERT INTO t_paused VALUES (301);
INSERT INTO t_before VALUES (3);

> SELECT * FROM t_before
1
2
3

$ mysql-execute name=mysql
INSERT INTO t_paused VALUES (302);
INSERT INTO t_before VALUES (4);

> SELECT * FROM t_before
1
2
3
4

$ mysql-execute name=mysql
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

# The snapshot point precedes the insert of 300. The read stages only 100 and
# 200. Rows 300 to 303 arrive through the staged CDC. A snapshot re-run after a
# dataflow restart would read all six rows.
> SELECT s.name, bool_and(u.snapshot_committed), sum(u.snapshot_records_staged)
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 't_paused'
  GROUP BY s.name
t_paused true 2

# And CDC after the release lands as for any hydrated table.
$ mysql-execute name=mysql
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
