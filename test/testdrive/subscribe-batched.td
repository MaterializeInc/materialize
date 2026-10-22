# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Batched subscribes report what plain subscribes report.
#
# Rows between two progress rows arrive in no particular order. Testdrive sorts the rows of each
# FETCH, so every FETCH below covers exactly one progress interval and compares it as a multiset.
# The plain reference over an index omits progress rows, because it reports a progress row for
# every batch of the index it replays.
#
# NOTE: A batched subscribe with `UP TO` reports a progress row at `UP TO` before it completes. A
# plain subscribe completes without one.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_subscribe_batched = true;
ALTER SYSTEM SET enable_index_options = true;
ALTER SYSTEM SET enable_logical_compaction_window = true;

# The catch-ups below wait for up to 16s of wall-clock time to pass.
$ set-sql-timeout duration=60s

> CREATE CLUSTER batched4 SIZE 'scale=1,workers=4'

# Constant inputs with temporal filters change at fixed times, starting at 1.
#
# `lag3` holds each row for three timestamps. The insertion of one row and the retraction of an
# earlier one project to the same row and cancel, except at the ends.
> CREATE VIEW lag3 AS
  SELECT x % 3 AS k FROM generate_series(1, 30) AS x
  WHERE mz_now() >= x AND mz_now() < x + 3

# `sums` inserts several rows that project to the same row at the same time.
> CREATE VIEW sums AS
  SELECT x % 2 AS k FROM generate_series(1, 20) AS x
  WHERE mz_now() >= x / 5

# `err` fails at time 5.
> CREATE VIEW err AS
  SELECT 10 / (x - 5) AS q FROM generate_series(1, 10) AS x
  WHERE mz_now() >= x

# `ev` holds 50 rows, each present for 10ms at a distinct time in the 2s after `base`. A subscribe
# that starts at `base` once those 2s have passed catches up over 100 timestamps. `base` is a
# multiple of 10000, so the regex below leaves the offset from `base` in every timestamp.
> CREATE TABLE ev (a int, ins mz_timestamp, del mz_timestamp) WITH (RETAIN HISTORY = FOR '1h')
> CREATE DEFAULT INDEX ev_idx ON ev WITH (RETAIN HISTORY = FOR '1h')
> CREATE INDEX ev_idx4 IN CLUSTER batched4 ON ev (a) WITH (RETAIN HISTORY = FOR '1h')
> CREATE MATERIALIZED VIEW ev_mv WITH (RETAIN HISTORY = FOR '1h') AS SELECT * FROM ev

$ set-from-sql var=base
SELECT ((trunc((mz_now()::text::numeric + 3000) / 10000) + 1) * 10000)::int8::text

$ set-from-sql var=up-to
SELECT (${base} + 2000)::text

$ set-from-sql var=prefix
SELECT (${base} / 10000)::text

> INSERT INTO ev SELECT x, ${base} + 20 * x, ${base} + 20 * x + 10 FROM generate_series(1, 50) AS x

# `r` changes by writes after `r-as-of`.
> CREATE TABLE r (a int, b text) WITH (RETAIN HISTORY = FOR '1h')
> CREATE DEFAULT INDEX r_idx ON r WITH (RETAIN HISTORY = FOR '1h')
> CREATE INDEX r_idx4 IN CLUSTER batched4 ON r (a) WITH (RETAIN HISTORY = FOR '1h')
> INSERT INTO r VALUES (1, 'a'), (2, 'b'), (3, 'c')

$ set-from-sql var=r-as-of
SELECT mz_now()::text FROM r LIMIT 1

> DELETE FROM r WHERE a = 2
> UPDATE r SET b = 'z' WHERE a = 3
> INSERT INTO r VALUES (4, 'd')

$ set-from-sql var=r-up-to
SELECT (mz_now()::text::int8 + 1)::text FROM r LIMIT 1

# `e` fails once it holds a 0.
> CREATE TABLE e (x int)
> INSERT INTO e VALUES (1)

# Wait until both catch-ups lie in the past, on both clusters.
> SELECT mz_now() > ${up-to} + 1000 AND mz_now() > ${r-up-to} FROM ev, r LIMIT 1
true

> SET cluster = batched4

> SELECT mz_now() > ${up-to} + 1000 AND mz_now() > ${r-up-to} FROM ev, r LIMIT 1
true

> SET cluster = quickstart

# Plain subscribes, the reference for the batched cases below.

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (PROGRESS) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 6 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0
31 false -1 1
32 false -1 2
33 false -1 0

> FETCH ALL c WITH (timeout = '60s')

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE sums WITH (PROGRESS) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 9 c WITH (timeout = '60s')
0 false 2 0
0 false 2 1
1 false 2 0
1 false 3 1
2 false 3 0
2 false 2 1
3 false 2 0
3 false 3 1
4 false 1 0

> FETCH ALL c WITH (timeout = '60s')

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (PROGRESS) AS OF 0 UP TO 20

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 3 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0

> FETCH ALL c WITH (timeout = '60s')

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (PROGRESS) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

$ set-regex match=${prefix} replacement=T-

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev WHERE mz_now() >= ins AND mz_now() < del) WITH (PROGRESS = false) AS OF ${base} UP TO ${up-to}

> FETCH 100 c WITH (timeout = '60s')
T-0020 1 1
T-0030 -1 1
T-0040 1 2
T-0050 -1 2
T-0060 1 3
T-0070 -1 3
T-0080 1 4
T-0090 -1 4
T-0100 1 5
T-0110 -1 5
T-0120 1 6
T-0130 -1 6
T-0140 1 7
T-0150 -1 7
T-0160 1 8
T-0170 -1 8
T-0180 1 9
T-0190 -1 9
T-0200 1 10
T-0210 -1 10
T-0220 1 11
T-0230 -1 11
T-0240 1 12
T-0250 -1 12
T-0260 1 13
T-0270 -1 13
T-0280 1 14
T-0290 -1 14
T-0300 1 15
T-0310 -1 15
T-0320 1 16
T-0330 -1 16
T-0340 1 17
T-0350 -1 17
T-0360 1 18
T-0370 -1 18
T-0380 1 19
T-0390 -1 19
T-0400 1 20
T-0410 -1 20
T-0420 1 21
T-0430 -1 21
T-0440 1 22
T-0450 -1 22
T-0460 1 23
T-0470 -1 23
T-0480 1 24
T-0490 -1 24
T-0500 1 25
T-0510 -1 25
T-0520 1 26
T-0530 -1 26
T-0540 1 27
T-0550 -1 27
T-0560 1 28
T-0570 -1 28
T-0580 1 29
T-0590 -1 29
T-0600 1 30
T-0610 -1 30
T-0620 1 31
T-0630 -1 31
T-0640 1 32
T-0650 -1 32
T-0660 1 33
T-0670 -1 33
T-0680 1 34
T-0690 -1 34
T-0700 1 35
T-0710 -1 35
T-0720 1 36
T-0730 -1 36
T-0740 1 37
T-0750 -1 37
T-0760 1 38
T-0770 -1 38
T-0780 1 39
T-0790 -1 39
T-0800 1 40
T-0810 -1 40
T-0820 1 41
T-0830 -1 41
T-0840 1 42
T-0850 -1 42
T-0860 1 43
T-0870 -1 43
T-0880 1 44
T-0890 -1 44
T-0900 1 45
T-0910 -1 45
T-0920 1 46
T-0930 -1 46
T-0940 1 47
T-0950 -1 47
T-0960 1 48
T-0970 -1 48
T-0980 1 49
T-0990 -1 49
T-1000 1 50
T-1010 -1 50

> FETCH ALL c WITH (timeout = '60s')

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev_mv WHERE mz_now() >= ins AND mz_now() < del) WITH (PROGRESS = false) AS OF ${base} UP TO ${up-to}

> FETCH 100 c WITH (timeout = '60s')
T-0020 1 1
T-0030 -1 1
T-0040 1 2
T-0050 -1 2
T-0060 1 3
T-0070 -1 3
T-0080 1 4
T-0090 -1 4
T-0100 1 5
T-0110 -1 5
T-0120 1 6
T-0130 -1 6
T-0140 1 7
T-0150 -1 7
T-0160 1 8
T-0170 -1 8
T-0180 1 9
T-0190 -1 9
T-0200 1 10
T-0210 -1 10
T-0220 1 11
T-0230 -1 11
T-0240 1 12
T-0250 -1 12
T-0260 1 13
T-0270 -1 13
T-0280 1 14
T-0290 -1 14
T-0300 1 15
T-0310 -1 15
T-0320 1 16
T-0330 -1 16
T-0340 1 17
T-0350 -1 17
T-0360 1 18
T-0370 -1 18
T-0380 1 19
T-0390 -1 19
T-0400 1 20
T-0410 -1 20
T-0420 1 21
T-0430 -1 21
T-0440 1 22
T-0450 -1 22
T-0460 1 23
T-0470 -1 23
T-0480 1 24
T-0490 -1 24
T-0500 1 25
T-0510 -1 25
T-0520 1 26
T-0530 -1 26
T-0540 1 27
T-0550 -1 27
T-0560 1 28
T-0570 -1 28
T-0580 1 29
T-0590 -1 29
T-0600 1 30
T-0610 -1 30
T-0620 1 31
T-0630 -1 31
T-0640 1 32
T-0650 -1 32
T-0660 1 33
T-0670 -1 33
T-0680 1 34
T-0690 -1 34
T-0700 1 35
T-0710 -1 35
T-0720 1 36
T-0730 -1 36
T-0740 1 37
T-0750 -1 37
T-0760 1 38
T-0770 -1 38
T-0780 1 39
T-0790 -1 39
T-0800 1 40
T-0810 -1 40
T-0820 1 41
T-0830 -1 41
T-0840 1 42
T-0850 -1 42
T-0860 1 43
T-0870 -1 43
T-0880 1 44
T-0890 -1 44
T-0900 1 45
T-0910 -1 45
T-0920 1 46
T-0930 -1 46
T-0940 1 47
T-0950 -1 47
T-0960 1 48
T-0970 -1 48
T-0980 1 49
T-0990 -1 49
T-1000 1 50
T-1010 -1 50

> FETCH ALL c WITH (timeout = '60s')

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

$ set-regex match=\d{13} replacement=<TS>

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE r WITH (PROGRESS = false) AS OF ${r-as-of} UP TO ${r-up-to}

> FETCH 7 c WITH (timeout = '60s')
<TS> 1 1 a
<TS> 1 2 b
<TS> 1 3 c
<TS> -1 2 b
<TS> -1 3 c
<TS> 1 3 z
<TS> 1 4 d

> FETCH ALL c WITH (timeout = '60s')

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT 10 / x AS q FROM e) WITH (PROGRESS)

> FETCH 2 c WITH (timeout = '60s')
<TS> true <null> <null>
<TS> false 1 10

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
INSERT INTO e VALUES (0)

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

> DELETE FROM e WHERE x = 0

# One worker, inline shares: deterministic times from constant inputs.

$ unset-regex

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 6 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0
31 false -1 1
32 false -1 2
33 false -1 0

> FETCH ALL c WITH (timeout = '60s')
100 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE sums WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 9 c WITH (timeout = '60s')
0 false 2 0
0 false 2 1
1 false 2 0
1 false 3 1
2 false 3 0
2 false 2 1
3 false 2 0
3 false 3 1
4 false 1 0

> FETCH ALL c WITH (timeout = '60s')
100 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (BATCHED) AS OF 0 UP TO 20

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 3 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0

> FETCH ALL c WITH (timeout = '60s')
20 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

# One worker, inline shares: a catch-up over 100 timestamps, from an index and from persist.

$ set-regex match=${prefix} replacement=T-

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev WHERE mz_now() >= ins AND mz_now() < del) WITH (BATCHED) AS OF ${base} UP TO ${up-to}

> FETCH 1 c WITH (timeout = '60s')
T-0000 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
T-0020 false 1 1
T-0030 false -1 1
T-0040 false 1 2
T-0050 false -1 2
T-0060 false 1 3
T-0070 false -1 3
T-0080 false 1 4
T-0090 false -1 4
T-0100 false 1 5
T-0110 false -1 5
T-0120 false 1 6
T-0130 false -1 6
T-0140 false 1 7
T-0150 false -1 7
T-0160 false 1 8
T-0170 false -1 8
T-0180 false 1 9
T-0190 false -1 9
T-0200 false 1 10
T-0210 false -1 10
T-0220 false 1 11
T-0230 false -1 11
T-0240 false 1 12
T-0250 false -1 12
T-0260 false 1 13
T-0270 false -1 13
T-0280 false 1 14
T-0290 false -1 14
T-0300 false 1 15
T-0310 false -1 15
T-0320 false 1 16
T-0330 false -1 16
T-0340 false 1 17
T-0350 false -1 17
T-0360 false 1 18
T-0370 false -1 18
T-0380 false 1 19
T-0390 false -1 19
T-0400 false 1 20
T-0410 false -1 20
T-0420 false 1 21
T-0430 false -1 21
T-0440 false 1 22
T-0450 false -1 22
T-0460 false 1 23
T-0470 false -1 23
T-0480 false 1 24
T-0490 false -1 24
T-0500 false 1 25
T-0510 false -1 25
T-0520 false 1 26
T-0530 false -1 26
T-0540 false 1 27
T-0550 false -1 27
T-0560 false 1 28
T-0570 false -1 28
T-0580 false 1 29
T-0590 false -1 29
T-0600 false 1 30
T-0610 false -1 30
T-0620 false 1 31
T-0630 false -1 31
T-0640 false 1 32
T-0650 false -1 32
T-0660 false 1 33
T-0670 false -1 33
T-0680 false 1 34
T-0690 false -1 34
T-0700 false 1 35
T-0710 false -1 35
T-0720 false 1 36
T-0730 false -1 36
T-0740 false 1 37
T-0750 false -1 37
T-0760 false 1 38
T-0770 false -1 38
T-0780 false 1 39
T-0790 false -1 39
T-0800 false 1 40
T-0810 false -1 40
T-0820 false 1 41
T-0830 false -1 41
T-0840 false 1 42
T-0850 false -1 42
T-0860 false 1 43
T-0870 false -1 43
T-0880 false 1 44
T-0890 false -1 44
T-0900 false 1 45
T-0910 false -1 45
T-0920 false 1 46
T-0930 false -1 46
T-0940 false 1 47
T-0950 false -1 47
T-0960 false 1 48
T-0970 false -1 48
T-0980 false 1 49
T-0990 false -1 49
T-1000 false 1 50
T-1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
T-2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev_mv WHERE mz_now() >= ins AND mz_now() < del) WITH (BATCHED) AS OF ${base} UP TO ${up-to}

> FETCH 1 c WITH (timeout = '60s')
T-0000 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
T-0020 false 1 1
T-0030 false -1 1
T-0040 false 1 2
T-0050 false -1 2
T-0060 false 1 3
T-0070 false -1 3
T-0080 false 1 4
T-0090 false -1 4
T-0100 false 1 5
T-0110 false -1 5
T-0120 false 1 6
T-0130 false -1 6
T-0140 false 1 7
T-0150 false -1 7
T-0160 false 1 8
T-0170 false -1 8
T-0180 false 1 9
T-0190 false -1 9
T-0200 false 1 10
T-0210 false -1 10
T-0220 false 1 11
T-0230 false -1 11
T-0240 false 1 12
T-0250 false -1 12
T-0260 false 1 13
T-0270 false -1 13
T-0280 false 1 14
T-0290 false -1 14
T-0300 false 1 15
T-0310 false -1 15
T-0320 false 1 16
T-0330 false -1 16
T-0340 false 1 17
T-0350 false -1 17
T-0360 false 1 18
T-0370 false -1 18
T-0380 false 1 19
T-0390 false -1 19
T-0400 false 1 20
T-0410 false -1 20
T-0420 false 1 21
T-0430 false -1 21
T-0440 false 1 22
T-0450 false -1 22
T-0460 false 1 23
T-0470 false -1 23
T-0480 false 1 24
T-0490 false -1 24
T-0500 false 1 25
T-0510 false -1 25
T-0520 false 1 26
T-0530 false -1 26
T-0540 false 1 27
T-0550 false -1 27
T-0560 false 1 28
T-0570 false -1 28
T-0580 false 1 29
T-0590 false -1 29
T-0600 false 1 30
T-0610 false -1 30
T-0620 false 1 31
T-0630 false -1 31
T-0640 false 1 32
T-0650 false -1 32
T-0660 false 1 33
T-0670 false -1 33
T-0680 false 1 34
T-0690 false -1 34
T-0700 false 1 35
T-0710 false -1 35
T-0720 false 1 36
T-0730 false -1 36
T-0740 false 1 37
T-0750 false -1 37
T-0760 false 1 38
T-0770 false -1 38
T-0780 false 1 39
T-0790 false -1 39
T-0800 false 1 40
T-0810 false -1 40
T-0820 false 1 41
T-0830 false -1 41
T-0840 false 1 42
T-0850 false -1 42
T-0860 false 1 43
T-0870 false -1 43
T-0880 false 1 44
T-0890 false -1 44
T-0900 false 1 45
T-0910 false -1 45
T-0920 false 1 46
T-0930 false -1 46
T-0940 false 1 47
T-0950 false -1 47
T-0960 false 1 48
T-0970 false -1 48
T-0980 false 1 49
T-0990 false -1 49
T-1000 false 1 50
T-1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
T-2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

# One worker, inline shares: retractions from writes, and an error mid-stream.

$ set-regex match=\d{13} replacement=<TS>

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE r WITH (BATCHED) AS OF ${r-as-of} UP TO ${r-up-to}

> FETCH 1 c WITH (timeout = '60s')
<TS> true <null> <null> <null>

> FETCH 7 c WITH (timeout = '60s')
<TS> false 1 1 a
<TS> false 1 2 b
<TS> false 1 3 c
<TS> false -1 2 b
<TS> false -1 3 c
<TS> false 1 3 z
<TS> false 1 4 d

> FETCH ALL c WITH (timeout = '60s')
<TS> true <null> <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT 10 / x AS q FROM e) WITH (BATCHED)

> FETCH 2 c WITH (timeout = '60s')
<TS> true <null> <null>
<TS> false 1 10

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
INSERT INTO e VALUES (0)

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

> DELETE FROM e WHERE x = 0

# Every share and every scan leaves the worker.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_batched_subscribe_inline_max_bytes = 1;
ALTER SYSTEM SET compute_batched_subscribe_offload_min_updates = 1;

# One worker, stashed shares: deterministic times from constant inputs.

$ unset-regex

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 6 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0
31 false -1 1
32 false -1 2
33 false -1 0

> FETCH ALL c WITH (timeout = '60s')
100 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE sums WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 9 c WITH (timeout = '60s')
0 false 2 0
0 false 2 1
1 false 2 0
1 false 3 1
2 false 3 0
2 false 2 1
3 false 2 0
3 false 3 1
4 false 1 0

> FETCH ALL c WITH (timeout = '60s')
100 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (BATCHED) AS OF 0 UP TO 20

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 3 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0

> FETCH ALL c WITH (timeout = '60s')
20 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

# One worker, stashed shares: a catch-up over 100 timestamps, from an index and from persist.

$ set-regex match=${prefix} replacement=T-

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev WHERE mz_now() >= ins AND mz_now() < del) WITH (BATCHED) AS OF ${base} UP TO ${up-to}

> FETCH 1 c WITH (timeout = '60s')
T-0000 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
T-0020 false 1 1
T-0030 false -1 1
T-0040 false 1 2
T-0050 false -1 2
T-0060 false 1 3
T-0070 false -1 3
T-0080 false 1 4
T-0090 false -1 4
T-0100 false 1 5
T-0110 false -1 5
T-0120 false 1 6
T-0130 false -1 6
T-0140 false 1 7
T-0150 false -1 7
T-0160 false 1 8
T-0170 false -1 8
T-0180 false 1 9
T-0190 false -1 9
T-0200 false 1 10
T-0210 false -1 10
T-0220 false 1 11
T-0230 false -1 11
T-0240 false 1 12
T-0250 false -1 12
T-0260 false 1 13
T-0270 false -1 13
T-0280 false 1 14
T-0290 false -1 14
T-0300 false 1 15
T-0310 false -1 15
T-0320 false 1 16
T-0330 false -1 16
T-0340 false 1 17
T-0350 false -1 17
T-0360 false 1 18
T-0370 false -1 18
T-0380 false 1 19
T-0390 false -1 19
T-0400 false 1 20
T-0410 false -1 20
T-0420 false 1 21
T-0430 false -1 21
T-0440 false 1 22
T-0450 false -1 22
T-0460 false 1 23
T-0470 false -1 23
T-0480 false 1 24
T-0490 false -1 24
T-0500 false 1 25
T-0510 false -1 25
T-0520 false 1 26
T-0530 false -1 26
T-0540 false 1 27
T-0550 false -1 27
T-0560 false 1 28
T-0570 false -1 28
T-0580 false 1 29
T-0590 false -1 29
T-0600 false 1 30
T-0610 false -1 30
T-0620 false 1 31
T-0630 false -1 31
T-0640 false 1 32
T-0650 false -1 32
T-0660 false 1 33
T-0670 false -1 33
T-0680 false 1 34
T-0690 false -1 34
T-0700 false 1 35
T-0710 false -1 35
T-0720 false 1 36
T-0730 false -1 36
T-0740 false 1 37
T-0750 false -1 37
T-0760 false 1 38
T-0770 false -1 38
T-0780 false 1 39
T-0790 false -1 39
T-0800 false 1 40
T-0810 false -1 40
T-0820 false 1 41
T-0830 false -1 41
T-0840 false 1 42
T-0850 false -1 42
T-0860 false 1 43
T-0870 false -1 43
T-0880 false 1 44
T-0890 false -1 44
T-0900 false 1 45
T-0910 false -1 45
T-0920 false 1 46
T-0930 false -1 46
T-0940 false 1 47
T-0950 false -1 47
T-0960 false 1 48
T-0970 false -1 48
T-0980 false 1 49
T-0990 false -1 49
T-1000 false 1 50
T-1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
T-2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev_mv WHERE mz_now() >= ins AND mz_now() < del) WITH (BATCHED) AS OF ${base} UP TO ${up-to}

> FETCH 1 c WITH (timeout = '60s')
T-0000 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
T-0020 false 1 1
T-0030 false -1 1
T-0040 false 1 2
T-0050 false -1 2
T-0060 false 1 3
T-0070 false -1 3
T-0080 false 1 4
T-0090 false -1 4
T-0100 false 1 5
T-0110 false -1 5
T-0120 false 1 6
T-0130 false -1 6
T-0140 false 1 7
T-0150 false -1 7
T-0160 false 1 8
T-0170 false -1 8
T-0180 false 1 9
T-0190 false -1 9
T-0200 false 1 10
T-0210 false -1 10
T-0220 false 1 11
T-0230 false -1 11
T-0240 false 1 12
T-0250 false -1 12
T-0260 false 1 13
T-0270 false -1 13
T-0280 false 1 14
T-0290 false -1 14
T-0300 false 1 15
T-0310 false -1 15
T-0320 false 1 16
T-0330 false -1 16
T-0340 false 1 17
T-0350 false -1 17
T-0360 false 1 18
T-0370 false -1 18
T-0380 false 1 19
T-0390 false -1 19
T-0400 false 1 20
T-0410 false -1 20
T-0420 false 1 21
T-0430 false -1 21
T-0440 false 1 22
T-0450 false -1 22
T-0460 false 1 23
T-0470 false -1 23
T-0480 false 1 24
T-0490 false -1 24
T-0500 false 1 25
T-0510 false -1 25
T-0520 false 1 26
T-0530 false -1 26
T-0540 false 1 27
T-0550 false -1 27
T-0560 false 1 28
T-0570 false -1 28
T-0580 false 1 29
T-0590 false -1 29
T-0600 false 1 30
T-0610 false -1 30
T-0620 false 1 31
T-0630 false -1 31
T-0640 false 1 32
T-0650 false -1 32
T-0660 false 1 33
T-0670 false -1 33
T-0680 false 1 34
T-0690 false -1 34
T-0700 false 1 35
T-0710 false -1 35
T-0720 false 1 36
T-0730 false -1 36
T-0740 false 1 37
T-0750 false -1 37
T-0760 false 1 38
T-0770 false -1 38
T-0780 false 1 39
T-0790 false -1 39
T-0800 false 1 40
T-0810 false -1 40
T-0820 false 1 41
T-0830 false -1 41
T-0840 false 1 42
T-0850 false -1 42
T-0860 false 1 43
T-0870 false -1 43
T-0880 false 1 44
T-0890 false -1 44
T-0900 false 1 45
T-0910 false -1 45
T-0920 false 1 46
T-0930 false -1 46
T-0940 false 1 47
T-0950 false -1 47
T-0960 false 1 48
T-0970 false -1 48
T-0980 false 1 49
T-0990 false -1 49
T-1000 false 1 50
T-1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
T-2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

# One worker, stashed shares: retractions from writes, and an error mid-stream.

$ set-regex match=\d{13} replacement=<TS>

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE r WITH (BATCHED) AS OF ${r-as-of} UP TO ${r-up-to}

> FETCH 1 c WITH (timeout = '60s')
<TS> true <null> <null> <null>

> FETCH 7 c WITH (timeout = '60s')
<TS> false 1 1 a
<TS> false 1 2 b
<TS> false 1 3 c
<TS> false -1 2 b
<TS> false -1 3 c
<TS> false 1 3 z
<TS> false 1 4 d

> FETCH ALL c WITH (timeout = '60s')
<TS> true <null> <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT 10 / x AS q FROM e) WITH (BATCHED)

> FETCH 2 c WITH (timeout = '60s')
<TS> true <null> <null>
<TS> false 1 10

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
INSERT INTO e VALUES (0)

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

> DELETE FROM e WHERE x = 0

> SELECT sum(value) > 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_batched_subscribe_share_bytes_total' AND labels -> 'tier' = 'stashed'
true

> SET cluster = batched4

# Four workers, stashed shares: deterministic times from constant inputs.

$ unset-regex

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 6 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0
31 false -1 1
32 false -1 2
33 false -1 0

> FETCH ALL c WITH (timeout = '60s')
100 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE sums WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 9 c WITH (timeout = '60s')
0 false 2 0
0 false 2 1
1 false 2 0
1 false 3 1
2 false 3 0
2 false 2 1
3 false 2 0
3 false 3 1
4 false 1 0

> FETCH ALL c WITH (timeout = '60s')
100 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (BATCHED) AS OF 0 UP TO 20

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 3 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0

> FETCH ALL c WITH (timeout = '60s')
20 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

# Four workers, stashed shares: a catch-up over 100 timestamps, from an index and from persist.

$ set-regex match=${prefix} replacement=T-

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev WHERE mz_now() >= ins AND mz_now() < del) WITH (BATCHED) AS OF ${base} UP TO ${up-to}

> FETCH 1 c WITH (timeout = '60s')
T-0000 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
T-0020 false 1 1
T-0030 false -1 1
T-0040 false 1 2
T-0050 false -1 2
T-0060 false 1 3
T-0070 false -1 3
T-0080 false 1 4
T-0090 false -1 4
T-0100 false 1 5
T-0110 false -1 5
T-0120 false 1 6
T-0130 false -1 6
T-0140 false 1 7
T-0150 false -1 7
T-0160 false 1 8
T-0170 false -1 8
T-0180 false 1 9
T-0190 false -1 9
T-0200 false 1 10
T-0210 false -1 10
T-0220 false 1 11
T-0230 false -1 11
T-0240 false 1 12
T-0250 false -1 12
T-0260 false 1 13
T-0270 false -1 13
T-0280 false 1 14
T-0290 false -1 14
T-0300 false 1 15
T-0310 false -1 15
T-0320 false 1 16
T-0330 false -1 16
T-0340 false 1 17
T-0350 false -1 17
T-0360 false 1 18
T-0370 false -1 18
T-0380 false 1 19
T-0390 false -1 19
T-0400 false 1 20
T-0410 false -1 20
T-0420 false 1 21
T-0430 false -1 21
T-0440 false 1 22
T-0450 false -1 22
T-0460 false 1 23
T-0470 false -1 23
T-0480 false 1 24
T-0490 false -1 24
T-0500 false 1 25
T-0510 false -1 25
T-0520 false 1 26
T-0530 false -1 26
T-0540 false 1 27
T-0550 false -1 27
T-0560 false 1 28
T-0570 false -1 28
T-0580 false 1 29
T-0590 false -1 29
T-0600 false 1 30
T-0610 false -1 30
T-0620 false 1 31
T-0630 false -1 31
T-0640 false 1 32
T-0650 false -1 32
T-0660 false 1 33
T-0670 false -1 33
T-0680 false 1 34
T-0690 false -1 34
T-0700 false 1 35
T-0710 false -1 35
T-0720 false 1 36
T-0730 false -1 36
T-0740 false 1 37
T-0750 false -1 37
T-0760 false 1 38
T-0770 false -1 38
T-0780 false 1 39
T-0790 false -1 39
T-0800 false 1 40
T-0810 false -1 40
T-0820 false 1 41
T-0830 false -1 41
T-0840 false 1 42
T-0850 false -1 42
T-0860 false 1 43
T-0870 false -1 43
T-0880 false 1 44
T-0890 false -1 44
T-0900 false 1 45
T-0910 false -1 45
T-0920 false 1 46
T-0930 false -1 46
T-0940 false 1 47
T-0950 false -1 47
T-0960 false 1 48
T-0970 false -1 48
T-0980 false 1 49
T-0990 false -1 49
T-1000 false 1 50
T-1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
T-2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev_mv WHERE mz_now() >= ins AND mz_now() < del) WITH (BATCHED) AS OF ${base} UP TO ${up-to}

> FETCH 1 c WITH (timeout = '60s')
T-0000 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
T-0020 false 1 1
T-0030 false -1 1
T-0040 false 1 2
T-0050 false -1 2
T-0060 false 1 3
T-0070 false -1 3
T-0080 false 1 4
T-0090 false -1 4
T-0100 false 1 5
T-0110 false -1 5
T-0120 false 1 6
T-0130 false -1 6
T-0140 false 1 7
T-0150 false -1 7
T-0160 false 1 8
T-0170 false -1 8
T-0180 false 1 9
T-0190 false -1 9
T-0200 false 1 10
T-0210 false -1 10
T-0220 false 1 11
T-0230 false -1 11
T-0240 false 1 12
T-0250 false -1 12
T-0260 false 1 13
T-0270 false -1 13
T-0280 false 1 14
T-0290 false -1 14
T-0300 false 1 15
T-0310 false -1 15
T-0320 false 1 16
T-0330 false -1 16
T-0340 false 1 17
T-0350 false -1 17
T-0360 false 1 18
T-0370 false -1 18
T-0380 false 1 19
T-0390 false -1 19
T-0400 false 1 20
T-0410 false -1 20
T-0420 false 1 21
T-0430 false -1 21
T-0440 false 1 22
T-0450 false -1 22
T-0460 false 1 23
T-0470 false -1 23
T-0480 false 1 24
T-0490 false -1 24
T-0500 false 1 25
T-0510 false -1 25
T-0520 false 1 26
T-0530 false -1 26
T-0540 false 1 27
T-0550 false -1 27
T-0560 false 1 28
T-0570 false -1 28
T-0580 false 1 29
T-0590 false -1 29
T-0600 false 1 30
T-0610 false -1 30
T-0620 false 1 31
T-0630 false -1 31
T-0640 false 1 32
T-0650 false -1 32
T-0660 false 1 33
T-0670 false -1 33
T-0680 false 1 34
T-0690 false -1 34
T-0700 false 1 35
T-0710 false -1 35
T-0720 false 1 36
T-0730 false -1 36
T-0740 false 1 37
T-0750 false -1 37
T-0760 false 1 38
T-0770 false -1 38
T-0780 false 1 39
T-0790 false -1 39
T-0800 false 1 40
T-0810 false -1 40
T-0820 false 1 41
T-0830 false -1 41
T-0840 false 1 42
T-0850 false -1 42
T-0860 false 1 43
T-0870 false -1 43
T-0880 false 1 44
T-0890 false -1 44
T-0900 false 1 45
T-0910 false -1 45
T-0920 false 1 46
T-0930 false -1 46
T-0940 false 1 47
T-0950 false -1 47
T-0960 false 1 48
T-0970 false -1 48
T-0980 false 1 49
T-0990 false -1 49
T-1000 false 1 50
T-1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
T-2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

# Four workers, stashed shares: retractions from writes, and an error mid-stream.

$ set-regex match=\d{13} replacement=<TS>

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE r WITH (BATCHED) AS OF ${r-as-of} UP TO ${r-up-to}

> FETCH 1 c WITH (timeout = '60s')
<TS> true <null> <null> <null>

> FETCH 7 c WITH (timeout = '60s')
<TS> false 1 1 a
<TS> false 1 2 b
<TS> false 1 3 c
<TS> false -1 2 b
<TS> false -1 3 c
<TS> false 1 3 z
<TS> false 1 4 d

> FETCH ALL c WITH (timeout = '60s')
<TS> true <null> <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT 10 / x AS q FROM e) WITH (BATCHED)

> FETCH 2 c WITH (timeout = '60s')
<TS> true <null> <null>
<TS> false 1 10

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
INSERT INTO e VALUES (0)

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

> DELETE FROM e WHERE x = 0

> SELECT sum(value) > 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_batched_subscribe_share_bytes_total' AND labels -> 'tier' = 'stashed'
true

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET compute_batched_subscribe_inline_max_bytes;
ALTER SYSTEM RESET compute_batched_subscribe_offload_min_updates;

# Four workers, inline shares: deterministic times from constant inputs.

$ unset-regex

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 6 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0
31 false -1 1
32 false -1 2
33 false -1 0

> FETCH ALL c WITH (timeout = '60s')
100 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE sums WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 9 c WITH (timeout = '60s')
0 false 2 0
0 false 2 1
1 false 2 0
1 false 3 1
2 false 3 0
2 false 2 1
3 false 2 0
3 false 3 1
4 false 1 0

> FETCH ALL c WITH (timeout = '60s')
100 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE lag3 WITH (BATCHED) AS OF 0 UP TO 20

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 3 c WITH (timeout = '60s')
1 false 1 1
2 false 1 2
3 false 1 0

> FETCH ALL c WITH (timeout = '60s')
20 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (BATCHED) AS OF 0 UP TO 100

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

# Four workers, inline shares: a catch-up over 100 timestamps, from an index and from persist.

$ set-regex match=${prefix} replacement=T-

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev WHERE mz_now() >= ins AND mz_now() < del) WITH (BATCHED) AS OF ${base} UP TO ${up-to}

> FETCH 1 c WITH (timeout = '60s')
T-0000 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
T-0020 false 1 1
T-0030 false -1 1
T-0040 false 1 2
T-0050 false -1 2
T-0060 false 1 3
T-0070 false -1 3
T-0080 false 1 4
T-0090 false -1 4
T-0100 false 1 5
T-0110 false -1 5
T-0120 false 1 6
T-0130 false -1 6
T-0140 false 1 7
T-0150 false -1 7
T-0160 false 1 8
T-0170 false -1 8
T-0180 false 1 9
T-0190 false -1 9
T-0200 false 1 10
T-0210 false -1 10
T-0220 false 1 11
T-0230 false -1 11
T-0240 false 1 12
T-0250 false -1 12
T-0260 false 1 13
T-0270 false -1 13
T-0280 false 1 14
T-0290 false -1 14
T-0300 false 1 15
T-0310 false -1 15
T-0320 false 1 16
T-0330 false -1 16
T-0340 false 1 17
T-0350 false -1 17
T-0360 false 1 18
T-0370 false -1 18
T-0380 false 1 19
T-0390 false -1 19
T-0400 false 1 20
T-0410 false -1 20
T-0420 false 1 21
T-0430 false -1 21
T-0440 false 1 22
T-0450 false -1 22
T-0460 false 1 23
T-0470 false -1 23
T-0480 false 1 24
T-0490 false -1 24
T-0500 false 1 25
T-0510 false -1 25
T-0520 false 1 26
T-0530 false -1 26
T-0540 false 1 27
T-0550 false -1 27
T-0560 false 1 28
T-0570 false -1 28
T-0580 false 1 29
T-0590 false -1 29
T-0600 false 1 30
T-0610 false -1 30
T-0620 false 1 31
T-0630 false -1 31
T-0640 false 1 32
T-0650 false -1 32
T-0660 false 1 33
T-0670 false -1 33
T-0680 false 1 34
T-0690 false -1 34
T-0700 false 1 35
T-0710 false -1 35
T-0720 false 1 36
T-0730 false -1 36
T-0740 false 1 37
T-0750 false -1 37
T-0760 false 1 38
T-0770 false -1 38
T-0780 false 1 39
T-0790 false -1 39
T-0800 false 1 40
T-0810 false -1 40
T-0820 false 1 41
T-0830 false -1 41
T-0840 false 1 42
T-0850 false -1 42
T-0860 false 1 43
T-0870 false -1 43
T-0880 false 1 44
T-0890 false -1 44
T-0900 false 1 45
T-0910 false -1 45
T-0920 false 1 46
T-0930 false -1 46
T-0940 false 1 47
T-0950 false -1 47
T-0960 false 1 48
T-0970 false -1 48
T-0980 false 1 49
T-0990 false -1 49
T-1000 false 1 50
T-1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
T-2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM ev_mv WHERE mz_now() >= ins AND mz_now() < del) WITH (BATCHED) AS OF ${base} UP TO ${up-to}

> FETCH 1 c WITH (timeout = '60s')
T-0000 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
T-0020 false 1 1
T-0030 false -1 1
T-0040 false 1 2
T-0050 false -1 2
T-0060 false 1 3
T-0070 false -1 3
T-0080 false 1 4
T-0090 false -1 4
T-0100 false 1 5
T-0110 false -1 5
T-0120 false 1 6
T-0130 false -1 6
T-0140 false 1 7
T-0150 false -1 7
T-0160 false 1 8
T-0170 false -1 8
T-0180 false 1 9
T-0190 false -1 9
T-0200 false 1 10
T-0210 false -1 10
T-0220 false 1 11
T-0230 false -1 11
T-0240 false 1 12
T-0250 false -1 12
T-0260 false 1 13
T-0270 false -1 13
T-0280 false 1 14
T-0290 false -1 14
T-0300 false 1 15
T-0310 false -1 15
T-0320 false 1 16
T-0330 false -1 16
T-0340 false 1 17
T-0350 false -1 17
T-0360 false 1 18
T-0370 false -1 18
T-0380 false 1 19
T-0390 false -1 19
T-0400 false 1 20
T-0410 false -1 20
T-0420 false 1 21
T-0430 false -1 21
T-0440 false 1 22
T-0450 false -1 22
T-0460 false 1 23
T-0470 false -1 23
T-0480 false 1 24
T-0490 false -1 24
T-0500 false 1 25
T-0510 false -1 25
T-0520 false 1 26
T-0530 false -1 26
T-0540 false 1 27
T-0550 false -1 27
T-0560 false 1 28
T-0570 false -1 28
T-0580 false 1 29
T-0590 false -1 29
T-0600 false 1 30
T-0610 false -1 30
T-0620 false 1 31
T-0630 false -1 31
T-0640 false 1 32
T-0650 false -1 32
T-0660 false 1 33
T-0670 false -1 33
T-0680 false 1 34
T-0690 false -1 34
T-0700 false 1 35
T-0710 false -1 35
T-0720 false 1 36
T-0730 false -1 36
T-0740 false 1 37
T-0750 false -1 37
T-0760 false 1 38
T-0770 false -1 38
T-0780 false 1 39
T-0790 false -1 39
T-0800 false 1 40
T-0810 false -1 40
T-0820 false 1 41
T-0830 false -1 41
T-0840 false 1 42
T-0850 false -1 42
T-0860 false 1 43
T-0870 false -1 43
T-0880 false 1 44
T-0890 false -1 44
T-0900 false 1 45
T-0910 false -1 45
T-0920 false 1 46
T-0930 false -1 46
T-0940 false 1 47
T-0950 false -1 47
T-0960 false 1 48
T-0970 false -1 48
T-0980 false 1 49
T-0990 false -1 49
T-1000 false 1 50
T-1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
T-2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

# Four workers, inline shares: retractions from writes, and an error mid-stream.

$ set-regex match=\d{13} replacement=<TS>

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE r WITH (BATCHED) AS OF ${r-as-of} UP TO ${r-up-to}

> FETCH 1 c WITH (timeout = '60s')
<TS> true <null> <null> <null>

> FETCH 7 c WITH (timeout = '60s')
<TS> false 1 1 a
<TS> false 1 2 b
<TS> false 1 3 c
<TS> false -1 2 b
<TS> false -1 3 c
<TS> false 1 3 z
<TS> false 1 4 d

> FETCH ALL c WITH (timeout = '60s')
<TS> true <null> <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT 10 / x AS q FROM e) WITH (BATCHED)

> FETCH 2 c WITH (timeout = '60s')
<TS> true <null> <null>
<TS> false 1 10

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
INSERT INTO e VALUES (0)

! FETCH ALL c WITH (timeout = '60s')
contains:division by zero

> ROLLBACK

> DELETE FROM e WHERE x = 0

> SET cluster = quickstart

> DROP CLUSTER batched4 CASCADE
