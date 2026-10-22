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
# Rows between two progress rows arrive in no particular order, and testdrive sorts the rows of
# each FETCH, so every FETCH below covers exactly one progress interval. That holds only where the
# batch boundaries are fixed: the inputs are constant, so their frontier is empty from the start
# and a subscribe cuts one description, clamped to `UP TO`. Catch-ups over live inputs, which may
# split anywhere, are tested in `test_subscribe_batched_catch_up_any_split`.
#
# NOTE: A batched subscribe with `UP TO` reports a progress row at `UP TO` before it completes. A
# plain subscribe completes without one.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_subscribe_batched = true;

> CREATE CLUSTER batched4 SIZE 'scale=1,workers=4'

# Constant inputs with temporal filters change at fixed times.
#
# `catchup` holds 50 rows that each live for 10 timestamps, so it changes at 100 distinct times.
> CREATE VIEW catchup AS
  SELECT x AS a FROM generate_series(1, 50) AS x
  WHERE mz_now() >= 20 * x AND mz_now() < 20 * x + 10

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

# `e` fails once it holds a 0. Its snapshot is the first batch, whatever the later boundaries.
> CREATE TABLE e (x int)
> INSERT INTO e VALUES (1)

# Plain subscribes, the reference for the batched cases below.

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE catchup WITH (PROGRESS) AS OF 0 UP TO 2000

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
20 false 1 1
30 false -1 1
40 false 1 2
50 false -1 2
60 false 1 3
70 false -1 3
80 false 1 4
90 false -1 4
100 false 1 5
110 false -1 5
120 false 1 6
130 false -1 6
140 false 1 7
150 false -1 7
160 false 1 8
170 false -1 8
180 false 1 9
190 false -1 9
200 false 1 10
210 false -1 10
220 false 1 11
230 false -1 11
240 false 1 12
250 false -1 12
260 false 1 13
270 false -1 13
280 false 1 14
290 false -1 14
300 false 1 15
310 false -1 15
320 false 1 16
330 false -1 16
340 false 1 17
350 false -1 17
360 false 1 18
370 false -1 18
380 false 1 19
390 false -1 19
400 false 1 20
410 false -1 20
420 false 1 21
430 false -1 21
440 false 1 22
450 false -1 22
460 false 1 23
470 false -1 23
480 false 1 24
490 false -1 24
500 false 1 25
510 false -1 25
520 false 1 26
530 false -1 26
540 false 1 27
550 false -1 27
560 false 1 28
570 false -1 28
580 false 1 29
590 false -1 29
600 false 1 30
610 false -1 30
620 false 1 31
630 false -1 31
640 false 1 32
650 false -1 32
660 false 1 33
670 false -1 33
680 false 1 34
690 false -1 34
700 false 1 35
710 false -1 35
720 false 1 36
730 false -1 36
740 false 1 37
750 false -1 37
760 false 1 38
770 false -1 38
780 false 1 39
790 false -1 39
800 false 1 40
810 false -1 40
820 false 1 41
830 false -1 41
840 false 1 42
850 false -1 42
860 false 1 43
870 false -1 43
880 false 1 44
890 false -1 44
900 false 1 45
910 false -1 45
920 false 1 46
930 false -1 46
940 false 1 47
950 false -1 47
960 false 1 48
970 false -1 48
980 false 1 49
990 false -1 49
1000 false 1 50
1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

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

# `err` is a constant, so it holds its error from before the as-of 7, in the snapshot that a
# subscribe without one does not report.
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (PROGRESS, SNAPSHOT = false) AS OF 7 UP TO 12

> FETCH 1 c WITH (timeout = '60s')
7 true <null> <null>

> FETCH ALL c WITH (timeout = '60s')

> COMMIT

$ set-regex match=\d{13} replacement=<TS>

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

$ unset-regex

# One worker, inline shares.

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE catchup WITH (BATCHED) AS OF 0 UP TO 2000

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
20 false 1 1
30 false -1 1
40 false 1 2
50 false -1 2
60 false 1 3
70 false -1 3
80 false 1 4
90 false -1 4
100 false 1 5
110 false -1 5
120 false 1 6
130 false -1 6
140 false 1 7
150 false -1 7
160 false 1 8
170 false -1 8
180 false 1 9
190 false -1 9
200 false 1 10
210 false -1 10
220 false 1 11
230 false -1 11
240 false 1 12
250 false -1 12
260 false 1 13
270 false -1 13
280 false 1 14
290 false -1 14
300 false 1 15
310 false -1 15
320 false 1 16
330 false -1 16
340 false 1 17
350 false -1 17
360 false 1 18
370 false -1 18
380 false 1 19
390 false -1 19
400 false 1 20
410 false -1 20
420 false 1 21
430 false -1 21
440 false 1 22
450 false -1 22
460 false 1 23
470 false -1 23
480 false 1 24
490 false -1 24
500 false 1 25
510 false -1 25
520 false 1 26
530 false -1 26
540 false 1 27
550 false -1 27
560 false 1 28
570 false -1 28
580 false 1 29
590 false -1 29
600 false 1 30
610 false -1 30
620 false 1 31
630 false -1 31
640 false 1 32
650 false -1 32
660 false 1 33
670 false -1 33
680 false 1 34
690 false -1 34
700 false 1 35
710 false -1 35
720 false 1 36
730 false -1 36
740 false 1 37
750 false -1 37
760 false 1 38
770 false -1 38
780 false 1 39
790 false -1 39
800 false 1 40
810 false -1 40
820 false 1 41
830 false -1 41
840 false 1 42
850 false -1 42
860 false 1 43
870 false -1 43
880 false 1 44
890 false -1 44
900 false 1 45
910 false -1 45
920 false 1 46
930 false -1 46
940 false 1 47
950 false -1 47
960 false 1 48
970 false -1 48
980 false 1 49
990 false -1 49
1000 false 1 50
1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

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

# `err` is a constant, so it holds its error from before the as-of 7, in the snapshot that a
# subscribe without one does not report.
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (BATCHED, SNAPSHOT = false) AS OF 7 UP TO 12

> FETCH 1 c WITH (timeout = '60s')
7 true <null> <null>

> FETCH ALL c WITH (timeout = '60s')
12 true <null> <null>

> COMMIT

$ set-regex match=\d{13} replacement=<TS>

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

$ unset-regex

# Every share and every scan leaves the worker.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_batched_subscribe_inline_max_bytes = 1;
ALTER SYSTEM SET compute_batched_subscribe_offload_min_updates = 1;

# One worker, stashed shares.

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE catchup WITH (BATCHED) AS OF 0 UP TO 2000

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
20 false 1 1
30 false -1 1
40 false 1 2
50 false -1 2
60 false 1 3
70 false -1 3
80 false 1 4
90 false -1 4
100 false 1 5
110 false -1 5
120 false 1 6
130 false -1 6
140 false 1 7
150 false -1 7
160 false 1 8
170 false -1 8
180 false 1 9
190 false -1 9
200 false 1 10
210 false -1 10
220 false 1 11
230 false -1 11
240 false 1 12
250 false -1 12
260 false 1 13
270 false -1 13
280 false 1 14
290 false -1 14
300 false 1 15
310 false -1 15
320 false 1 16
330 false -1 16
340 false 1 17
350 false -1 17
360 false 1 18
370 false -1 18
380 false 1 19
390 false -1 19
400 false 1 20
410 false -1 20
420 false 1 21
430 false -1 21
440 false 1 22
450 false -1 22
460 false 1 23
470 false -1 23
480 false 1 24
490 false -1 24
500 false 1 25
510 false -1 25
520 false 1 26
530 false -1 26
540 false 1 27
550 false -1 27
560 false 1 28
570 false -1 28
580 false 1 29
590 false -1 29
600 false 1 30
610 false -1 30
620 false 1 31
630 false -1 31
640 false 1 32
650 false -1 32
660 false 1 33
670 false -1 33
680 false 1 34
690 false -1 34
700 false 1 35
710 false -1 35
720 false 1 36
730 false -1 36
740 false 1 37
750 false -1 37
760 false 1 38
770 false -1 38
780 false 1 39
790 false -1 39
800 false 1 40
810 false -1 40
820 false 1 41
830 false -1 41
840 false 1 42
850 false -1 42
860 false 1 43
870 false -1 43
880 false 1 44
890 false -1 44
900 false 1 45
910 false -1 45
920 false 1 46
930 false -1 46
940 false 1 47
950 false -1 47
960 false 1 48
970 false -1 48
980 false 1 49
990 false -1 49
1000 false 1 50
1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

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

# `err` is a constant, so it holds its error from before the as-of 7, in the snapshot that a
# subscribe without one does not report.
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (BATCHED, SNAPSHOT = false) AS OF 7 UP TO 12

> FETCH 1 c WITH (timeout = '60s')
7 true <null> <null>

> FETCH ALL c WITH (timeout = '60s')
12 true <null> <null>

> COMMIT

$ set-regex match=\d{13} replacement=<TS>

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

$ unset-regex

> SELECT sum(value) > 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_batched_subscribe_share_bytes_total' AND labels -> 'tier' = 'stashed'
true

> SET cluster = batched4

# Four workers, stashed shares.

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE catchup WITH (BATCHED) AS OF 0 UP TO 2000

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
20 false 1 1
30 false -1 1
40 false 1 2
50 false -1 2
60 false 1 3
70 false -1 3
80 false 1 4
90 false -1 4
100 false 1 5
110 false -1 5
120 false 1 6
130 false -1 6
140 false 1 7
150 false -1 7
160 false 1 8
170 false -1 8
180 false 1 9
190 false -1 9
200 false 1 10
210 false -1 10
220 false 1 11
230 false -1 11
240 false 1 12
250 false -1 12
260 false 1 13
270 false -1 13
280 false 1 14
290 false -1 14
300 false 1 15
310 false -1 15
320 false 1 16
330 false -1 16
340 false 1 17
350 false -1 17
360 false 1 18
370 false -1 18
380 false 1 19
390 false -1 19
400 false 1 20
410 false -1 20
420 false 1 21
430 false -1 21
440 false 1 22
450 false -1 22
460 false 1 23
470 false -1 23
480 false 1 24
490 false -1 24
500 false 1 25
510 false -1 25
520 false 1 26
530 false -1 26
540 false 1 27
550 false -1 27
560 false 1 28
570 false -1 28
580 false 1 29
590 false -1 29
600 false 1 30
610 false -1 30
620 false 1 31
630 false -1 31
640 false 1 32
650 false -1 32
660 false 1 33
670 false -1 33
680 false 1 34
690 false -1 34
700 false 1 35
710 false -1 35
720 false 1 36
730 false -1 36
740 false 1 37
750 false -1 37
760 false 1 38
770 false -1 38
780 false 1 39
790 false -1 39
800 false 1 40
810 false -1 40
820 false 1 41
830 false -1 41
840 false 1 42
850 false -1 42
860 false 1 43
870 false -1 43
880 false 1 44
890 false -1 44
900 false 1 45
910 false -1 45
920 false 1 46
930 false -1 46
940 false 1 47
950 false -1 47
960 false 1 48
970 false -1 48
980 false 1 49
990 false -1 49
1000 false 1 50
1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

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

# `err` is a constant, so it holds its error from before the as-of 7, in the snapshot that a
# subscribe without one does not report.
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (BATCHED, SNAPSHOT = false) AS OF 7 UP TO 12

> FETCH 1 c WITH (timeout = '60s')
7 true <null> <null>

> FETCH ALL c WITH (timeout = '60s')
12 true <null> <null>

> COMMIT

$ set-regex match=\d{13} replacement=<TS>

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

$ unset-regex

> SELECT sum(value) > 0 FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_compute_batched_subscribe_share_bytes_total' AND labels -> 'tier' = 'stashed'
true

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET compute_batched_subscribe_inline_max_bytes;
ALTER SYSTEM RESET compute_batched_subscribe_offload_min_updates;

# Four workers, inline shares.

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE catchup WITH (BATCHED) AS OF 0 UP TO 2000

> FETCH 1 c WITH (timeout = '60s')
0 true <null> <null>

> FETCH 100 c WITH (timeout = '60s')
20 false 1 1
30 false -1 1
40 false 1 2
50 false -1 2
60 false 1 3
70 false -1 3
80 false 1 4
90 false -1 4
100 false 1 5
110 false -1 5
120 false 1 6
130 false -1 6
140 false 1 7
150 false -1 7
160 false 1 8
170 false -1 8
180 false 1 9
190 false -1 9
200 false 1 10
210 false -1 10
220 false 1 11
230 false -1 11
240 false 1 12
250 false -1 12
260 false 1 13
270 false -1 13
280 false 1 14
290 false -1 14
300 false 1 15
310 false -1 15
320 false 1 16
330 false -1 16
340 false 1 17
350 false -1 17
360 false 1 18
370 false -1 18
380 false 1 19
390 false -1 19
400 false 1 20
410 false -1 20
420 false 1 21
430 false -1 21
440 false 1 22
450 false -1 22
460 false 1 23
470 false -1 23
480 false 1 24
490 false -1 24
500 false 1 25
510 false -1 25
520 false 1 26
530 false -1 26
540 false 1 27
550 false -1 27
560 false 1 28
570 false -1 28
580 false 1 29
590 false -1 29
600 false 1 30
610 false -1 30
620 false 1 31
630 false -1 31
640 false 1 32
650 false -1 32
660 false 1 33
670 false -1 33
680 false 1 34
690 false -1 34
700 false 1 35
710 false -1 35
720 false 1 36
730 false -1 36
740 false 1 37
750 false -1 37
760 false 1 38
770 false -1 38
780 false 1 39
790 false -1 39
800 false 1 40
810 false -1 40
820 false 1 41
830 false -1 41
840 false 1 42
850 false -1 42
860 false 1 43
870 false -1 43
880 false 1 44
890 false -1 44
900 false 1 45
910 false -1 45
920 false 1 46
930 false -1 46
940 false 1 47
950 false -1 47
960 false 1 48
970 false -1 48
980 false 1 49
990 false -1 49
1000 false 1 50
1010 false -1 50

> FETCH ALL c WITH (timeout = '60s')
2000 true <null> <null>

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> COMMIT

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

# `err` is a constant, so it holds its error from before the as-of 7, in the snapshot that a
# subscribe without one does not report.
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE err WITH (BATCHED, SNAPSHOT = false) AS OF 7 UP TO 12

> FETCH 1 c WITH (timeout = '60s')
7 true <null> <null>

> FETCH ALL c WITH (timeout = '60s')
12 true <null> <null>

> COMMIT

$ set-regex match=\d{13} replacement=<TS>

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

$ unset-regex

> SET cluster = quickstart

> DROP CLUSTER batched4 CASCADE
