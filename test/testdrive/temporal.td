# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Using mz_now() with a VIEW source
#

> CREATE VIEW one_ts (ts) AS VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(NULL)

> CREATE MATERIALIZED VIEW one_bound1 AS SELECT * FROM one_ts WHERE mz_now() >= ts

> SELECT * FROM one_bound1 AS OF 0

> SELECT * FROM one_bound1 AS OF 2
1
2

> CREATE MATERIALIZED VIEW one_bound2 AS SELECT * FROM one_ts WHERE mz_now() <= ts

> SELECT * FROM one_bound2 AS OF 0
1
2
3
4
5
6
7
8
9
10

> SELECT * FROM one_bound2 AS OF 2
2
3
4
5
6
7
8
9
10

> SELECT * FROM one_bound2 AS OF 10
10

> SELECT * FROM one_bound2 AS OF 11

> CREATE MATERIALIZED VIEW two_bounds AS SELECT * FROM one_ts WHERE mz_now() >= ts AND mz_now() >= ts + 3

> SELECT * FROM two_bounds AS OF 2

> SELECT * FROM two_bounds AS OF 5
ts
---
1
2

> CREATE MATERIALIZED VIEW two_bounds2 AS SELECT * FROM one_ts WHERE mz_now() <= ts AND mz_now() <= ts + 3

> SELECT * FROM two_bounds2 AS OF 2
ts
---
2
3
4
5
6
7
8
9
10

> SELECT * FROM two_bounds2 AS OF 9
ts
---
9
10

> CREATE MATERIALIZED VIEW two_bounds_between AS SELECT * FROM one_ts WHERE mz_now() BETWEEN ts AND ts + 3

> SELECT * FROM two_bounds_between AS OF 3
ts
---
1
2
3

> SELECT * FROM two_bounds_between AS OF 5
ts
---
2
3
4
5

> CREATE MATERIALIZED VIEW two_bounds_and AS SELECT * FROM one_ts WHERE mz_now() >= ts AND mz_now() <= ts + 3

> SELECT * FROM two_bounds_and AS OF 3

ts
---
1
2
3

> SELECT * FROM two_bounds_and AS OF 5
ts
---
2
3
4
5

> CREATE VIEW two_ts (ts1,ts2) AS VALUES (1,1+5),(2,2+5),(3,3+5),(4,4+5),(5,5+5),(6,6+5),(7,7+5),(8,8+5),(9,9+5),(10,10+5)

> DROP VIEW IF EXISTS two_bounds_two_cols CASCADE

> CREATE MATERIALIZED VIEW two_bounds_two_cols AS SELECT * FROM two_ts WHERE mz_now() >= ts1 AND mz_now() >= ts2

> SELECT * FROM two_bounds_two_cols AS OF 3

> SELECT * FROM two_bounds_two_cols AS OF 6
1    6

> SELECT * FROM two_bounds_two_cols AS OF 15
1    6
2    7
3    8
4    9
5   10
6   11
7   12
8   13
9   14
10   15

> CREATE MATERIALIZED VIEW two_bounds_two_cols2 AS SELECT * FROM two_ts WHERE mz_now() >= ts1 AND mz_now() <= ts2;

> SELECT * FROM two_bounds_two_cols2 AS OF 3
1    6
2    7
3    8

> SELECT * FROM two_bounds_two_cols2 AS OF 6
1    6
2    7
3    8
4    9
5   10
6   11

> SELECT * FROM two_bounds_two_cols2 AS OF 15
10   15

> SELECT * FROM two_bounds_two_cols2 AS OF 16

> DROP VIEW one_ts CASCADE;

> DROP VIEW two_ts CASCADE;

#
# Using a TABLE source
#

> CREATE TABLE one_ts (ts mz_timestamp)

> INSERT INTO one_ts VALUES ('2000-01-01'::timestamp)

> INSERT INTO one_ts VALUES ('2199-12-31'::timestamp)

> INSERT INTO one_ts VALUES (NULL)

> CREATE MATERIALIZED VIEW one_bound1 AS SELECT to_timestamp(ts::string::numeric / 1000) FROM one_ts WHERE ts >= mz_now()

> SELECT * FROM one_bound1
      to_timestamp
"2199-12-31 00:00:00 UTC"

> INSERT INTO one_ts VALUES ('2000-01-01'::timestamp)

> INSERT INTO one_ts VALUES ('2199-12-31'::timestamp)

> INSERT INTO one_ts VALUES (NULL)

> SELECT * FROM one_bound1
      to_timestamp
"2199-12-31 00:00:00 UTC"
"2199-12-31 00:00:00 UTC"

> CREATE MATERIALIZED VIEW one_bound2 AS SELECT to_timestamp(ts::string::numeric / 1000) FROM one_ts WHERE ts <= mz_now()

> SELECT * FROM one_bound2
      to_timestamp
"2000-01-01 00:00:00 UTC"
"2000-01-01 00:00:00 UTC"

> INSERT INTO one_ts VALUES ('2000-01-01'::timestamp)

> INSERT INTO one_ts VALUES ('2199-12-31'::timestamp)

> INSERT INTO one_ts VALUES (NULL)

> SELECT * FROM one_bound2
      to_timestamp
"2000-01-01 00:00:00 UTC"
"2000-01-01 00:00:00 UTC"
"2000-01-01 00:00:00 UTC"

> CREATE TABLE first_ts (ts mz_timestamp)

> INSERT INTO first_ts VALUES ('2000-01-01'::timestamp)

> INSERT INTO first_ts VALUES ('2199-12-31'::timestamp)

> INSERT INTO first_ts VALUES (NULL)

> CREATE TABLE second_ts (ts mz_timestamp)

> INSERT INTO second_ts VALUES ('2000-01-01'::timestamp)

> INSERT INTO second_ts VALUES ('2199-12-31'::timestamp)

> INSERT INTO second_ts VALUES (NULL)

> CREATE MATERIALIZED VIEW both_ts (first_ts_to_ts, second_ts_to_ts) AS SELECT to_timestamp(first_ts.ts::string::numeric / 1000), to_timestamp(second_ts.ts::string::numeric / 1000) FROM first_ts, second_ts WHERE mz_now() BETWEEN first_ts.ts AND second_ts.ts

> SELECT * FROM both_ts
      to_timestamp
"2000-01-01 00:00:00 UTC" "2199-12-31 00:00:00 UTC"

> INSERT INTO first_ts VALUES ('2000-01-01'::timestamp)

> INSERT INTO first_ts VALUES (NULL)

> INSERT INTO second_ts VALUES ('2199-12-31'::timestamp)

> INSERT INTO second_ts VALUES (NULL)

> SELECT * FROM both_ts
"2000-01-01 00:00:00 UTC" "2199-12-31 00:00:00 UTC"
"2000-01-01 00:00:00 UTC" "2199-12-31 00:00:00 UTC"
"2000-01-01 00:00:00 UTC" "2199-12-31 00:00:00 UTC"
"2000-01-01 00:00:00 UTC" "2199-12-31 00:00:00 UTC"

> CREATE MATERIALIZED VIEW v2 (first_ts_a, first_ts_b) AS SELECT * FROM first_ts a, first_ts b WHERE mz_now() = a.ts;

> CREATE MATERIALIZED VIEW v3 (first_ts_a, first_ts_b) AS SELECT * FROM first_ts a, first_ts b WHERE mz_now() = a.ts AND mz_now() = b.ts;

#
# Various errors in the placement of mz_now()
#

!CREATE MATERIALIZED VIEW v1 AS SELECT * FROM first_ts WHERE mz_now() != ts;
contains:Unsupported binary temporal operation: NotEq

!CREATE MATERIALIZED VIEW v1 AS SELECT * FROM first_ts WHERE mz_now() + 1 = ts;
contains:no overload for mz_timestamp + integer

!CREATE MATERIALIZED VIEW v1 AS SELECT * FROM first_ts WHERE mz_now() > ts OR ts = 1;
contains:Unsupported temporal predicate. Note: `mz_now()` must be directly compared to a non-temporal expression of mz_timestamp-castable type. Expression found: ((#0 = 1) OR (mz_now() > #0))

!CREATE MATERIALIZED VIEW v1 AS SELECT * FROM first_ts WHERE ts BETWEEN mz_now() AND mz_now() + 1;
contains:no overload for mz_timestamp + integer

#
# Numeric comparisons
#

# Checks that comparisons against mz_now using values with
# fractional components fail due to lossy conversion of numeric to u64.

> CREATE OR REPLACE MATERIALIZED VIEW numeric_trunc AS
  SELECT 1
  WHERE mz_now() > 1927418240000.1;

! SELECT * FROM numeric_trunc;
contains:Evaluation error: mz_timestamp out of range
