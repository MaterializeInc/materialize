# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


#
# Tests for Move unrolling of peek multiplicities to coordinator. #10673
#
# The important thing here is to test ORDER BY + LIMIT in the presence of
# records where the diff will be > 1
#

> CREATE TABLE t1 (f1 INTEGER);

> INSERT INTO t1 VALUES (1), (1), (1);

> INSERT INTO t1 VALUES (2), (2);

> INSERT INTO t1 VALUES (NULL), (NULL);


#
# LIMIT only
#

> SELECT * FROM t1 LIMIT 0;

> SELECT * FROM t1 LIMIT 1;
<null>

> SELECT * FROM t1 LIMIT 2;
<null>
<null>

> SELECT * FROM t1 LIMIT 3;
<null>
<null>
1

> SELECT * FROM t1 LIMIT 4;
<null>
<null>
1
1

> SELECT * FROM t1 LIMIT 999999;
<null>
<null>
1
1
1
2
2

#
# ORDER BY + LIMIT
#

> SELECT * FROM t1 ORDER BY f1 LIMIT 0;

> SELECT * FROM t1 ORDER BY f1 LIMIT 1;
1

> SELECT * FROM t1 ORDER BY f1 LIMIT 2;
1
1

> SELECT * FROM t1 ORDER BY f1 LIMIT 3;
1
1
1

> SELECT * FROM t1 ORDER BY f1 LIMIT 4;
1
1
1
2

> SELECT * FROM t1 ORDER BY f1 LIMIT 5;
1
1
1
2
2

> SELECT * FROM t1 ORDER BY f1 LIMIT 6;
1
1
1
2
2
<null>

> SELECT * FROM t1 ORDER BY f1 LIMIT 65535;
1
1
1
2
2
<null>
<null>

#
# LIMIT + OFFSET
#

> SELECT * FROM t1 LIMIT 0 OFFSET 0;

> SELECT * FROM t1 LIMIT 0 OFFSET 1;

> SELECT * FROM t1 LIMIT 1 OFFSET 1;
<null>

> SELECT * FROM t1 LIMIT 1 OFFSET 2;
1

> SELECT * FROM t1 LIMIT 2 OFFSET 1;
<null>
1

> SELECT * FROM t1 LIMIT 3 OFFSET 1;
<null>
1
1

> SELECT * FROM t1 LIMIT 5 OFFSET 1;
<null>
1
1
1
2


#
# ORDER BY + LIMIT + OFFSET
#

> SELECT * FROM t1 ORDER BY f1 LIMIT 0 OFFSET 0;

> SELECT * FROM t1 ORDER BY f1 LIMIT 0 OFFSET 1;

> SELECT * FROM t1 ORDER BY f1 LIMIT 1 OFFSET 1;
1

> SELECT * FROM t1 ORDER BY f1 LIMIT 1 OFFSET 2;
1

> SELECT * FROM t1 ORDER BY f1 LIMIT 3 OFFSET 2;
1
2
2

> SELECT * FROM t1 ORDER BY f1 LIMIT 4 OFFSET 2;
1
2
2
<null>

> SELECT * FROM t1 ORDER BY f1 LIMIT 3 OFFSET 3;
2
2
<null>

#
# OFFSET alone
#

> SELECT * FROM t1 OFFSET 0;
<null>
<null>
1
1
1
2
2


> SELECT * FROM t1 OFFSET 4;
1
2
2

> SELECT * FROM t1 OFFSET 999999;


#
# And some larger offsets
#

> CREATE TABLE ten (f1 INTEGER);

> INSERT INTO ten VALUES (0),(1), (2),(3),(4),(5),(6),(7),(8),(9);

> CREATE VIEW v1 AS SELECT (a2.f1 * 10) + (a3.f1 * 100) + (a4.f1 * 1000) + (a5.f1 * 10000) + (a6.f1 * 100000) AS f1 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> SELECT * FROM v1 ORDER BY f1 OFFSET 999980;
999980
999980
999980
999980
999980
999980
999980
999980
999980
999980
999990
999990
999990
999990
999990
999990
999990
999990
999990
999990

> SELECT * FROM v1 ORDER BY f1 LIMIT 3 OFFSET 999990;
999990
999990
999990

> CREATE VIEW v2 AS SELECT a1.f1 + (a2.f1 * 10) + (a3.f1 * 100) + (a4.f1 * 1000) + (a5.f1 * 10000) + (a6.f1 * 100000) < 999999 AS f1 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> SELECT * FROM v2 ORDER BY f1 DESC LIMIT 33 OFFSET 999990;
true
true
true
true
true
true
true
true
true
false
