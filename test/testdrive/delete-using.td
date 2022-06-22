# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Additional test cases for DELETE ... USING, beyond those in update.td

> CREATE TABLE t1 (f1 INTEGER);

> CREATE TABLE t2 (f1 INTEGER);

> CREATE TABLE t3 (f1 INTEGER);

! DELETE FROM t1 USING t1;
contains:table name "t1" specified more than once

# Ambiguous column name
! DELETE FROM t1 USING t2 WHERE f1 = 1;
contains:column reference "f1" is ambiguous

#
# Self-join
#

> INSERT INTO t1 VALUES (1), (10), (100);

> DELETE FROM t1 AS a1 USING t1 AS a2;

> SELECT COUNT(*) from t1;
0

# Impossible join condition

> INSERT INTO t1 VALUES (1), (10), (100);

> INSERT INTO t2 VALUES (1), (10), (100);

> DELETE FROM t1 USING t2 WHERE FALSE;

> SELECT COUNT(*)  FROM t1;
3

> DELETE FROM t1;

> DELETE FROM t2;

# Null handling

> INSERT INTO t1 VALUES (1), (NULL);

> INSERT INTO t2 VALUES (1), (NULL);

> DELETE FROM t1 USING t2 WHERE t1.f1 = t2.f1;

> SELECT * FROM t1;
<null>

> DELETE FROM t1;

> DELETE FROM t2;

# Non-equality join condition

> INSERT INTO t1 VALUES (1), (2), (3);

> INSERT INTO t2 VALUES (1);

> DELETE FROM t1 USING t2 WHERE t1.f1 > t2.f1;

> SELECT * FROM t1;
1

> DELETE FROM t1;

> DELETE FROM t2;

> INSERT INTO t1 VALUES (1), (2), (3);

> INSERT INTO t2 VALUES (1);

> DELETE FROM t1 USING t2 WHERE t1.f1 IN (t2.f1, t2.f1 + 1);

> SELECT * FROM t1;
3

> DELETE FROM t1;

> DELETE FROM t2;

# Join condition OR-ed to a predicate

> INSERT INTO t1 VALUES (1), (10), (100);

> INSERT INTO t2 VALUES (1);

> DELETE FROM t1 USING t2 WHERE t1.f1 = t2.f1 OR t1.f1 = 100;

> SELECT * FROM t1;
10

> DELETE FROM t1;

> DELETE FROM t2;

# Join condition AND-ed to a predicate

> INSERT INTO t1 VALUES (1),(10),(100);

> INSERT INTO t2 VALUES (1),(10);

> DELETE FROM t1 USING t2 WHERE t1.f1 = t2.f1 AND t1.f1 = 10;

> SELECT * FROM t1;
1
100

> DELETE FROM t1;

> DELETE FROM t2;

# Join with an empty table does not delete anything

> INSERT INTO t1 VALUES (1),(10),(100);

> DELETE FROM t1 USING t2 WHERE t1.f1 = t2.f1 OR t1.f1 = 10;

> SELECT COUNT(*) FROM t1;
3

> DELETE FROM t1;

# Multi-table join

> INSERT INTO t1 VALUES (1),(10),(100);

> INSERT INTO t2 VALUES (1), (10);

> INSERT INTO t3 VALUES (1);

> DELETE FROM t1 USING t2, t3 WHERE t1.f1 = t2.f1 AND t1.f1 = t3.f1;

> SELECT * FROM t1;
10
100

> DELETE FROM t1;

> DELETE FROM t2;

> DELETE FROM t3;

# Empty join condition

> INSERT INTO t1 VALUES (1),(10),(100);

> INSERT INTO t2 VALUES (1000);

> DELETE FROM t1 USING t2;

> SELECT COUNT(*) FROM t1;
0

> DELETE FROM t1;

> DELETE FROM t2;

# JOIN ... USING ()

> INSERT INTO t1 VALUES (1),(10),(100);

> INSERT INTO t2 VALUES (100);

> INSERT INTO t3 VALUES (100);

> DELETE FROM t1 USING t2 JOIN t3 USING (f1) WHERE t1.f1 = t2.f1;

> SELECT * FROM t1;
1
10

> DELETE FROM t1;

> DELETE FROM t2;

> DELETE FROM t3;

# Outer joins

> INSERT INTO t1 VALUES (1), (2), (3), (NULL);

> INSERT INTO t2 VALUES (1), (2);

> INSERT INTO t3 VALUES (1), (3);

> DELETE FROM t1 USING t2 LEFT JOIN t3 ON (t2.f1 = t3.f1) WHERE t1.f1 = t2.f1;

> SELECT * FROM t1;
3
<null>

> DELETE FROM t1;

> DELETE FROM t2;

> DELETE FROM t3;

> INSERT INTO t1 VALUES (1), (2), (3), (NULL);

> INSERT INTO t2 VALUES (1), (2);

> INSERT INTO t3 VALUES (1), (3);

> DELETE FROM t1 USING t2 LEFT JOIN t3 ON (FALSE) WHERE t1.f1 = t2.f1;

> SELECT * FROM t1;
3
<null>

> DELETE FROM t1;

> DELETE FROM t2;

> DELETE FROM t3;

# Derived table

> INSERT INTO t1 VALUES (1), (2), (3), (4), (5);

> INSERT INTO t2 VALUES (2), (2), (3), (3), (3);

> DELETE FROM t1 USING (SELECT f1 FROM t2 GROUP BY f1) AS a2 WHERE t1.f1 = a2.f1;

> SELECT * FROM t1;
1
4
5

> DELETE FROM t1;

> DELETE FROM t2;

# Union

> INSERT INTO t1 VALUES (1), (2), (3), (4), (5);

> INSERT INTO t2 VALUES (2), (2);

> INSERT INTO t3 VALUES (3), (3), (3);

> DELETE FROM t1 USING (SELECT f1 FROM t2 UNION ALL SELECT f1 FROM t3) AS a2 WHERE t1.f1 = a2.f1;

> SELECT * FROM t1;
1
4
5

> DELETE FROM t1;

> DELETE FROM t2;

> DELETE FROM t3;
