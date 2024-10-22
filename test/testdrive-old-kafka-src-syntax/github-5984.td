# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for https://github.com/MaterializeInc/database-issues/issues/1846
#
# wrong result with ORDER BY and NULLs in scalar subquery
#

> CREATE TABLE t1 (f1 INTEGER);
> CREATE INDEX i1 ON t1(f1);

> INSERT INTO t1 VALUES (1);

> CREATE TABLE t2 (f1 INTEGER);

> INSERT INTO t2 VALUES (1);

> CREATE TABLE t3 (f1 INTEGER);

> INSERT INTO t3 VALUES (NULL);
> INSERT INTO t3 VALUES (1);

> SELECT COUNT(*) FROM t1 WHERE f1 = ( SELECT t2 . f1 FROM t3 LEFT JOIN t2 ON ( t3 . f1 = t1.f1 ) ORDER BY 1 DESC LIMIT 1);
0

> SELECT f1 = ( SELECT t2 . f1 FROM t3 LEFT JOIN t2 ON ( t3 . f1 = t1.f1 ) ORDER BY 1 DESC LIMIT 1) IS NULL FROM t1;
true
