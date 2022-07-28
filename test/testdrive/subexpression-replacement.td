# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Tests for the optimization described in https://github.com/MaterializeInc/materialize/pull/6196/
# Additional tests in test/sqllogictest/transform/predicate_reduction.slt
#

$ set-sql-timeout duration=125ms

# Remove both newlines, references to internal table identifiers, and "materialize.public" strings, all with a single regexp
$ set-regex match=(\s\(u\d+\)|\n|materialize\.public\.) replacement=

> CREATE TABLE t1 (col_null INTEGER, col_not_null INTEGER NOT NULL);
> CREATE DEFAULT INDEX on t1

> INSERT INTO t1 VALUES (1, 1);

# The simplest expression there could be

> EXPLAIN SELECT * FROM t1 WHERE col_null IS NULL AND (col_null IS NULL AND col_not_null = 5);
"%0 =| Get t1| Filter (#0) IS NULL, (#1 = 5)"

> EXPLAIN SELECT * FROM t1 WHERE col_not_null = 1 AND (col_not_null = 1 AND col_null = 5);
"%0 =| Get t1| ArrangeBy (#0, #1)%1 =| Join %0| | implementation = IndexedFilter #0 = 5 AND #1 = 1| Filter (#0 = 5), (#1 = 1)"

# NULL-able expressions are dedupped
> EXPLAIN SELECT * FROM t1 WHERE col_null = 1 AND (col_null = 1 AND col_not_null = 5);
"%0 =| Get t1| ArrangeBy (#0, #1)%1 =| Join %0| | implementation = IndexedFilter #0 = 1 AND #1 = 5| Filter (#0 = 1), (#1 = 5)"

# OR/disjunction at the top level

> EXPLAIN SELECT * FROM t1 WHERE col_null IS NULL OR (col_null IS NULL AND col_not_null = 5);
"%0 =| Get t1| Filter (#0) IS NULL"

> EXPLAIN SELECT * FROM t1 WHERE col_null IS NULL OR col_null IS NULL OR (col_null IS NULL AND col_not_null = 5);
"%0 =| Get t1| Filter (#0) IS NULL"

> EXPLAIN SELECT * FROM t1 WHERE col_null IS NULL OR (col_null IS NULL AND col_not_null = 5) OR (col_null IS NULL AND col_not_null = 6);
"%0 =| Get t1| Filter (#0) IS NULL"

# OR/disjunction at the lower level

> EXPLAIN SELECT * FROM t1 WHERE col_null IS NULL AND (col_null IS NULL OR col_not_null = 5);
"%0 =| Get t1| Filter (#0) IS NULL"

# Nested OR/disjunction

> EXPLAIN SELECT * FROM t1 WHERE col_null IS NULL OR (col_null IS NULL OR col_not_null = 5);
"%0 =| Get t1| Filter ((#0) IS NULL OR (#1 = 5))"

# A more complex expression

> EXPLAIN SELECT * FROM t1 WHERE (col_not_null + 1 / col_not_null) = 5 AND ((col_not_null + 1 / col_not_null) = 5 AND col_null = 6);
"%0 =| Get t1| Filter (#0 = 6), (5 = (#1 + (1 / #1)))"

# More nesting

> EXPLAIN SELECT * FROM t1 WHERE col_not_null + col_not_null + col_not_null = 5 AND (col_not_null + col_not_null + col_not_null = 5);
"%0 =| Get t1| Filter (5 = ((#1 + #1) + #1))"

# The common expression contains an AND/conjunction itself

> EXPLAIN SELECT * FROM t1 WHERE ((col_not_null > 3) AND (col_not_null < 5)) AND ((col_not_null > 3) AND (col_not_null < 5) OR col_not_null = 10);
"%0 =| Get t1| Filter (#1 < 5), (#1 > 3)"

# The common expression contains an OR/disjunction

> EXPLAIN SELECT * FROM t1 WHERE ((col_not_null > 3) OR (col_not_null < 5)) OR ((col_not_null > 3) OR (col_not_null < 5));
"%0 =| Get t1| Filter ((#1 < 5) OR (#1 > 3))"

# Use of a deterministic function

> EXPLAIN SELECT * FROM t1 WHERE col_not_null % 2 = 5 AND (col_not_null % 2 = 5 IS NULL);
"%0 =| Constant"

# This is not optimized
> EXPLAIN SELECT * FROM t1 WHERE (col_not_null % 2) = 1 AND (((col_not_null % 2) = 1) = TRUE);
"%0 =| Get t1| Filter (1 = (#1 % 2))"

# Column used on both sides of the expression
> EXPLAIN SELECT * FROM t1 WHERE (col_not_null = col_not_null + 1) AND (col_not_null = col_not_null + 1);
"%0 =| Get t1| Filter (#1 = (#1 + 1))"

# TODO (materialize#6043):  Avoid simplifying mz_sleep.

> EXPLAIN SELECT * FROM t1
  WHERE mz_internal.mz_sleep(col_not_null) > mz_internal.mz_sleep(col_not_null)
  AND (mz_internal.mz_sleep(col_not_null) > mz_internal.mz_sleep(col_not_null) = true);
"%0 =| Get t1| Filter (mz_sleep(integer_to_double(#1)) > mz_sleep(integer_to_double(#1)))"

# IN list inside the expression

> EXPLAIN SELECT * FROM t1 WHERE col_not_null IN (2, 3) AND col_not_null IN (2, 3);
"%0 =| Get t1| Filter ((#1 = 2) OR (#1 = 3))"

# Partial matches are not optimized

> EXPLAIN SELECT * FROM t1 WHERE col_not_null IN (2, 3) AND col_not_null IN (3, 4);
"%0 =| Get t1| Filter ((#1 = 3) OR ((#1 = 2) AND (#1 = 4)))"

> EXPLAIN SELECT * FROM t1 WHERE col_not_null IN (2, 3) AND col_not_null IN (4, 5);
"%0 =| Get t1| Filter ((#1 = 2) OR (#1 = 3)), ((#1 = 4) OR (#1 = 5))"

# Expression inside an IN list

# Optimized in AND/conjunctions

> EXPLAIN SELECT * FROM t1 WHERE col_not_null = 1 AND TRUE IN (col_not_null = 1, col_not_null = 2)
"%0 =| Get t1| Filter (#1 = 1)"

# Not optimized in OR/disjunctions

> EXPLAIN SELECT * FROM t1 WHERE col_not_null = 1 OR TRUE IN (col_not_null = 1, col_not_null = 2)
"%0 =| Get t1| Map (#1 = 1)| Filter (#2 OR (#2 = true) OR (true = (#1 = 2)))| Project (#0, #1)"
