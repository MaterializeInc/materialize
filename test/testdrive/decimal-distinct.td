# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that decimal numbers that are the same are considered identical
# and those that are truly different are considered distinct
#

#
# Truly distinct numbers
#

> CREATE TABLE decimal_distinct (f1 DECIMAL);

> INSERT INTO decimal_distinct VALUES ('-999999999999999999999999999999999999999');

> INSERT INTO decimal_distinct VALUES ('-1.00000000000000000000000000000000000001');

> INSERT INTO decimal_distinct VALUES ('-0.999999999999999999999999999999999999999');

> INSERT INTO decimal_distinct VALUES ('-0.000000000000000000000000000000000000001');

> INSERT INTO decimal_distinct VALUES ('0.000000000000000000000000000000000000001');

> INSERT INTO decimal_distinct VALUES ('0.000000000000000000000000000000000000002');

> INSERT INTO decimal_distinct VALUES ('0.999999999999999999999999999999999999998');

> INSERT INTO decimal_distinct VALUES ('0.999999999999999999999999999999999999999');

> INSERT INTO decimal_distinct VALUES ('1.00000000000000000000000000000000000001');

> INSERT INTO decimal_distinct VALUES ('1.00000000000000000000000000000000000002');

> INSERT INTO decimal_distinct VALUES ('999999999999999999999999999999999999999');

> INSERT INTO decimal_distinct VALUES ('999999999999999999999999999999999999998');

> SELECT DISTINCT f1 FROM decimal_distinct;
-999999999999999999999999999999999999999
-1.00000000000000000000000000000000000001
-0.999999999999999999999999999999999999999
-0.000000000000000000000000000000000000001
0.000000000000000000000000000000000000001
0.000000000000000000000000000000000000002
0.999999999999999999999999999999999999998
0.999999999999999999999999999999999999999
1.00000000000000000000000000000000000001
1.00000000000000000000000000000000000002
999999999999999999999999999999999999998
999999999999999999999999999999999999999

> SELECT f1 FROM decimal_distinct UNION DISTINCT SELECT * FROM decimal_distinct;
-999999999999999999999999999999999999999
-1.00000000000000000000000000000000000001
-0.999999999999999999999999999999999999999
-0.000000000000000000000000000000000000001
0.000000000000000000000000000000000000001
0.000000000000000000000000000000000000002
0.999999999999999999999999999999999999998
0.999999999999999999999999999999999999999
1.00000000000000000000000000000000000001
1.00000000000000000000000000000000000002
999999999999999999999999999999999999998
999999999999999999999999999999999999999

> SELECT COUNT(DISTINCT f1), COUNT(*) FROM decimal_distinct;
12 12

> SELECT COUNT(DISTINCT f1::DECIMAL(5,2)) FROM decimal_distinct WHERE f1 BETWEEN -1 AND 1;
3

#
# Fundamentally "identical" numbers as far as the DECIMAL type is concerned
#

> CREATE TABLE decimal_same (f1 DECIMAL);

> INSERT INTO decimal_same VALUES (0),(-0);

# The numbers below differ only in the part that is outside of the precision of the DECIMAL type

> INSERT INTO decimal_same VALUES ('999999999999999999999999999999999999999.1');

> INSERT INTO decimal_same VALUES ('999999999999999999999999999999999999999.2');

# The number of leading or trailing zeroes should not matter

> INSERT INTO decimal_same VALUES ('1.010');

> INSERT INTO decimal_same VALUES ('1.01000');

> INSERT INTO decimal_same VALUES ('001.01');

> SELECT DISTINCT f1 FROM decimal_same;
0
1.01
999999999999999999999999999999999999999

> SELECT COUNT(DISTINCT f1) FROM decimal_same;
3

> SELECT f1 FROM decimal_same GROUP BY 1;
0
1.01
999999999999999999999999999999999999999

# The remainder of the file is adapted from the logic test suite in CockroachDB.

#
# Distinct numeric values
#

> CREATE TABLE xyz (
  x numeric,
  y numeric,
  z numeric)

> INSERT INTO xyz VALUES
  (1.0, 1.0, NULL),
  (1.00, 1.00, 2.0),
  (1.000, 1.000, 2.00),
  (1.0000, 2.00, 1.0),
  (2.0, 2.00, 3.0),
  (4.0, 5.00, 6.0),
  (4.00, 1.00, 6.00)

##################
# Simple queries #
##################

# 3/3 columns

> SELECT DISTINCT ON (x, y, z) x, y, z FROM xyz
1 1 <null>
1 1 2
1 2 1
2 2 3
4 5 6
4 1 6

> SELECT DISTINCT ON (y, x, z) x FROM xyz
1
1
1
2
4
4

> SELECT DISTINCT ON (z, y, x) z FROM xyz
<null>
2
1
3
6
6

# 2/3 columns

> SELECT DISTINCT ON (x, y) y, x FROM xyz
1 1
2 1
2 2
5 4
1 4

> SELECT DISTINCT ON (y, x) x FROM xyz
1
1
2
4
4

> SELECT DISTINCT ON (x, y) y FROM xyz
1
2
2
5
1

# 1/3 columns

> SELECT DISTINCT ON (y) y FROM xyz
1
2
5

#################
# With ORDER BY #
#################

> SELECT DISTINCT ON (x) x FROM xyz ORDER BY x DESC
4
2
1

# We add a filter to eliminate one of the rows that may be flakily returned
# depending on parallel execution of DISTINCT ON.
# Note: Result differs from Cockroach but matches Postgres.
> SELECT DISTINCT ON (x, z) y, z, x FROM xyz WHERE (x,y,z) != (4, 1, 6) ORDER BY z
2 1      1
1 2      1
2 3      2
5 6      4
1 <null> 1

# Note: Result differs from Cockroach but matches Postgres.
> SELECT DISTINCT ON (x) y, z, x FROM xyz ORDER BY x ASC, z DESC, y DESC
1 <null> 1
2 3      2
5 6      4

#####################
# With aggregations #
#####################

> SELECT DISTINCT ON (max(x)) min(y) FROM xyz
1

> SELECT DISTINCT ON (min(x)) max(y) FROM xyz
5

#################
# With GROUP BY #
#################

> SELECT DISTINCT ON(y) min(x) FROM xyz GROUP BY y
1
1
4

> SELECT DISTINCT ON(min(x)) min(x) FROM xyz GROUP BY y HAVING min(x) = 1
1

###########################
# With ordinal references #
###########################

> SELECT DISTINCT ON (1) x FROM xyz
1
2
4

> SELECT DISTINCT ON (1,2,3) x, y, z FROM xyz
1  1  <null>
1  1  2
1  2  1
2  2  3
4  5  6
4  1  6

#########################
# With alias references #
#########################

# This should prioritize alias (use 'x' as the key).
# This would be non-deterministic if we don't select y (actually x) from the
# subquery.
> SELECT y FROM (SELECT DISTINCT ON(y) x AS y, y AS x FROM xyz)
1
2
4

# Ignores the alias.
> SELECT DISTINCT ON(x) x AS y FROM xyz
1
2
4

##################################
# With nested parentheses/tuples #
##################################

> SELECT DISTINCT ON(((x)), (x, y)) x, y FROM xyz
1  1
1  2
2  2
4  5
4  1

#
# Distinct numeric list values
#

# Note that in these tests, we use string ordering because:
# - Testdrive doesn't support outputting lists directly, so we need to encode
#   values as text.
# - We must use subqueries to output text versions of the inner queries' values;
#   placing the text cast in the inner query changes the results.
# - MZ will not preserve subquery's ordering.
#
# However, ordering on these values does, in fact, work as expected.

> CREATE TABLE xyz_list (
  x numeric list,
  y numeric list,
  z numeric list)

> INSERT INTO xyz_list VALUES
  (LIST[1.0], LIST[1.00], NULL),
  (LIST[1.00], LIST[1.000], LIST[2.000]),
  (LIST[1.000], LIST[1.0000], LIST[2.0000]),
  (LIST[1.0000], LIST[2.0], LIST[1.0000]),
  (LIST[2.0], LIST[2.00], LIST[3.0]),
  (LIST[4.0], LIST[5.0], LIST[6.0]),
  (LIST[4.00], LIST[1.00000], LIST[6.00])

##################
# Simple queries #
##################

# 3/3 columns

> SELECT x::text, y::text, z::text FROM (
    SELECT DISTINCT ON (x, y, z) x, y, z FROM xyz_list
  ) i ORDER BY x, y, z
{1.0000}  {2.0}     {1.0000}
{1.00}    {1.000}   {2.000}
{1.0}     {1.00}    <null>
{2.0}     {2.00}    {3.0}
{4.00}    {1.00000} {6.00}
{4.0}     {5.0}     {6.0}

> SELECT x::text FROM (
    SELECT DISTINCT ON (y, x, z) x FROM xyz_list
  ) ORDER BY x
{1.0000}
{1.00}
{1.0}
{2.0}
{4.00}
{4.0}

> SELECT z::text FROM (
    SELECT DISTINCT ON (z, y, x) z FROM xyz_list
  ) ORDER BY z
{1.0000}
{2.000}
{3.0}
{6.00}
{6.0}
<null>

# 2/3 columns

> SELECT y::text, x::text FROM (
    SELECT DISTINCT ON (x, y) y, x FROM xyz_list
  ) ORDER BY y, x
{1.00000} {4.00}
{1.00}    {1.0}
{2.00}    {2.0}
{2.0}     {1.0000}
{5.0}     {4.0}

> SELECT x::text FROM (
    SELECT DISTINCT ON (y, x) x FROM xyz_list ORDER BY x
  ) ORDER BY x
{1.0000}
{1.0}
{2.0}
{4.00}
{4.0}

> SELECT y::text FROM (
    SELECT DISTINCT ON (x, y) y FROM xyz_list
  ) ORDER BY y
{1.00000}
{1.00}
{2.00}
{2.0}
{5.0}

##################################
# With nested parentheses/tuples #
##################################

> SELECT x::text, y::text FROM (
    SELECT DISTINCT ON(((x)), (x, y)) x, y FROM xyz_list
  ) ORDER BY x, y
{1.0000} {2.0}
{1.0}    {1.00}
{2.0}    {2.00}
{4.00}   {1.00000}
{4.0}    {5.0}

# 1/3 columns

> SELECT y::text FROM (
    SELECT DISTINCT ON (y) y FROM xyz_list
  ) ORDER BY y
{1.00}
{2.00}
{5.0}

# 2/3 columns

> SELECT y::text, x::text FROM (
    SELECT DISTINCT ON (x, y) y, x FROM xyz_list
  ) ORDER BY y, x
{1.00000} {4.00}
{1.00}    {1.0}
{2.00}    {2.0}
{2.0}     {1.0000}
{5.0}     {4.0}

> SELECT x::text FROM (
    SELECT DISTINCT ON (y, x) x FROM xyz_list
  ) ORDER BY x
{1.0000}
{1.0}
{2.0}
{4.00}
{4.0}

> SELECT y::text FROM (
    SELECT DISTINCT ON (x, y) y FROM xyz_list
  ) ORDER BY y
{1.00000}
{1.00}
{2.00}
{2.0}
{5.0}

#
# Distinct numeric list list values
#

> CREATE TABLE x_list_list (
    x numeric list list
  )

> INSERT INTO x_list_list VALUES
  (NULL),
  (LIST[LIST[2.000], LIST[3.000]]),
  (LIST[LIST[2.0]]),
  (LIST[LIST[2.00]]),
  (LIST[LIST[1.000]]),
  (LIST[LIST[3.000]]),
  (LIST[LIST[6.0], LIST[7.0]]),
  (LIST[LIST[6.00], LIST[7.00]]),
  (LIST[LIST[6.000]])

> SELECT x::text, count FROM (
    SELECT x, count(*) FROM x_list_list GROUP BY x
  )
<null>      1
{{1}}       1
{{2}}       2
{{3}}       1
{{6}}       1
{{2},{3}}   1
{{6},{7}}   2
