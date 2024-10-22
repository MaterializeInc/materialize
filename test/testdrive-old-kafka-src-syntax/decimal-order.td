# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure DECIMAL numbers are ordered appropriately in various contexts
#

#
# Simple comparisons
#

> SELECT '0.000000000000000000000000000000000000001'::decimal < '0.000000000000000000000000000000000000002'::decimal;
true

> SELECT '999999999999999999999999999999999999998'::decimal < '999999999999999999999999999999999999999'::decimal;
true

#
# Across types
#

> SELECT '0.000000000000000000000000000000000000001'::decimal < '0.000000000000000000000000000000000000002'::double;
true

# Due to loss of precision, this is unexpectedly false
> SELECT '999999999999999999999999999999999999998'::decimal < '999999999999999999999999999999999999999'::double;
false

> SELECT '0'::double = '-0'::decimal;
true

> SELECT '-0'::double = '0'::decimal;
true

#
# When used by a top-level ORDER BY
#
# Since testdrive sorts results by default, we use LIMIT + OFFSET to "probe" the correct placement of individual rows
#
#

> SELECT * FROM (VALUES ('999999999999999999999999999999999999998'::decimal), ('999999999999999999999999999999999999999')) AS t1 ORDER BY 1 LIMIT 1 OFFSET 0;
999999999999999999999999999999999999998

> SELECT * FROM (VALUES ('999999999999999999999999999999999999998'::decimal), ('999999999999999999999999999999999999999')) AS t1 ORDER BY 1 LIMIT 1 OFFSET 1;
999999999999999999999999999999999999999

> SELECT * FROM (VALUES ('0.0'::decimal),  ('0.000000000000000000000000000000000000001'::decimal)) AS t1 ORDER BY 1 LIMIT 1 OFFSET 0;
0

> SELECT * FROM (VALUES ('0.0'::decimal),  ('0.000000000000000000000000000000000000001'::decimal)) AS t1 ORDER BY 1 LIMIT 1 OFFSET 1;
0.000000000000000000000000000000000000001

> SELECT * FROM (VALUES ('0.000000000000000000000000000000000000001'::decimal),  ('0.000000000000000000000000000000000000002'::decimal)) AS t1 ORDER BY 1 LIMIT 1 OFFSET 0;
0.000000000000000000000000000000000000001

> SELECT * FROM (VALUES ('0.000000000000000000000000000000000000001'::decimal),  ('0.000000000000000000000000000000000000002'::decimal)) AS t1 ORDER BY 1 LIMIT 1 OFFSET 1;
0.000000000000000000000000000000000000002

# Negative zero is equal to positive zero, so the two are not ordered in any way against each other, the ordering of the second column applies

> SELECT * FROM (VALUES ('0'::decimal, 'row1'), ('-0', 'row2'), ('123', 'prevent optimizations')) AS t1 ORDER BY 1,2 LIMIT 1 OFFSET 0;
0 row1

> SELECT * FROM (VALUES ('0'::decimal, 'row1'), ('-0', 'row2'), ('123', 'prevent optimizations')) AS t1 ORDER BY 1,2 LIMIT 1 OFFSET 1;
0 row2

#

# Different trailing or leading zeroes should amount to the same thing, that is, the order is determined by the second column alone

> SELECT * FROM (VALUES ('0.10'::decimal, 'row1'), ('0.1', 'row2'), ('00.100', 'row3')) AS t1 ORDER BY 1,2 LIMIT 1 OFFSET 0;
0.1 row1

> SELECT * FROM (VALUES ('0.10'::decimal, 'row1'), ('0.1', 'row2'), ('00.100', 'row3')) AS t1 ORDER BY 1,2 LIMIT 1 OFFSET 1;
0.1 row2

> SELECT * FROM (VALUES ('0.10'::decimal, 'row1'), ('0.1', 'row2'), ('00.100', 'row3')) AS t1 ORDER BY 1,2 LIMIT 1 OFFSET 2;
0.1 row3

#
# When used by a TopK operator inside the dataflow
#

> SELECT MIN(column1 + 0) FROM (SELECT column1 + 0 AS column1 FROM (VALUES ('999999999999999999999999999999999999998'::decimal), ('999999999999999999999999999999999999999')) AS t1 ORDER BY 1 LIMIT 1 OFFSET 0);
999999999999999999999999999999999999998

> SELECT MIN(column1 + 0) FROM (SELECT column1 + 0 AS column1 FROM (VALUES ('999999999999999999999999999999999999998'::decimal), ('999999999999999999999999999999999999999')) AS t1 ORDER BY 1 LIMIT 1 OFFSET 1);
999999999999999999999999999999999999999


> SELECT column1 + 0, column2 FROM (
    SELECT column1 + 0 AS column1 , column2 FROM (
      SELECT '0.10'::decimal(3,2) AS column1, 'row1' AS column2 UNION ALL SELECT '0.1'::decimal(2,1), 'row2' UNION ALL SELECT '00.100'::decimal(5,3), 'row3' UNION ALL SELECT '123'::decimal, 'prevent optimizations'
    ) AS t1
    ORDER BY 1,2 LIMIT 1 OFFSET 0
  );
0.1 row1

> SELECT column1 + 0, column2 FROM (
    SELECT column1 + 0 AS column1 , column2 FROM (
      SELECT '0.10'::decimal(3,2) AS column1, 'row1' AS column2 UNION ALL SELECT '0.1'::decimal(2,1), 'row2' UNION ALL SELECT '00.100'::decimal(5,3), 'row3' UNION ALL SELECT '123'::decimal, 'prevent optimizations'
    ) AS t1
    ORDER BY 1,2 LIMIT 1 OFFSET 1
  );
0.1 row2

> SELECT column1 + 0, column2 FROM (
    SELECT column1 + 0 AS column1 , column2 FROM (
      SELECT '0.10'::decimal(3,2) AS column1, 'row1' AS column2 UNION ALL SELECT '0.1'::decimal(2,1), 'row2' UNION ALL SELECT '00.100'::decimal(5,3), 'row3' UNION ALL SELECT '123'::decimal, 'prevent optimizations'
    ) AS t1
    ORDER BY 1,2 LIMIT 1 OFFSET 2
  );
0.1 row3

#
# Infinity
#

# Positive Infinity is greater than largest decimal number

> SELECT (SELECT SUM(column1) FROM (VALUES ('999999999999999999999999999999999999999'::decimal), ('999999999999999999999999999999999999999'))) > '999999999999999999999999999999999999999'::decimal;
true

# Negative Infinity is smaller than the smallest number
> SELECT (SELECT SUM(column1) FROM (VALUES ('-999999999999999999999999999999999999999'::decimal), ('-999999999999999999999999999999999999999'))) < '-999999999999999999999999999999999999999'::decimal;
true
