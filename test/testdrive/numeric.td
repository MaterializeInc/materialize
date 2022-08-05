# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE numeric_insertions (a numeric(39,2));

> CREATE TABLE numeric_deletions (a numeric(39,2));

> CREATE VIEW numeric_values AS
  SELECT a FROM numeric_insertions
    EXCEPT (SELECT a FROM numeric_deletions);

> INSERT INTO numeric_insertions VALUES ('0.0001');
> INSERT INTO numeric_insertions VALUES ('0.009');

> SELECT a, a = 0 AS eq_zero FROM numeric_values;
a    eq_zero
------------
0    true
0.01 false

# Ensure values are rescaled on insert
> INSERT INTO numeric_insertions VALUES (1.2345);
> SELECT a FROM numeric_values ORDER BY a
a
----
0
0.01
1.23

# This previously panicked due to a failed conversion from numeric to i128
# statement ok
> CREATE OR REPLACE MATERIALIZED VIEW numeric_cast_ok AS
  SELECT 1
  WHERE mz_logical_timestamp() < 18446744073709551615::numeric(38,0);

> SELECT * FROM numeric_cast_ok;
1
