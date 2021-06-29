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
