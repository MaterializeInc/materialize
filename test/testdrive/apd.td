# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE apd_insertions (a APD(39,2));

> CREATE TABLE apd_deletions (a APD(39,2));

> CREATE VIEW apd_values AS
  SELECT a FROM apd_insertions
    EXCEPT (SELECT a FROM apd_deletions);

> INSERT INTO apd_insertions VALUES ('0.0001');
> INSERT INTO apd_insertions VALUES ('0.009');

> SELECT a::text, a = '0'::apd AS eq_zero FROM apd_values;
a    eq_zero
------------
0    true
0.01 false
