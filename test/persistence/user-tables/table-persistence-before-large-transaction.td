# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Tests for wide and long transactions
#

> CREATE TABLE wide_transaction (f1 TEXT);

> INSERT INTO wide_transaction VALUES (REPEAT('x', 100 * 1024 * 1024));

> CREATE TABLE long_ten (f1 INTEGER);

> INSERT INTO long_ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

> CREATE TABLE long_transaction (f1 INTEGER);

# Insert 1M unique rows
> INSERT INTO long_transaction
  SELECT (a1.f1 * 1) +
         (a2.f1 * 10) +
         (a3.f1 * 100) +
         (a4.f1 * 1000) +
         (a5.f1 * 10000) +
         (a6.f1 * 100000)
  FROM long_ten AS a1, long_ten AS a2, long_ten AS a3, long_ten AS a4, long_ten AS a5, long_ten AS a6;
