# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Multi-table insert transactions

> CREATE TABLE multi_table1 (f1 INTEGER);

> CREATE TABLE multi_table2 (f1 INTEGER);

> BEGIN

> INSERT INTO multi_table1 VALUES (1);

> INSERT INTO multi_table2 VALUES (11);

> INSERT INTO multi_table1 VALUES (2);

> INSERT INTO multi_table2 VALUES (12);

> INSERT INTO multi_table1 VALUES (3);

> INSERT INTO multi_table2 VALUES (13);

> INSERT INTO multi_table1 VALUES (4);

> INSERT INTO multi_table2 VALUES (14);

> INSERT INTO multi_table1 VALUES (5);

> INSERT INTO multi_table2 VALUES (15);

> INSERT INTO multi_table1 VALUES (6);

> INSERT INTO multi_table2 VALUES (16);

> INSERT INTO multi_table1 VALUES (7);

> INSERT INTO multi_table2 VALUES (17);

> INSERT INTO multi_table1 VALUES (8);

> INSERT INTO multi_table2 VALUES (18);

> INSERT INTO multi_table1 VALUES (9);

> INSERT INTO multi_table2 VALUES (19);

> INSERT INTO multi_table1 VALUES (10);

> INSERT INTO multi_table2 VALUES (20);

> COMMIT;
