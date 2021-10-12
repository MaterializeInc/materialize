# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Tests for the various DML statements that are currently supported
#

# Multi-row INSERT

> CREATE TABLE multi_row_insert (f1 INTEGER);

> INSERT INTO multi_row_insert VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

# Multi-INSERT transaction

> CREATE TABLE multi_insert_transaction (f1 INTEGER);

> BEGIN

> INSERT INTO multi_insert_transaction VALUES (1);

> INSERT INTO multi_insert_transaction VALUES (2);

> INSERT INTO multi_insert_transaction VALUES (3);

> INSERT INTO multi_insert_transaction VALUES (4);

> INSERT INTO multi_insert_transaction VALUES (5);

> INSERT INTO multi_insert_transaction VALUES (6);

> INSERT INTO multi_insert_transaction VALUES (7);

> INSERT INTO multi_insert_transaction VALUES (8);

> INSERT INTO multi_insert_transaction VALUES (9);

> INSERT INTO multi_insert_transaction VALUES (10);

> COMMIT

# ROLLBACK

> CREATE TABLE insert_rollback (f1 INTEGER);

> BEGIN

> INSERT INTO insert_rollback VALUES (1);

> INSERT INTO insert_rollback VALUES (2);

> INSERT INTO insert_rollback VALUES (3);

> INSERT INTO insert_rollback VALUES (4);

> INSERT INTO insert_rollback VALUES (5);

> INSERT INTO insert_rollback VALUES (6);

> INSERT INTO insert_rollback VALUES (7);

> INSERT INTO insert_rollback VALUES (8);

> INSERT INTO insert_rollback VALUES (9);

> INSERT INTO insert_rollback VALUES (10);

> ROLLBACK

# INSERT ... SELECT

> CREATE TABLE ten (f1 INTEGER);

> INSERT INTO ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

> CREATE TABLE insert_select (f1 INTEGER);

> INSERT INTO insert_select SELECT * FROM ten;

# DELETE

> CREATE TABLE delete_table (f1 INTEGER);

> INSERT INTO delete_table VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

> INSERT INTO delete_table VALUES (11), (12), (13), (14), (15), (16), (17), (18), (19), (20);

> DELETE FROM delete_table WHERE f1 IN (1, 2, 3, 4, 5, 16, 17, 18, 19, 20);

# UPDATE

> CREATE TABLE update_table (f1 INTEGER);

> INSERT INTO update_table VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

> UPDATE update_table SET f1 = f1 + 10;
