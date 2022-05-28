# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Tests with interleaved transactions - the INSERTs made by both transactions
# must be present in full post-restart regardless of the order of execution.
#
# Mz currently does not support other DML statements in a transactional context.
#

# Second transaction completes before first

> CREATE TABLE second_completes_first (f1 INTEGER);

$ postgres-connect name=conn1 url=postgres://materialize:materialize@${testdrive.materialized-sql-addr}
$ postgres-connect name=conn2 url=postgres://materialize:materialize@${testdrive.materialized-sql-addr}

$ postgres-execute connection=conn1
BEGIN;
INSERT INTO second_completes_first VALUES (1);
INSERT INTO second_completes_first VALUES (2);
INSERT INTO second_completes_first VALUES (3);

$ postgres-execute connection=conn2
BEGIN;
INSERT INTO second_completes_first VALUES (4);
INSERT INTO second_completes_first VALUES (5);
INSERT INTO second_completes_first VALUES (6);
COMMIT;

$ postgres-execute connection=conn1
INSERT INTO second_completes_first VALUES (7);
INSERT INTO second_completes_first VALUES (8);
INSERT INTO second_completes_first VALUES (9);
INSERT INTO second_completes_first VALUES (10)
COMMIT;

# Second transaction completes after first

> CREATE TABLE second_completes_last (f1 INTEGER);

$ postgres-execute connection=conn1
BEGIN;
INSERT INTO second_completes_last VALUES (1);
INSERT INTO second_completes_last VALUES (2);
INSERT INTO second_completes_last VALUES (3);

$ postgres-execute connection=conn2
INSERT INTO second_completes_last VALUES (4);
INSERT INTO second_completes_last VALUES (5);
INSERT INTO second_completes_last VALUES (6);

$ postgres-execute connection=conn1
INSERT INTO second_completes_last VALUES (7);
INSERT INTO second_completes_last VALUES (8);
INSERT INTO second_completes_last VALUES (9);
COMMIT;

$ postgres-execute connection=conn2
INSERT INTO second_completes_last VALUES (10);
COMMIT;


# Both transaction operate on the same values

> CREATE TABLE same_values (f1 INTEGER);

$ postgres-execute connection=conn1
BEGIN;
INSERT INTO same_values VALUES (1);

$ postgres-execute connection=conn2
BEGIN;
INSERT INTO same_values VALUES (5);

$ postgres-execute connection=conn1
INSERT INTO same_values VALUES (2);

$ postgres-execute connection=conn2
INSERT INTO same_values VALUES (4);

$ postgres-execute connection=conn1
INSERT INTO same_values VALUES (3);

$ postgres-execute connection=conn2
INSERT INTO same_values VALUES (3);

$ postgres-execute connection=conn1
INSERT INTO same_values VALUES (4);

$ postgres-execute connection=conn2
INSERT INTO same_values VALUES (2);

$ postgres-execute connection=conn1
INSERT INTO same_values VALUES (5);
COMMIT;

$ postgres-execute connection=conn2
INSERT INTO same_values VALUES (1);
COMMIT;
