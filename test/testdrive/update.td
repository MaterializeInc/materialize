# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test UPDATE and DELETE. This must be a testdrive test to avoid symbiosis
# in sqllogictest.

> CREATE TABLE t (i INT, f FLOAT, t TEXT);

> INSERT INTO t VALUES (1, 2, 'a'), (3, 4, 'b');

> SELECT * FROM t ORDER BY i
1 2 a
3 4 b

> UPDATE t SET i = i + 1

> SELECT * FROM t ORDER BY i
2 2 a
4 4 b

> UPDATE t SET i = i + 1 WHERE i = 2

> SELECT * FROM t ORDER BY i
3 2 a
4 4 b

# Check for assignment, implicit casts
> UPDATE t SET i = i::bigint, f = 5, t = t || 'e'

> SELECT * FROM t ORDER BY i
3 5 ae
4 5 be

! UPDATE t SET i = '4'::text
SET clause does not support casting from text to integer

> DELETE FROM t WHERE i < 4

> SELECT * FROM t ORDER BY i
4 5 be

! UPDATE t SET f = 'a'
invalid input syntax for type double precision

# Ensure that we can update after an error.
> UPDATE t SET f = 6::FLOAT

> SELECT * FROM t ORDER BY i
4 6 be

> CREATE TABLE o (i int);
> INSERT INTO o VALUES (1), (2), (3), (4);

# Support subqueries
> UPDATE t SET t = 'xy' WHERE i IN (SELECT i FROM o)

> SELECT * FROM t
4 6 xy

> CREATE VIEW v AS SELECT 1 a;

! UPDATE v SET a = 1
cannot mutate view

! UPDATE mz_tables SET a = 1
cannot mutate system table

! UPDATE t SET a = 1
unknown column a

! UPDATE t SET i = 1 WHERE a = 1
column "a" does not exist

! UPDATE t SET i = 1 WHERE i = 'a'
invalid input syntax for type integer

! UPDATE t SET i = 1, i = 1
column i set twice

> BEGIN

! UPDATE t SET i = 1
cannot be run inside a transaction block

> ROLLBACK

# Verify that UPDATE and INSERT cannot co-exist.
> BEGIN

> INSERT INTO t DEFAULT VALUES;
! UPDATE t SET i = 1
cannot be run inside a transaction block

> ROLLBACK

> BEGIN

! DELETE FROM t
cannot be run inside a transaction block

> ROLLBACK

# Verify that multiple inserts can be run in a transaction.
> BEGIN

> INSERT INTO t DEFAULT VALUES;
> INSERT INTO t DEFAULT VALUES;

> COMMIT

> SELECT * FROM t ORDER BY i
4 6 xy
<null> <null> <null>
<null> <null> <null>

# Multiple connections

> CREATE TABLE c (a int);
> INSERT INTO c VALUES (1);

$ postgres-connect name=txn url=postgres://materialize:materialize@${testdrive.materialized-addr}

# In-flight txns don't affect updates/deletes, and vice versa

$ postgres-execute connection=txn
BEGIN;
INSERT INTO c VALUES (1);

> UPDATE c SET a = 3 WHERE a < 3;

> SELECT a FROM c
3

$ postgres-execute connection=txn
INSERT INTO c VALUES (2);
COMMIT;

> SELECT a FROM c
1
2
3

> BEGIN;
> SELECT a FROM c
1
2
3

$ postgres-execute connection=txn
DELETE FROM c WHERE a < 3;

> SELECT a FROM c
1
2
3

> COMMIT;

> SELECT a FROM c;
3
