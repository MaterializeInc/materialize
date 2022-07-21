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
contains:SET clause does not support casting from text to integer

> DELETE FROM t WHERE i < 4

> SELECT * FROM t ORDER BY i
4 5 be

! UPDATE t SET f = 'a'
contains:invalid input syntax for type double precision

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

> CREATE MATERIALIZED VIEW v (a) AS SELECT 4;

> UPDATE t SET i = i + 1 WHERE i IN (SELECT a FROM v);

> SELECT * FROM t
5 6 xy

! UPDATE v SET a = 1
contains:cannot mutate materialized view

! UPDATE mz_tables SET a = 1
contains:cannot mutate system table

! UPDATE t SET a = 1
contains:unknown column a

! UPDATE t SET i = 1 WHERE a = 1
contains:column "a" does not exist

! UPDATE t SET i = 1 WHERE i = 'a'
contains:invalid input syntax for type integer

! UPDATE t SET i = 1, i = 1
contains:column i set twice

> BEGIN

! UPDATE t SET i = 1
contains:cannot be run inside a transaction block

> ROLLBACK

# Verify that UPDATE and INSERT cannot co-exist.
> BEGIN

> INSERT INTO t DEFAULT VALUES;
! UPDATE t SET i = 1
contains:cannot be run inside a transaction block

> ROLLBACK

> BEGIN

! DELETE FROM t
contains:cannot be run inside a transaction block

> ROLLBACK

# Update subqueries cannot reference sources

$ set schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "float"},
      {"name": "c", "type": "string"}
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 2, "c": "a"}

> CREATE SOURCE source_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED VIEW source_data_mat_view AS
  SELECT * FROM source_data;

! UPDATE t SET i = i + 1 WHERE i IN (SELECT a FROM source_data_mat_view)
contains:invalid selection

# Verify that multiple inserts can be run in a transaction.
> BEGIN

> INSERT INTO t DEFAULT VALUES;
> INSERT INTO t DEFAULT VALUES;

> COMMIT

> SELECT * FROM t ORDER BY i
5 6 xy
<null> <null> <null>
<null> <null> <null>

# Multiple connections

> CREATE TABLE c (a int);
> INSERT INTO c VALUES (1);

$ postgres-connect name=txn url=postgres://materialize:materialize@${testdrive.materialize-sql-addr}

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

# DELETE USING

> CREATE TABLE t1 (a int, b int);
> CREATE TABLE t2 (a int, b int);
> CREATE TABLE t3 (x int, y int);

# This test should exercise at least one case where a cross-join of t1, t3
# produce more rows than t3 wants to DELETE.

> INSERT INTO t1 VALUES
    (5, 10), (5, 15), (15, 20), (100, 100), (200, 1000);
> INSERT INTO t2 VALUES
    (200, 2000), (200, 2000);
> INSERT INTO t3 VALUES
    (5, 20), (6, 7), (7, 8), (500, 100);

> DELETE FROM t3 USING t1 table1 WHERE t3.x = table1.a;
> SELECT * FROM t3 ORDER BY x;
6   7
7   8
500 100

> DELETE FROM t3 USING t1 JOIN t2 USING (a) WHERE t3.x > t1.a;
> SELECT * FROM t3 ORDER BY x;
6 7
7 8

> DELETE FROM t3 USING t3 t3_other WHERE t3.x = t3_other.y;
> SELECT * FROM t3 ORDER BY x;
6 7

# Ensure that `USING` empty tables does not delete any rows
> DELETE FROM t1;
> SELECT * FROM t1;

> DELETE FROM t3 USING t1;
> SELECT * FROM t3 ORDER BY x;
6 7

> DELETE FROM t3 USING t1, t2;
> SELECT * FROM t3 ORDER BY x;
6 7

> DELETE FROM t3 USING t2;
> SELECT * FROM t3 ORDER BY x;

# Lateral subqueries cannot access `FROM` table
! DELETE FROM t3 USING lateral (SELECT t3.x FROM t1) t4 WHERE true;
contains:column "t3.x" does not exist

# ...unless it's explicitly brought into scope
> DELETE FROM t3 USING lateral (SELECT t3.x FROM t3) t4 WHERE true;

# Test different arities
> CREATE TABLE t4 (a int, b int, c int);
> CREATE TABLE t5 (a int);
> CREATE TABLE t6 (a int, b int);

> INSERT INTO t4 VALUES
    (6, 0, 0), (7, 0, 0), (10, 0, 0), (11, 0, 0);
> INSERT INTO t5 VALUES
    (8), (9), (10), (12);
> INSERT INTO t6 VALUES
    (0, 6), (0, 7), (0, 8), (0, 9);

> DELETE FROM t6 USING t4 WHERE t4.a + t4.b + t4.c = t6.a + t6.b;
> DELETE FROM t4 USING t6 WHERE t4.a + t4.b + t4.c = t6.a + t6.b;
> DELETE FROM t6 USING t5 WHERE t5.a = t6.a + t6.b;
> DELETE FROM t5 USING t6 WHERE t5.a = t6.a + t6.b;
> DELETE FROM t4 USING t5 WHERE t4.a + t4.b + t4.c = t5.a;
> DELETE FROM t5 USING t4 WHERE t4.a + t4.b + t4.c + 1 = t5.a;

> SELECT * FROM t4 ORDER BY a;
6  0 0
7  0 0
11 0 0

> SELECT * FROM t5 ORDER BY a;
9
10

> SELECT * FROM t6 ORDER BY a;
