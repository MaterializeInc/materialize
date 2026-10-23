# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test TRUNCATE. A truncate retracts the table's contents, unless the transaction changed the
# table before the truncate, in which case the table errors out.
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1), (2), (2);

CREATE TABLE t2 (f1 INTEGER);
ALTER TABLE t2 REPLICA IDENTITY FULL;
INSERT INTO t2 VALUES (1);

CREATE TABLE t3a (f1 INTEGER);
ALTER TABLE t3a REPLICA IDENTITY FULL;
INSERT INTO t3a VALUES (1);
CREATE TABLE t3b (f1 INTEGER);
ALTER TABLE t3b REPLICA IDENTITY FULL;
INSERT INTO t3b VALUES (2);

CREATE TABLE parent (id INTEGER PRIMARY KEY);
ALTER TABLE parent REPLICA IDENTITY FULL;
INSERT INTO parent VALUES (1), (2);
CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent (id));
ALTER TABLE child REPLICA IDENTITY FULL;
INSERT INTO child VALUES (10, 1), (20, 2);

CREATE TABLE t4 (f1 INTEGER);
ALTER TABLE t4 REPLICA IDENTITY FULL;
INSERT INTO t4 VALUES (1);

CREATE TABLE t5 (f1 INTEGER);
ALTER TABLE t5 REPLICA IDENTITY FULL;
INSERT INTO t5 VALUES (1);

CREATE TABLE t6 (f1 INTEGER, f2 TEXT, f3 NUMERIC);
ALTER TABLE t6 REPLICA IDENTITY FULL;
INSERT INTO t6 VALUES (1, 'a', 1), (2, 'b', 1e100);

CREATE TABLE t7a (f1 INTEGER);
ALTER TABLE t7a REPLICA IDENTITY FULL;
INSERT INTO t7a VALUES (1);
CREATE TABLE t7b (f1 INTEGER);
ALTER TABLE t7b REPLICA IDENTITY FULL;
INSERT INTO t7b VALUES (1);

ANALYZE;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE t2);
> CREATE TABLE t3a FROM SOURCE mz_source (REFERENCE t3a);
> CREATE TABLE t3b FROM SOURCE mz_source (REFERENCE t3b);
> CREATE TABLE parent FROM SOURCE mz_source (REFERENCE parent);
> CREATE TABLE child FROM SOURCE mz_source (REFERENCE child);
> CREATE TABLE t4a FROM SOURCE mz_source (REFERENCE t4);
> CREATE TABLE t4b FROM SOURCE mz_source (REFERENCE t4);
> CREATE TABLE t6 FROM SOURCE mz_source (REFERENCE t6) WITH (TEXT COLUMNS (f2));
> CREATE TABLE t7a FROM SOURCE mz_source (REFERENCE t7a);
> CREATE TABLE t7b FROM SOURCE mz_source (REFERENCE t7b);

> CREATE MATERIALIZED VIEW t1_count AS SELECT count(*) AS c, sum(f1) AS s FROM t1;
> CREATE DEFAULT INDEX ON t1;

> SELECT * FROM t1;
1
2
2

> SELECT * FROM t1_count;
3 5

! SELECT * FROM t6;
contains:exceeds maximum precision

# 1. insert, truncate, insert

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (3);
TRUNCATE TABLE t1;
INSERT INTO t1 VALUES (4), (5);

> SELECT * FROM t1;
4
5

# 9. Downstream objects stay live.
> SELECT * FROM t1_count;
2 9

# A second truncate retracts the post-truncate contents, durable or not.
$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE t1;
INSERT INTO t1 VALUES (6);
TRUNCATE TABLE t1;
INSERT INTO t1 VALUES (7);

> SELECT * FROM t1;
7

> SELECT * FROM t1_count;
1 7

# 2. truncate and insert in one transaction

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
TRUNCATE t2;
INSERT INTO t2 VALUES (2), (3);
COMMIT;

> SELECT * FROM t2;
2
3

# 3. multiple tables, and cascade

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE t3a, t3b;
INSERT INTO t3b VALUES (3);
TRUNCATE parent CASCADE;
INSERT INTO parent VALUES (3);
INSERT INTO child VALUES (30, 3);

> SELECT * FROM t3a;

> SELECT * FROM t3b;
3

> SELECT * FROM parent;
3

> SELECT * FROM child;
30 3

# 4. two subsources on one table

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t4 VALUES (2);
TRUNCATE t4;
INSERT INTO t4 VALUES (3);

> SELECT * FROM t4a;
3

> SELECT * FROM t4b;
3

# 5. a truncate before the subsource exists

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE t5;
INSERT INTO t5 VALUES (2);

> CREATE TABLE t5 FROM SOURCE mz_source (REFERENCE t5);

> SELECT * FROM t5;
2

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t5 VALUES (3);

> SELECT * FROM t5;
2
3

# 6. text columns and errored rows

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE t6;
INSERT INTO t6 VALUES (3, 'c', 3);

> SELECT * FROM t6;
3 c 3

# 7. a truncate after changes in the same transaction errors

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
INSERT INTO t7a VALUES (2);
TRUNCATE t7a;
COMMIT;
BEGIN;
TRUNCATE t7b;
INSERT INTO t7b VALUES (2);
TRUNCATE t7b;
COMMIT;

! SELECT * FROM t7a;
contains:table was truncated

! SELECT * FROM t7b;
contains:table was truncated

# Other subsources keep running.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (8);

> SELECT * FROM t1;
7
8

> SELECT * FROM t1_count;
2 15
