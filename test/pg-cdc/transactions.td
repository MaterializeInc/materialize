# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that transactions work properly
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-connect name=conn url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES (3);
INSERT INTO t1 VALUES (4);

CREATE TABLE t2 (a INT);

CREATE TABLE t3 (a INT);

ALTER TABLE t1 REPLICA IDENTITY FULL;
ALTER TABLE t2 REPLICA IDENTITY FULL;
ALTER TABLE t3 REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE t2);
> CREATE TABLE t3 FROM SOURCE mz_source (REFERENCE t3);

> SELECT * FROM t1;
3
4

$ postgres-execute connection=conn
BEGIN;
DELETE FROM t1;
INSERT INTO t1 VALUES (5);

> SELECT * FROM t1;
3
4

$ postgres-execute connection=conn
COMMIT;
BEGIN;

> SELECT * FROM t1;
5

$ postgres-execute connection=conn
INSERT INTO t1 VALUES (6);
COMMIT;
BEGIN;

INSERT INTO t2 VALUES (20);
INSERT INTO t3 VALUES (300);
COMMIT;
BEGIN;

DELETE FROM t1;
ROLLBACK;
BEGIN;

UPDATE t1 SET a = a + 8;
DELETE FROM t3 WHERE a = 300;
INSERT INTO t2 SELECT * FROM t1;
UPDATE t1 SET a = 100 WHERE a = 14;
COMMIT;
BEGIN;

UPDATE t2 SET a = a + 10;
INSERT INTO t3 VALUES (500), (600);
INSERT INTO t2 VALUES (44);
DELETE FROM t3 WHERE a = 500;
COMMIT;
BEGIN;

INSERT INTO t2 VALUES (99);

> SELECT * FROM t1;
13
100

> SELECT * FROM t2;
30
23
24
44

> SELECT * FROM t3;
600

$ postgres-execute connection=conn
COMMIT;
BEGIN;

> SELECT * FROM t2;
30
23
24
44
99
