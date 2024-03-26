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

$ postgres-execute connection=postgres://postgres:postgres@postgres

$ postgres-connect name=conn1 url=postgres://postgres:postgres@postgres
$ postgres-connect name=conn2 url=postgres://postgres:postgres@postgres
$ postgres-connect name=conn3 url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn1
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (a INT);
CREATE TABLE t2 (a INT);
CREATE TABLE t3 (a INT);

ALTER TABLE t1 REPLICA IDENTITY FULL;
ALTER TABLE t2 REPLICA IDENTITY FULL;
ALTER TABLE t3 REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT count(*) FROM t1;
0

> SELECT count(*) FROM t2;
0

> SELECT count(*) FROM t3;
0

$ postgres-execute connection=conn1
BEGIN;
INSERT INTO t1 VALUES (1000);
INSERT INTO t2 VALUES (1000);
INSERT INTO t3 VALUES (1000);

$ postgres-execute connection=conn2
BEGIN;
INSERT INTO t1 VALUES (2000);
INSERT INTO t2 VALUES (2000);
INSERT INTO t3 VALUES (2000);

$ postgres-execute connection=conn3
BEGIN;
INSERT INTO t1 VALUES (3000);
INSERT INTO t2 VALUES (3000);
INSERT INTO t3 VALUES (3000);

$ postgres-execute connection=conn1
COMMIT;
BEGIN;

$ postgres-execute connection=conn3
COMMIT;
BEGIN;

> SELECT * FROM t1;
1000
3000

> SELECT * FROM t2;
1000
3000

> SELECT * FROM t3;
1000
3000

$ postgres-execute connection=conn2
COMMIT;
BEGIN;

> SELECT * FROM t1;
1000
2000
3000

# delete and insert statements cannot be done in multiple transactions on the same table even with fine-grained where condition

$ postgres-execute connection=conn1
INSERT INTO t1 VALUES (1001);
INSERT INTO t2 VALUES (1001);
INSERT INTO t3 VALUES (1001);

$ postgres-execute connection=conn2
INSERT INTO t1 VALUES (2001);
INSERT INTO t2 VALUES (2001);
INSERT INTO t3 VALUES (2001);

$ postgres-execute connection=conn3
INSERT INTO t1 VALUES (3001);
INSERT INTO t2 VALUES (3001);
INSERT INTO t3 VALUES (3001);

$ postgres-execute connection=conn1
COMMIT;
BEGIN;

$ postgres-execute connection=conn3
COMMIT;
BEGIN;

> SELECT * FROM t1;
1000
1001
2000
3000
3001

> SELECT * FROM t2;
1000
1001
2000
3000
3001

> SELECT * FROM t3;
1000
1001
2000
3000
3001

$ postgres-execute connection=conn2
COMMIT;
BEGIN;
INSERT INTO t1 VALUES (2002);

$ postgres-execute connection=conn1
INSERT INTO t1 VALUES (1002);
DELETE FROM t2 WHERE a = 2000;

$ postgres-execute connection=conn2
DELETE FROM t3 WHERE a = 2000;

$ postgres-execute connection=conn1
COMMIT;
BEGIN;

> SELECT * FROM t1;
1000
1001
1002
2000
2001
3000
3001

> SELECT * FROM t2;
1000
1001
2001
3000
3001

> SELECT * FROM t3;
1000
1001
2000
2001
3000
3001

$ postgres-execute connection=conn2
COMMIT;
BEGIN;

> SELECT * FROM t1;
1000
1001
1002
2000
2001
2002
3000
3001

> SELECT * FROM t2;
1000
1001
2001
3000
3001

> SELECT * FROM t3;
1000
1001
2001
3000
3001
