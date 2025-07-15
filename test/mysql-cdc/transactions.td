# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s


#
# Test that transactions work properly
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES (3);
INSERT INTO t1 VALUES (4);

CREATE TABLE t2 (a INT);

CREATE TABLE t3 (a INT);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE public.t2);
> CREATE TABLE t3 FROM SOURCE mz_source (REFERENCE public.t3);

> SELECT * FROM t1;
3
4

$ mysql-execute name=mysql
SET AUTOCOMMIT = FALSE
DELETE FROM t1;
INSERT INTO t1 VALUES (5);

> SELECT * FROM t1;
3
4

$ mysql-execute name=mysql
COMMIT;

> SELECT * FROM t1;
5

$ mysql-execute name=mysql
INSERT INTO t1 VALUES (6);
COMMIT;

INSERT INTO t2 VALUES (20);
INSERT INTO t3 VALUES (300);
COMMIT;

DELETE FROM t1;
ROLLBACK;

UPDATE t1 SET a = a + 8;
DELETE FROM t3 WHERE a = 300;
INSERT INTO t2 SELECT * FROM t1;
UPDATE t1 SET a = 100 WHERE a = 14;
COMMIT;

UPDATE t2 SET a = a + 10;
INSERT INTO t3 VALUES (500), (600);
INSERT INTO t2 VALUES (44);
DELETE FROM t3 WHERE a = 500;
COMMIT;

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

$ mysql-execute name=mysql
COMMIT;

> SELECT * FROM t2;
30
23
24
44
99
