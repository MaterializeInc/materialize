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
# Test the BIT data type
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

# Insert data pre-snapshot
CREATE TABLE t1 (f1 BIT(11), f2 BIT(1));
INSERT INTO t1 VALUES (8, 0);
INSERT INTO t1 VALUES (13, 1);
INSERT INTO t1 VALUES (b'11100000100', b'1');
INSERT INTO t1 VALUES (b'0000', b'0');
INSERT INTO t1 VALUES (b'11111111111', b'0');

CREATE TABLE t2 (f1 BIT(64));
INSERT INTO t2 VALUES (0);
INSERT INTO t2 VALUES (1);
INSERT INTO t2 VALUES (2032);
INSERT INTO t2 VALUES (b'11111111');
INSERT INTO t2 VALUES (b'1111111111111111111111111111111111111111111111111111111111111111');

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);

> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE public.t2);
> COMMIT

> SELECT COUNT(*) > 0 FROM t1;
true

> SELECT COUNT(*) > 0 FROM t2;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

# MySQL does not have a proper boolean type
> SELECT pg_typeof(f1), pg_typeof(f2) FROM t1 LIMIT 1;
uint8 uint8

> SELECT * FROM t1 ORDER BY f1 DESC;
0 0
0 0
8 0
8 0
13 1
13 1
1796 1
1796 1
2047 0
2047 0

> SELECT * FROM t2 ORDER BY f1 DESC;
0
1
255
2032
18446744073709551615
