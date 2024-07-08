# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test mysql DECIMAL / NUMERIC type
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'


> CREATE CONNECTION mysqc TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# Insert data pre-snapshot
# We insert values with both default precision (0) and a value of 6 where allowed
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 DECIMAL(20, 0), f2 DECIMAL(20, 20), f3 DECIMAL(20,10), f4 NUMERIC(20,5));

INSERT INTO t1 VALUES ('99999999999999999999', '0.99999999999999999999', '9999999999.9999999999', '9999999999.1111111111');
INSERT INTO t1 VALUES ('-99999999999999999999', '-0.99999999999999999999', '-9999999999.9999999999', '-9999999999.1111111111');

> CREATE SOURCE da FROM MYSQL CONNECTION mysqc FOR TABLES (public.t1);

> SELECT * FROM t1;
99999999999999999999 0.99999999999999999999 9999999999.9999999999 9999999999.11111
-99999999999999999999 -0.99999999999999999999 -9999999999.9999999999 -9999999999.11111

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3), pg_typeof(f4) FROM t1 LIMIT 1;
numeric numeric numeric numeric

> SELECT * FROM t1;
99999999999999999999 0.99999999999999999999 9999999999.9999999999 9999999999.11111
-99999999999999999999 -0.99999999999999999999 -9999999999.9999999999 -9999999999.11111
99999999999999999999 0.99999999999999999999 9999999999.9999999999 9999999999.11111
-99999999999999999999 -0.99999999999999999999 -9999999999.9999999999 -9999999999.11111
