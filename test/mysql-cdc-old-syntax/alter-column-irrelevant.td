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
# Test ALTER TABLE -- some column changes should not do any harm
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

CREATE TABLE t1 (f1 INTEGER);
INSERT INTO t1 VALUES (1);

CREATE TABLE t2 (f1 INTEGER);
INSERT INTO t2 VALUES (1);

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

> SELECT * FROM t1;
1

# just add a new column to t1
$ mysql-execute name=mysql
ALTER TABLE t1 ADD COLUMN f2 INTEGER;
INSERT INTO t1 VALUES (2, 2);

> SELECT * FROM t1;
1
2

# add a new column to t1 at the beginning of
$ mysql-execute name=mysql
ALTER TABLE t1 ADD COLUMN f3 INTEGER FIRST;
INSERT INTO t1 VALUES (3, 3, 3);

! SELECT * FROM t1;
contains:incompatible schema change

# add a new column to t2
$ mysql-execute name=mysql
ALTER TABLE t2 ADD COLUMN f2 INTEGER;
INSERT INTO t2 VALUES (2, 2);

> SELECT * FROM t2;
1
2

# drop the column added after source creation
$ mysql-execute name=mysql
ALTER TABLE t2 DROP COLUMN f2;

# t2 still works
> SELECT * FROM t2;
1
2
