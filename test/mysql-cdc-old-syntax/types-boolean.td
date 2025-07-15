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
# Test the BOOLEAN data type
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
CREATE TABLE t1 (f1 BOOLEAN);
INSERT INTO t1 VALUES (true), (false);

# default values
CREATE TABLE t2 (f1 INT, f2 BOOLEAN DEFAULT true, f3 BOOLEAN DEFAULT false, f4 BOOLEAN);
INSERT INTO t2 VALUES (1, NULL, NULL, NULL);
INSERT INTO t2 VALUES (2, false, false, false);
INSERT INTO t2 (f1) VALUES (3);

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

# MySQL does not have a proper boolean type
> SELECT pg_typeof(f1) FROM t1 LIMIT 1;
smallint

> SELECT * FROM t1;
1
0
1
0

> SELECT * FROM t2;
1 <null> <null> <null>
2 0 0 0
3 1 0 <null>
