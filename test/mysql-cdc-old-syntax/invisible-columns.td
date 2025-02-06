# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=20


#
# Invisible columns
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

CREATE TABLE t1 (f1 INT, f2 INT INVISIBLE, f3 INT INVISIBLE);
INSERT INTO t1 (f1, f2, f3) VALUES (10, 20, 30);
INSERT INTO t1 VALUES (11);

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

> SELECT * FROM t1;
10 20 30
11 <null> <null>

$ mysql-execute name=mysql
ALTER TABLE t1 ALTER COLUMN f2 SET VISIBLE;

> SELECT * FROM t1;
10 20 30
11 <null> <null>

$ mysql-execute name=mysql
ALTER TABLE t1 DROP COLUMN f2;

! SELECT * FROM t1;
contains:incompatible schema change
