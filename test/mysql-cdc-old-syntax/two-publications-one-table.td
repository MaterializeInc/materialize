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
# Test that two publications can replicate the same table
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

> DROP SCHEMA IF EXISTS schema1

> CREATE SCHEMA schema1

> CREATE SOURCE mz_source1
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.t1 AS t1_1);

> CREATE SOURCE mz_source2
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.t1 AS t1_2);

> SELECT * FROM t1_1;
1

> SELECT * FROM t1_2;
1

$ mysql-execute name=mysql
INSERT INTO t1 VALUES (2);

> SELECT * FROM t1_1;
1
2

> SELECT * FROM t1_2;
1
2

> DROP SCHEMA schema1 CASCADE;
