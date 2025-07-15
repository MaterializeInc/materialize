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
# Test further temporal data types
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

CREATE TABLE t1 (time_col TIME);
INSERT INTO t1 VALUES ('00:00:00');
INSERT INTO t1 VALUES ('23:59:59');

CREATE TABLE t2 (year_col YEAR);
INSERT INTO t2 VALUES ('1901');
INSERT INTO t2 VALUES ('2155');

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn (
    TEXT COLUMNS (public.t2.year_col)
  )
  FOR ALL TABLES;

> SELECT * FROM t1;
"00:00:00"
"23:59:59"

> SELECT * FROM t2;
"1901"
"2155"
