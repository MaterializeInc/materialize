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
# Test the temporal data types (DATE, DATETIME, TIME, TIMESTAMP)
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

# MySQL 5.7 needs a default value for timestamp_trunc_col
CREATE TABLE t1 (date_col DATE, time_col TIME(6), time_trunc_col TIME, datetime_col DATETIME(6), datetime_trunc_col DATETIME, timestamp_col TIMESTAMP(6), timestamp_trunc_col TIMESTAMP DEFAULT '2000-01-01');

INSERT INTO t1 VALUES ('2011-11-11', '11:11:11.123456', '11:11:11.123456', '2011-11-11 11:11:11.123456', '2011-11-11 11:11:11.123456', '2011-11-11 11:11:11.123456', '2011-11-11 11:11:11.123456');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);

> SELECT * FROM t1;
2011-11-11 11:11:11.123456 11:11:11 "2011-11-11 11:11:11.123456" "2011-11-11 11:11:11" "2011-11-11 11:11:11.123456" "2011-11-11 11:11:11"

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(date_col), pg_typeof(datetime_col), pg_typeof(time_col), pg_typeof(timestamp_col) FROM t1 LIMIT 1;
date "timestamp without time zone" time "timestamp without time zone"

> SELECT * FROM t1;
2011-11-11 11:11:11.123456 11:11:11 "2011-11-11 11:11:11.123456" "2011-11-11 11:11:11" "2011-11-11 11:11:11.123456" "2011-11-11 11:11:11"
2011-11-11 11:11:11.123456 11:11:11 "2011-11-11 11:11:11.123456" "2011-11-11 11:11:11" "2011-11-11 11:11:11.123456" "2011-11-11 11:11:11"
