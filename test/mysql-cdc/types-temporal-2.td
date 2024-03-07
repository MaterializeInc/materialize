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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

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

SET GLOBAL time_zone = 'US/Eastern';

# reconnect for global variable change to take effect
$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
USE public;
SET SESSION time_zone = 'UTC';

# MySQL 5.7 needs a default value for timestamp_col
CREATE TABLE t1 (date_col DATE, date_time_col DATETIME, timestamp_col TIMESTAMP DEFAULT '2000-01-01');
INSERT INTO t1 VALUES ('1000-01-01', '1000-01-01 00:00:00', '1970-01-01 00:00:01');
INSERT INTO t1 VALUES ('9999-12-31', '9999-12-31 23:59:59', '2038-01-18 03:14:17');

CREATE TABLE t2 (time_col TIME);
INSERT INTO t2 VALUES ('00:00:00');
INSERT INTO t2 VALUES ('23:59:59');

CREATE TABLE t3 (year_col YEAR);
INSERT INTO t3 VALUES ('1901');
INSERT INTO t3 VALUES ('2155');

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn (
    TEXT COLUMNS (public.t3.year_col)
  )
  FOR ALL TABLES;

> SELECT date_col, date_time_col, timestamp_col AT TIME ZONE 'UTC' FROM t1;
"1000-01-01" "1000-01-01 00:00:00" "1970-01-01 00:00:01 UTC"
"9999-12-31" "9999-12-31 23:59:59" "2038-01-18 03:14:17 UTC"

> SELECT * FROM t2;
"00:00:00"
"23:59:59"

> SELECT * FROM t3;
"1901"
"2155"
