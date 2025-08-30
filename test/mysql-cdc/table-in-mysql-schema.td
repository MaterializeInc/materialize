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
# Test that tables in the mysql schema are not replicated
#

> DROP SOURCE IF EXISTS mz_source;

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
DROP DATABASE IF EXISTS another_schema;
USE mysql;
CREATE TABLE t_in_mysql (f1 INT);
INSERT INTO t_in_mysql VALUES (1), (2);
CREATE DATABASE public;
USE public;
CREATE TABLE time_zone (f1 INT);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

! CREATE TABLE timezone FROM SOURCE mz_source (REFERENCE public.timezone);
contains:reference to public.timezone not found in source

> CREATE TABLE t_in_mysql FROM SOURCE mz_source (REFERENCE mysql.t_in_mysql);

> SELECT * FROM t_in_mysql;
1
2

> DROP SOURCE mz_source CASCADE;

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t_in_mysql_int_enum FROM SOURCE mz_source (REFERENCE mysql.time_zone);

$ mysql-execute name=mysql
USE mysql;
DROP TABLE t_in_mysql;
USE public;
DROP TABLE time_zone;
