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
DROP DATABASE IF EXISTS other;
CREATE DATABASE IF NOT EXISTS mysql;
USE mysql;
# Insert data pre-snapshot
CREATE TABLE mysql.t_in_mysql (f1 INT);
INSERT INTO mysql.t_in_mysql VALUES (1), (2);

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;
contains:MySQL source must ingest at least one table, but FOR ALL TABLES matched none

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (mysql.timezone);
contains:reference to mysql.timezone not found in source

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (mysql.t_in_mysql);

> SELECT * FROM t_in_mysql;
1
2

> DROP SOURCE mz_source CASCADE;

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (mysql);
contains:referenced tables use unsupported types

$ mysql-execute name=mysql
DROP TABLE t_in_mysql;
