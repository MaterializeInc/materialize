# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE table_a (x int, y int);

CREATE TABLE table_b (x int, y int);

INSERT INTO table_a SELECT 1,2 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 100;

INSERT INTO table_b SELECT 1,2 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 100;

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysql_conn_1 TO MYSQL (
    HOST toxiproxy,
    PORT 3306,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE CONNECTION mysql_conn_2 TO MYSQL (
    HOST toxiproxy,
    PORT 2306,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE SOURCE mysql_source1
  FROM MYSQL CONNECTION mysql_conn_1
  FOR TABLES (public.table_a);

> CREATE SOURCE mysql_source2
  FROM MYSQL CONNECTION mysql_conn_2
  FOR TABLES (public.table_b);

> CREATE TABLE t (a int);
> INSERT INTO t VALUES (1);

> CREATE MATERIALIZED VIEW sum AS
  SELECT sum(count)
  FROM (
      SELECT count(*) FROM table_a
      UNION ALL SELECT count(*) FROM table_b
      UNION ALL SELECT count(*) FROM t
  ) AS x;
