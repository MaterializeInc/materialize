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
# Generated columns
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

CREATE TABLE t1 (f1 INT, f2 INT, gf3 INT GENERATED ALWAYS AS (f1 * 100) VIRTUAL, gf4 INT GENERATED ALWAYS AS (f1 * f2) STORED);
INSERT INTO t1 (f1, f2) VALUES (10, 20);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> COMMIT

> SELECT * FROM t1;
10 20 1000 200

$ mysql-execute name=mysql
UPDATE t1 SET f1 = 55;

> SELECT * FROM t1;
55 20 5500 1100
