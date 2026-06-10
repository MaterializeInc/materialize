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

SET sql_generate_invisible_primary_key=ON;
CREATE TABLE t1 (f1 INT, f2 INT INVISIBLE, f3 DATE INVISIBLE, f4 INT INVISIBLE);
INSERT INTO t1 (f1, f2, f3, f4) VALUES (10, 20, '2025-01-28', 6);
INSERT INTO t1 VALUES (11);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1) WITH (TEXT COLUMNS (f3), EXCLUDE COLUMNS (f4));
> COMMIT

> SELECT * FROM t1;
1 10 20 "2025-01-28"
2 11 <null> <null>

$ mysql-execute name=mysql
ALTER TABLE t1 ALTER COLUMN f2 SET VISIBLE;
INSERT INTO t1 (f1, f2, f3, f4) VALUES (111, 222, '2025-01-29', 6);
INSERT INTO t1 (f1, f2) VALUES (1111, 2222);

> SELECT * from t1;
1 10 20 "2025-01-28"
2 11 <null> <null>
3 111 222 "2025-01-29"
4 1111 2222 <null>

$ mysql-execute name=mysql
ALTER TABLE t1 DROP COLUMN f2;

! SELECT * FROM t1;
contains:incompatible schema change
