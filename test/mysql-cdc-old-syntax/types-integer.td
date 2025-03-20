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
# Test various variants of integer types

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

# Insert data pre-snapshot
CREATE TABLE t1 (f1 SMALLINT, f2 INTEGER, f3 BIGINT, f4 TINYINT);

INSERT INTO t1 VALUES (-32768, -2147483648, -9223372036854775808, -128);
INSERT INTO t1 VALUES (32767, 2147483647, 9223372036854775807, 127);

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3), pg_typeof(f4) FROM t1 LIMIT 1;
smallint integer bigint smallint

> SELECT * FROM t1;
-32768 -2147483648 -9223372036854775808 -128
32767 2147483647 9223372036854775807 127
-32768 -2147483648 -9223372036854775808 -128
32767 2147483647 9223372036854775807 127
