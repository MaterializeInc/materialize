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
# Test the BLOB data type
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
# needed for MySQL 5.7
SET GLOBAL max_allowed_packet=67108864;

# reconnect after setting the max_allowed_packet var
$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

# Insert data pre-snapshot
CREATE TABLE t1 (f1 BLOB, f2 TINYBLOB, f3 MEDIUMBLOB, f4 LONGBLOB) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
INSERT INTO t1 VALUES ('abc ', 'abc ', 'abc ', 'abc ');

# 4 bytes per character
CREATE TABLE t2 (f1 LONGBLOB) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
INSERT INTO t2 VALUES (REPEAT('abcdefg x ', 1000));
UPDATE t2 SET f1 = CONCAT(f1, CONCAT(f1, CONCAT(f1, CONCAT(f1, f1))));
UPDATE t2 SET f1 = CONCAT(f1, CONCAT(f1, CONCAT(f1, CONCAT(f1, f1))));
UPDATE t2 SET f1 = CONCAT(f1, CONCAT(f1, CONCAT(f1, CONCAT(f1, f1))));
UPDATE t2 SET f1 = CONCAT(f1, CONCAT(f1, CONCAT(f1, CONCAT(f1, f1))));
UPDATE t2 SET f1 = CONCAT(f1, CONCAT(f1, CONCAT(f1, CONCAT(f1, f1))));
UPDATE t2 SET f1 = CONCAT(f1, f1);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE public.t2);
> COMMIT

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

# MySQL does not have a proper boolean type
> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3), pg_typeof(f4) FROM t1 LIMIT 1;
bytea bytea bytea bytea

> SELECT * FROM t1;
"abc " "abc " "abc " "abc "
"abc " "abc " "abc " "abc "

> SELECT length(f1) FROM t2;
62500000
