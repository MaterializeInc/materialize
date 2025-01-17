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
# Test the BIT data type
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

# Insert data pre-snapshot
CREATE TABLE t1 (f1 BIT(4), f2 BIT(1));
INSERT INTO t1 VALUES (8, 0);
INSERT INTO t1 VALUES (b'0100', b'1');
INSERT INTO t1 VALUES (b'0000', b'0');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

# MySQL does not have a proper boolean type
> SELECT pg_typeof(f1), pg_typeof(f2) FROM t1 LIMIT 1;
bytea bytea

> SELECT * FROM t1;
"\\x00" "\\x00"
"\\x00" "\\x00"
"\\x04" "\\x01"
"\\x04" "\\x01"
"\\x08" "\\x00"
"\\x08" "\\x00"
