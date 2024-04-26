# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# MySQL specifies that pg type INTERVAL is mapped to TIME in MySQL.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

#
# Test the INTERVAL type
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
CREATE TABLE t1 (f1 TIME(6));

INSERT INTO t1 VALUES (TIME '23:59:59')

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1) FROM t1 LIMIT 1;
time

> SELECT * FROM t1;
"23:59:59"
"23:59:59"

# Now insert an out-of-bounds time value that should
# put us into an error state

$ mysql-execute name=mysql
INSERT INTO t1 VALUES (TIME '838:59:59')

! SELECT * FROM t1;
contains: error decoding value

# Drop and recreate the source to confirm the decoding
# error is also encountered in the snapshot phase

> DROP SOURCE mz_source CASCADE;

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

! SELECT * FROM t1;
contains: error decoding value

> DROP SOURCE mz_source CASCADE;
