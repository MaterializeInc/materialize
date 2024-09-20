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
# Test UTF-8 data both pre- and -post snapshot
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# Insert data pre-snapshot
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

# explicitly set the character set for MySQL 5.7
CREATE TABLE t1 (f1 TEXT CHARACTER SET utf8mb4);

INSERT INTO t1 VALUES ('това е текст'), ('това ''е'' "текст"');

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1) FROM t1 LIMIT 1;
text

> SELECT * FROM t1;
"това е текст"
"това \'е\' \"текст\""
"това е текст"
"това \'е\' \"текст\""
