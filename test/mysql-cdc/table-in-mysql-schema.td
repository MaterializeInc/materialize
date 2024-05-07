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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

#
# Test that tables in the mysql schema are not replicated
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
DROP DATABASE IF EXISTS another_schema;
USE mysql;

# Insert data pre-snapshot
CREATE TABLE t_in_mysql (f1 INT);
INSERT INTO t_in_mysql VALUES (1), (2);

> DROP SOURCE IF EXISTS mz_source;

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;
contains:No tables found

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (mysql.timezone);
contains:No tables found

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
