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
# Test that identically-named tables in two destination schemas can be
# successfully disambiguated and replicated
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

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (f1 INTEGER);
INSERT INTO t1 VALUES (1);

CREATE TABLE t2 (f1 INTEGER);
INSERT INTO t2 VALUES (2);


> DROP SCHEMA IF EXISTS schema1 CASCADE;
> DROP SCHEMA IF EXISTS schema2 CASCADE;

> CREATE SCHEMA schema1;
> CREATE SCHEMA schema2;

# TODO: #24797 (schema must be specified even when no collisions exist): remove public prefix
> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.t1 AS schema1.t1, public.t2 AS schema2.t1);

> SELECT * FROM schema1.t1;
1

> SELECT * FROM schema2.t1;
2

$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t2 SELECT * FROM t2;

> SELECT * FROM schema1.t1;
1
1

> SELECT * FROM schema2.t1;
2
2

$ mysql-execute name=mysql
DROP TABLE t1;
DROP TABLE t2;

> DROP SOURCE mz_source CASCADE;
> DROP SCHEMA schema1 CASCADE;
> DROP SCHEMA schema2 CASCADE;
