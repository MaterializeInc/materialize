# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=3

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'


> CREATE CONNECTION mysqc TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE dummy (f1 INTEGER PRIMARY KEY, id VARCHAR(128));
COMMIT;

#
# Validate CREATE SOURCE
#

> CREATE SOURCE da FROM MYSQL CONNECTION mysqc;
> CREATE TABLE dummy FROM SOURCE da (REFERENCE public.dummy);


> SELECT name, type FROM mz_sources WHERE type IN ('mysql', 'subsource') ORDER BY name DESC;
name       type
------------------------------
da       mysql

> SELECT * FROM dummy;

#
# Validate sub-source naming
#

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;

CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 INTEGER);
INSERT INTO t1 VALUES (1);
CREATE TABLE t2 (f1 INTEGER);
INSERT INTO t2 VALUES (5);
COMMIT;

> DROP SCHEMA IF EXISTS a;
> CREATE SCHEMA a;
> CREATE SOURCE a.mz_source FROM MYSQL CONNECTION mysqc;
> CREATE TABLE a.t1 FROM SOURCE a.mz_source (REFERENCE public.t1);

> SELECT * FROM a.t1;
1

> DROP SCHEMA IF EXISTS another;
> CREATE SCHEMA another;
> CREATE SOURCE another.mz_source FROM MYSQL CONNECTION mysqc;
> CREATE TABLE another.t1 FROM SOURCE another.mz_source (REFERENCE public.t1);
> CREATE TABLE another.t2 FROM SOURCE another.mz_source (REFERENCE public.t2);

> SELECT * FROM another.t1;
1

> SELECT * FROM another.t2;
5

> DROP SCHEMA IF EXISTS foo;
> DROP SCHEMA IF EXISTS bar;
> CREATE SCHEMA foo;
> CREATE SCHEMA bar;
> CREATE SOURCE foo.mz_source FROM MYSQL CONNECTION mysqc;
> CREATE TABLE table1 FROM SOURCE foo.mz_source (REFERENCE public.t1);
> CREATE TABLE bar.table2 FROM SOURCE foo.mz_source (REFERENCE public.t2);

> SELECT * FROM table1;
1

! SELECT * FROM foo.table2;
contains: unknown

# table2 gets created in mentioned bar because it does have a prefix
> SELECT * FROM bar.table2;
5

> DROP SCHEMA IF EXISTS baz;
> CREATE SCHEMA baz;
> CREATE SOURCE baz.mz_source FROM MYSQL CONNECTION mysqc;
> CREATE TABLE baz.t1 FROM SOURCE baz.mz_source (REFERENCE public.t1);
> CREATE TABLE baz.t2 FROM SOURCE baz.mz_source (REFERENCE public.t2);

> SELECT * FROM baz.t1;
1

> SELECT * FROM baz.t2;
5
