# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s

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

> CREATE SOURCE da FROM MYSQL CONNECTION mysqc FOR ALL TABLES;

> SELECT name, type FROM mz_sources WHERE type IN ('mysql', 'subsource') ORDER BY name DESC;
name       type
------------------------------
da       mysql
dummy    subsource

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
> CREATE SOURCE a.mz_source FROM MYSQL CONNECTION mysqc
  FOR TABLES (public.t1);

> SELECT * FROM a.t1;
1

> DROP SCHEMA IF EXISTS another;
> CREATE SCHEMA another;
> CREATE SOURCE another.mz_source FROM MYSQL CONNECTION mysqc
  FOR TABLES (public.t1, public.t2);

> SELECT * FROM another.t1;
1

> SELECT * FROM another.t2;
5

> DROP SCHEMA IF EXISTS foo;
> DROP SCHEMA IF EXISTS bar;
> CREATE SCHEMA foo;
> CREATE SCHEMA bar;
> CREATE SOURCE foo.mz_source FROM MYSQL CONNECTION mysqc
  FOR TABLES (public.t1 as table1, public.t2 as bar.table2);

! SELECT * FROM table1;
contains: unknown

# table1 gets created in source schema foo because it doesn't have any prefix
> SELECT * FROM foo.table1;
1

! SELECT * FROM foo.table2;
contains: unknown

# table2 gets created in mentioned bar because it does have a prefix
> SELECT * FROM bar.table2;
5

> DROP SCHEMA IF EXISTS baz;
> CREATE SCHEMA baz;
> CREATE SOURCE baz.mz_source FROM MYSQL CONNECTION mysqc
  FOR SCHEMAS (public);

> SELECT * FROM baz.t1;
1

> SELECT * FROM baz.t2;
5
