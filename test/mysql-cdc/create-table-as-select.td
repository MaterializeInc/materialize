# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION myconn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE foo (id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO foo VALUES (1, 11), (2,22), (3,33);
COMMIT;


> CREATE SOURCE mysrc FROM MYSQL CONNECTION myconn FOR TABLES (public.foo);

> SELECT * FROM foo;
1 11
2 22
3 33

$ mysql-execute name=mysql
USE public;
CREATE TABLE bar SELECT * FROM foo;
CREATE TABLE charlie TABLE foo;
CREATE TABLE moo AS SELECT * FROM (VALUES ROW(1,2,3), ROW(4,5,6)) AS v;
CREATE TABLE lou(id INTEGER, val INTEGER) partition by hash(id) SELECT * FROM foo;

> SELECT * FROM foo;
1 11
2 22
3 33
