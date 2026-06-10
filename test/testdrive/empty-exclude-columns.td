# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for ss-159: an empty `EXCLUDE COLUMNS = ()` on a
# `CREATE TABLE .. FROM SOURCE` statement is treated as though the option
# were omitted and must succeed.

# Test Postgres version

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;
CREATE TABLE foo (id INT PRIMARY KEY, name TEXT);
ALTER TABLE foo REPLICA IDENTITY FULL;
INSERT INTO foo VALUES (1, 'one'), (2, 'two');
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pgconn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
> CREATE SOURCE s FROM POSTGRES CONNECTION pgconn (PUBLICATION 'mz_source')

> CREATE TABLE foo FROM SOURCE s (REFERENCE = public.foo) WITH (EXCLUDE COLUMNS = ());

> SELECT * FROM foo;
1 one
2 two


# Test MySql version

$ mysql-connect name=mysql url=mysql://root@mysql password=p@ssw0rd

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE bar (id INT PRIMARY KEY, name TEXT);
INSERT INTO bar VALUES (1, 'one'), (2, 'two');

> CREATE SECRET mysqlpass AS 'p@ssw0rd';

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE SOURCE s2 FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE bar FROM SOURCE s2 (REFERENCE = public.bar) WITH (EXCLUDE COLUMNS = ());

> SELECT * FROM bar;
1 one
2 two
