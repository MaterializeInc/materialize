# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Perform various schema updates to the upstream table

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
CREATE TABLE foo (name VARCHAR(16), value VARCHAR(32));
INSERT INTO foo VALUES ('a', 'apple'), ('b', 'banana');

> CREATE SOURCE mysql_src FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE foo1 FROM SOURCE mysql_src (REFERENCE public.foo);

> SELECT * FROM foo1;
a apple
b banana

$ mysql-execute name=mysql
ALTER TABLE foo ADD COLUMN meta_col VARCHAR(32);
INSERT INTO foo VALUES ('c', 'cherry', 'wild');

# Adding a column to the upstream table does not affect foo1 — it continues to
# replicate only the columns it was created with, ignoring the new meta_col.
> SELECT * FROM foo1;
a apple
b banana
c cherry

$ mysql-execute name=mysql
ALTER TABLE foo MODIFY meta_col VARCHAR(64);
INSERT INTO foo VALUES ('d', 'date', 'ajwa');

# Altering the newly added `meta_col` column does not brick `foo1` because `foo1` is
# not following/replicating `meta_col`, so schema updates involving it are inconsequential.
> SELECT * FROM foo1;
a apple
b banana
c cherry
d date

> DROP TABLE foo1;

# Unlike SQL Server CDC, MySQL binlog-based replication does not require a new capture
# instance for the new column. Creating a table from the existing source will include
# meta_col from the snapshot onward.
> CREATE TABLE foo2 FROM SOURCE mysql_src (REFERENCE public.foo);
> SELECT * FROM foo2;
a apple <null>
b banana <null>
c cherry wild
d date ajwa

$ mysql-execute name=mysql
INSERT INTO foo VALUES ('e', 'elderberry', 'montypython');

> SELECT * FROM foo2;
a apple <null>
b banana <null>
c cherry wild
d date ajwa
e elderberry montypython

> DROP TABLE foo2;

# We can also use EXCLUDE COLUMNS to exclude meta_col if desired.
> CREATE TABLE foo3 FROM SOURCE mysql_src (REFERENCE public.foo) WITH (EXCLUDE COLUMNS = (meta_col));
> SELECT * FROM foo3;
a apple
b banana
c cherry
d date
e elderberry

$ mysql-execute name=mysql
INSERT INTO foo VALUES ('f', 'fig', 'sweet');

> SELECT * FROM foo3;
a apple
b banana
c cherry
d date
e elderberry
f fig

# In MySQL, dropping an excluded column stalls the source with an incompatible schema change.
$ mysql-execute name=mysql
ALTER TABLE foo DROP COLUMN meta_col;

! SELECT * FROM foo3;
contains:incompatible schema change

> DROP TABLE foo3;

# After meta_col has been dropped upstream, foo4 can be created without excluding it.
> CREATE TABLE foo4 FROM SOURCE mysql_src (REFERENCE public.foo);
> SELECT * FROM foo4;
a apple
b banana
c cherry
d date
e elderberry
f fig

$ mysql-execute name=mysql
INSERT INTO foo VALUES ('g', 'grape');

> SELECT * FROM foo4;
a apple
b banana
c cherry
d date
e elderberry
f fig
g grape

# Dropping a non-excluded (tracked) column stalls the source.
$ mysql-execute name=mysql
ALTER TABLE foo DROP COLUMN value;

! SELECT * FROM foo4;
contains:incompatible schema change

> DROP TABLE foo4;
> DROP SOURCE mysql_src CASCADE;

# Test with multiple excluded columns.
$ mysql-execute name=mysql
CREATE TABLE bar (name VARCHAR(16), value VARCHAR(32), age BIGINT, location VARCHAR(32), meta_col VARCHAR(32));
INSERT INTO bar VALUES ('a', 'apple', 5, 'orchard', 'blah'), ('b', 'banana', 7, 'tree', 'blahblah');

> CREATE SOURCE mysql_src2 FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE bar1 FROM SOURCE mysql_src2 (REFERENCE public.bar) WITH (EXCLUDE COLUMNS = (meta_col, location));

> SELECT * FROM bar1;
a apple 5
b banana 7

$ mysql-execute name=mysql
ALTER TABLE bar DROP COLUMN meta_col;

# Dropping an excluded column stalls the source in MySQL.
! SELECT * FROM bar1;
contains:incompatible schema change

> DROP SOURCE mysql_src2 CASCADE;
