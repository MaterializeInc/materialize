# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

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

CREATE TABLE table_a (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_a VALUES (1, 'one');
INSERT INTO table_a VALUES (2, 'two');

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE table_a FROM SOURCE mz_source (REFERENCE public.table_a);
> COMMIT

> SELECT * FROM table_a;
1 one
2 two

$ mysql-execute name=mysql
DROP TABLE table_a CASCADE;

# Adding a table with the same name as a running table does not allow you to add
# the new table.

$ mysql-execute name=mysql
CREATE TABLE table_a (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_a VALUES (9, 'nine');

! SELECT * FROM table_a;
contains:table was dropped

# We are not aware that the new table_a is different
! CREATE TABLE table_a FROM SOURCE mz_source (REFERENCE public.table_a);
contains:catalog item 'table_a' already exists

> DROP SOURCE mz_source CASCADE;

# Re-populate tables for rest of test.

$ mysql-execute name=mysql

DELETE FROM table_a;
INSERT INTO table_a VALUES (1, 'one');
INSERT INTO table_a VALUES (2, 'two');

CREATE TABLE table_b (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_b VALUES (1, 'one');
INSERT INTO table_b VALUES (2, 'two');

CREATE TABLE table_c (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_c VALUES (1, 'one');
INSERT INTO table_c VALUES (2, 'two');

CREATE TABLE table_d (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_d VALUES (1, 'one');
INSERT INTO table_d VALUES (2, 'two');

CREATE TABLE table_e (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_e VALUES (1, 'one');
INSERT INTO table_e VALUES (2, 'two');

CREATE TABLE table_f (pk INTEGER PRIMARY KEY, f2 ENUM ('var0', 'var1'));
INSERT INTO table_f VALUES (1, 'var0');
INSERT INTO table_f VALUES (2, 'var1');

CREATE TABLE table_g (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_g VALUES (1, 'one');
INSERT INTO table_g VALUES (2, 'two');

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE table_a FROM SOURCE mz_source (REFERENCE public.table_a);
> CREATE TABLE table_b FROM SOURCE mz_source (REFERENCE public.table_b);
> CREATE TABLE table_c FROM SOURCE mz_source (REFERENCE public.table_c);
> CREATE TABLE table_d FROM SOURCE mz_source (REFERENCE public.table_d);
> CREATE TABLE table_e FROM SOURCE mz_source (REFERENCE public.table_e);
> CREATE TABLE table_f FROM SOURCE mz_source (REFERENCE public.table_f) WITH (TEXT COLUMNS (f2));
> CREATE TABLE table_g FROM SOURCE mz_source (REFERENCE public.table_g);
> COMMIT

> SHOW SUBSOURCES ON mz_source

> SHOW TABLES
table_a               ""
table_b               ""
table_c               ""
table_d               ""
table_e               ""
table_f               ""
table_g               ""

$ set-regex match="DETAILS = '[a-f0-9]+'" replacement=<DETAILS>

> SHOW CREATE SOURCE mz_source;
materialize.public.mz_source "CREATE SOURCE materialize.public.mz_source\nIN CLUSTER quickstart\nFROM MYSQL CONNECTION materialize.public.mysql_conn;"

> SHOW CREATE TABLE table_a;
materialize.public.table_a "CREATE TABLE materialize.public.table_a (pk pg_catalog.int4 NOT NULL, f2 pg_catalog.text, CONSTRAINT \"PRIMARY\" PRIMARY KEY (pk)) FROM SOURCE materialize.public.mz_source (REFERENCE = public.table_a) WITH (<DETAILS>);"

#
# Error checking
#


> CREATE TABLE mz_table (a int);

> DROP TABLE mz_table;

> BEGIN
> CREATE SOURCE "mz_source_too"
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE t_a FROM SOURCE "mz_source_too" (REFERENCE public.table_a);
> COMMIT

> DROP SOURCE mz_source_too CASCADE;

#
# State checking
#

> DROP TABLE table_a

> SELECT * FROM table_b;
1 one
2 two

> SHOW SUBSOURCES ON mz_source

> SHOW TABLES
table_b               ""
table_c               ""
table_d               ""
table_e               ""
table_f               ""
table_g               ""

! SELECT * FROM table_a;
contains: unknown catalog item 'table_a'

# Makes progress after dropping subsources
$ mysql-execute name=mysql
INSERT INTO table_b VALUES (3, 'three');

> SELECT * FROM table_b;
1 one
2 two
3 three

# IF EXISTS works
> DROP TABLE IF EXISTS table_a;

# Multiple, repetitive tables work
> DROP TABLE table_b, table_c, table_b, table_c, table_b, table_c;

# IF EXISTS works with multiple tables
> DROP TABLE IF EXISTS table_c, table_d;

> CREATE MATERIALIZED VIEW mv_e AS SELECT pk + 1 FROM table_e;
> CREATE MATERIALIZED VIEW mv_f AS SELECT pk + 1 FROM table_f;

# Makes progress after dropping subsources
$ mysql-execute name=mysql
INSERT INTO table_e VALUES (3, 'three');

> SELECT * FROM mv_e;
2
3
4

> SELECT name FROM mz_materialized_views
mv_e
mv_f

# RESTRICT works
! DROP TABLE table_e RESTRICT;
contains:cannot drop table "table_e": still depended upon by materialized view "mv_e"

# CASCADE works
> DROP TABLE table_e CASCADE;

# IF NOT EXISTS + CASCADE works
> DROP TABLE IF EXISTS table_e, table_f CASCADE;

> SHOW SUBSOURCES ON mz_source

> SHOW TABLES
table_g               ""

> SELECT name FROM mz_materialized_views

> DROP TABLE table_g;

> SHOW SUBSOURCES ON mz_source

#
# Add tables
#
> CREATE TABLE table_g FROM SOURCE mz_source (REFERENCE public.table_g);
! CREATE TABLE table_g FROM SOURCE mz_source (REFERENCE public.table_g);
contains:catalog item 'table_g' already exists

> CREATE TABLE table_a FROM SOURCE mz_source (REFERENCE public.table_a);
> CREATE TABLE tb FROM SOURCE mz_source (REFERENCE public.table_b);

> SELECT * FROM table_a;
1 one
2 two

> SELECT * FROM tb;
1 one
2 two
3 three

! SELECT * FROM table_b;
contains:unknown catalog item

# We can add tables that didn't exist at the time of publication
$ mysql-execute name=mysql
CREATE TABLE table_h (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_h VALUES (1, 'one');
INSERT INTO table_h VALUES (2, 'two');

> CREATE TABLE table_h FROM SOURCE mz_source (REFERENCE public.table_h);

> SELECT * FROM table_h;
1 one
2 two

> SHOW SUBSOURCES ON mz_source

> SHOW TABLES
table_a            ""
table_g            ""
table_h            ""
tb                 ""

#
# Complex subsource operations
#
# If your schema change breaks the subsource, you can fix it.
$ mysql-execute name=mysql
ALTER TABLE table_a DROP COLUMN f2;
INSERT INTO table_a VALUES (3);

! SELECT * FROM table_a;
contains:incompatible schema change

> SELECT error ~~ '%incompatible schema change%' FROM mz_internal.mz_source_statuses WHERE name = 'table_a';
true

# Subsource errors not propagated to primary source
> SELECT error IS NULL FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
true

> DROP TABLE table_a;

> CREATE TABLE table_a FROM SOURCE mz_source (REFERENCE public.table_a);

> SELECT * FROM table_a;
1
2
3

# If you add columns you can re-ingest them
$ mysql-execute name=mysql
ALTER TABLE table_a ADD COLUMN f2 text;
INSERT INTO table_a VALUES (4, 'four');

> SELECT * FROM table_a;
1
2
3

> DROP TABLE table_a;
> CREATE TABLE table_a FROM SOURCE mz_source (REFERENCE public.table_a);

> SELECT * FROM table_a;
1 <null>
2 <null>
3 <null>
4 four

# If you add a NOT NULL constraint, you can propagate it.
$ mysql-execute name=mysql
ALTER TABLE table_a ADD COLUMN f3 int DEFAULT 1 NOT NULL;
INSERT INTO table_a VALUES (5, 'five', 5);

> DROP TABLE table_a;
> CREATE TABLE table_a FROM SOURCE mz_source (REFERENCE public.table_a);

> SELECT * FROM table_a;
1 <null> 1
2 <null> 1
3 <null> 1
4 four 1
5 five 5

> EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM table_a WHERE f3 IS NULL;
"Explained Query (fast path):\n  Constant <empty>\n\nTarget cluster: quickstart\n"

#
# Can add tables with text columns
#
! CREATE TABLE table_f FROM SOURCE mz_source (REFERENCE public.table_f) WITH (TEXT COLUMNS [f2, f2]);
contains: invalid TEXT COLUMNS option value: unexpected multiple references to public.table_f.f2

> CREATE TABLE table_f FROM SOURCE mz_source (REFERENCE public.table_f) WITH (TEXT COLUMNS (f2));

> SELECT * FROM table_f
1 var0
2 var1

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE TABLE table_f);
f2

# Drop a table, which shuffles the tables' output indexes, then add a table and ensure it can be added.
$ mysql-execute name=mysql
DROP TABLE table_c, table_d;

CREATE TABLE table_i (pk INTEGER PRIMARY KEY, f2 ENUM ('var0', 'var1'));
INSERT INTO table_i VALUES (1, 'var0');
INSERT INTO table_i VALUES (2, 'var1');
INSERT INTO table_f VALUES (3, 'var1');

> CREATE TABLE table_i FROM SOURCE mz_source (REFERENCE public.table_i) WITH (TEXT COLUMNS [f2]);

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE TABLE table_i);
f2

> SELECT * FROM table_f
1 var0
2 var1
3 var1

> DROP TABLE table_f, table_i;

! CREATE TABLE table_e FROM SOURCE mz_source (REFERENCE public.table_e) WITH (TEXT COLUMNS (xyz));
contains:TEXT COLUMNS refers to table not currently being added

# Test adding text cols w/o original text columns

> BEGIN
> CREATE SOURCE "mz_source_wo_init_text_cols"
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE t_a FROM SOURCE "mz_source_wo_init_text_cols" (REFERENCE public.table_a);
> COMMIT

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE TABLE t_a);
<null>

> CREATE TABLE t_f FROM SOURCE mz_source_wo_init_text_cols (REFERENCE public.table_f) WITH (TEXT COLUMNS [f2]);

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE TABLE t_f);
f2


#
# Can add tables with exclude columns
#
! CREATE TABLE table_f FROM SOURCE mz_source (REFERENCE public.table_f) WITH (EXCLUDE COLUMNS [f2, f2]);
contains: invalid EXCLUDE COLUMNS option value: unexpected multiple references to public.table_f.f2

> CREATE TABLE table_f FROM SOURCE mz_source (REFERENCE public.table_f) WITH (EXCLUDE COLUMNS [f2]);

> SELECT * FROM table_f
1
2
3

> CREATE TABLE table_i FROM SOURCE mz_source (REFERENCE public.table_i) WITH (EXCLUDE COLUMNS [f2]);

> SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE TABLE table_i);
f2

> SELECT * FROM table_i
1
2

> DROP TABLE table_f, table_i;

! CREATE TABLE table_e FROM SOURCE mz_source (REFERENCE public.table_e) WITH (EXCLUDE COLUMNS (a));
contains:EXCLUDE COLUMNS refers to table not currently being added

! CREATE TABLE table_e FROM SOURCE mz_source (REFERENCE public.table_e) WITH (EXCLUDE COLUMNS [a]);
contains:EXCLUDE COLUMNS refers to table not currently being added

# Test adding exclude cols w/o original EXCLUDE COLUMNS

> BEGIN
> CREATE SOURCE "mz_source_wo_init_exclude_cols"
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE t_a2 FROM SOURCE "mz_source_wo_init_exclude_cols" (REFERENCE public.table_a);
> COMMIT

> SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE TABLE t_a2);
<null>

> CREATE TABLE t_f2 FROM SOURCE mz_source_wo_init_exclude_cols (REFERENCE public.table_f) WITH (EXCLUDE COLUMNS [f2]);

> SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE TABLE t_f2);
f2

# Add a table after having created the source
$ mysql-execute name=mysql
CREATE TABLE t2 (f1 BOOLEAN);

! SELECT COUNT(*) > 0 FROM t2;
contains:unknown catalog item 't2'
