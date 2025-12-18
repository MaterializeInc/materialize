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

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (public);

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

# table names must be fully qualified
! ALTER SOURCE mz_source ADD SUBSOURCE table_abc;
contains:reference to table_abc not found in source

# We are not aware that the new table_a is different
! ALTER SOURCE mz_source ADD SUBSOURCE public.table_a;
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

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn (
   TEXT COLUMNS (public.table_f.f2)
  )
  FOR SCHEMAS (public);

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_a               subsource
table_b               subsource
table_c               subsource
table_d               subsource
table_e               subsource
table_f               subsource
table_g               subsource

>[version>=14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
public.table_f.f2

>[version<14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"public\".\"table_f\".\"f2\""

#
# Error checking
#


> CREATE TABLE mz_table (a int);

> DROP TABLE mz_table;

> CREATE SOURCE "mz_source_too"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.table_a AS t_a);

> DROP SOURCE mz_source_too CASCADE;

#
# State checking
#

> DROP SOURCE table_a

> SELECT * FROM table_b;
1 one
2 two

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_b               subsource
table_c               subsource
table_d               subsource
table_e               subsource
table_f               subsource
table_g               subsource

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
> DROP SOURCE IF EXISTS table_a;

# Multiple, repetitive tables work
> DROP SOURCE table_b, table_c, table_b, table_c, table_b, table_c;

# IF EXISTS works with multiple tables
> DROP SOURCE IF EXISTS table_c, table_d;

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
! DROP SOURCE table_e RESTRICT;
contains:cannot drop source "table_e": still depended upon by materialized view "mv_e"

# CASCADE works
> DROP SOURCE table_e CASCADE;

# IF NOT EXISTS + CASCADE works
> DROP SOURCE IF EXISTS table_e, table_f CASCADE;

# TEXT COLUMNS removed from table_f
> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
<null>

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_g               subsource

> SELECT name FROM mz_materialized_views

> DROP SOURCE table_g;

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress

#
# Add subsources
#
> ALTER SOURCE mz_source ADD SUBSOURCE public.table_g;

! ALTER SOURCE mz_source ADD SUBSOURCE public.table_g;
contains:catalog item 'table_g' already exists

> ALTER SOURCE mz_source ADD SUBSOURCE public.table_a, public.table_b AS tb;

> SELECT * FROM table_a;
1 one
2 two

! ALTER SOURCE mz_source ADD SUBSOURCE public.table_a;
contains:catalog item 'table_a' already exists

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

> ALTER SOURCE mz_source ADD SUBSOURCE public.table_h;

> SELECT * FROM table_h;
1 one
2 two

> SHOW SUBSOURCES ON mz_source
mz_source_progress progress
table_a            subsource
table_g            subsource
table_h            subsource
tb                 subsource

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

> DROP SOURCE table_a;

> ALTER SOURCE mz_source ADD SUBSOURCE public.table_a;

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

> DROP SOURCE table_a;
> ALTER SOURCE mz_source ADD SUBSOURCE public.table_a;

> SELECT * FROM table_a;
1 <null>
2 <null>
3 <null>
4 four

# If you add a NOT NULL constraint, you can propagate it.
$ mysql-execute name=mysql
ALTER TABLE table_a ADD COLUMN f3 int DEFAULT 1 NOT NULL;
INSERT INTO table_a VALUES (5, 'five', 5);

> DROP SOURCE table_a;
> ALTER SOURCE mz_source ADD SUBSOURCE public.table_a;

> SELECT * FROM table_a;
1 <null> 1
2 <null> 1
3 <null> 1
4 four 1
5 five 5

>[version>=13500] EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM table_a WHERE f3 IS NULL;
"Explained Query (fast path):\n  Constant <empty>\n\nTarget cluster: quickstart\n"

>[version<13500] EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM table_a WHERE f3 IS NULL;
"Explained Query (fast path):\n  Constant <empty>\n\nTarget cluster: quickstart\n"

#
# Can add tables with text columns
#
! ALTER SOURCE mz_source ADD SUBSOURCE public.table_f WITH (TEXT COLUMNS [public.table_f.f2, public.table_f.f2]);
contains: invalid TEXT COLUMNS option value: unexpected multiple references to public.table_f.f2

> ALTER SOURCE mz_source ADD SUBSOURCE public.table_f WITH (TEXT COLUMNS [public.table_f.f2]);

> SELECT * FROM table_f
1 var0
2 var1

>[version>=14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
public.table_f.f2

>[version<14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"public\".\"table_f\".\"f2\""

>[version>=14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE table_f);
"f2"

>[version<14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE table_f);
"\"f2\""

# Drop a table, which shuffles the tables' output indexes, then add a table and ensure it can be added.
$ mysql-execute name=mysql
DROP TABLE table_c, table_d;

CREATE TABLE table_i (pk INTEGER PRIMARY KEY, f2 ENUM ('var0', 'var1'));
INSERT INTO table_i VALUES (1, 'var0');
INSERT INTO table_i VALUES (2, 'var1');
INSERT INTO table_f VALUES (3, 'var1');

> ALTER SOURCE mz_source ADD SUBSOURCE public.table_i WITH (TEXT COLUMNS [public.table_i.f2]);

>[version>=14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"public.table_f.f2, public.table_i.f2"

>[version<14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"public\".\"table_f\".\"f2\", \"public\".\"table_i\".\"f2\""

>[version>=14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE table_i);
f2

>[version<14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE table_i);
"\"f2\""

> SELECT * FROM table_f
1 var0
2 var1
3 var1

> DROP SOURCE table_f, table_i;

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
<null>

! ALTER SOURCE mz_source ADD SUBSOURCE public.table_e WITH (TEXT COLUMNS (public.table_z.a));
contains:TEXT COLUMNS refers to table not currently being added

! ALTER SOURCE mz_source ADD SUBSOURCE public.table_e WITH (TEXT COLUMNS [public.table_f.f2]);
contains:TEXT COLUMNS refers to table not currently being added
detail:the following columns are referenced but not added: public.table_f.f2

# Test adding text cols w/o original text columns

> CREATE SOURCE "mz_source_wo_init_text_cols"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.table_a AS t_a);

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_text_cols);
<null>

> ALTER SOURCE mz_source_wo_init_text_cols ADD SUBSOURCE public.table_f AS t_f WITH (TEXT COLUMNS [public.table_f.f2]);

>[version>=14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_text_cols);
"public.table_f.f2"

>[version<14000] SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_text_cols);
"\"public\".\"table_f\".\"f2\""

#
# Can add tables with exclude columns
#
! ALTER SOURCE mz_source ADD SUBSOURCE public.table_f WITH (EXCLUDE COLUMNS [public.table_f.f2, public.table_f.f2]);
contains: invalid EXCLUDE COLUMNS option value: unexpected multiple references to public.table_f.f2

> ALTER SOURCE mz_source ADD SUBSOURCE public.table_f WITH (EXCLUDE COLUMNS [public.table_f.f2]);

> SELECT * FROM table_f
1
2
3

>[version>=14000] SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"public.table_f.f2"

>[version<14000] SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"public\".\"table_f\".\"f2\""

> ALTER SOURCE mz_source ADD SUBSOURCE public.table_i WITH (EXCLUDE COLUMNS [public.table_i.f2]);

>[version>=14000] SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"public.table_f.f2, public.table_i.f2"

>[version<14000] SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"public\".\"table_f\".\"f2\", \"public\".\"table_i\".\"f2\""

>[version>=14000] SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE table_i);
f2

>[version<14000] SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE table_i);
"\"f2\""

> SELECT * FROM table_i
1
2

> DROP SOURCE table_f, table_i;

> SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
<null>

! ALTER SOURCE mz_source ADD SUBSOURCE public.table_e WITH (EXCLUDE COLUMNS (public.table_z.a));
contains:EXCLUDE COLUMNS refers to table not currently being added

! ALTER SOURCE mz_source ADD SUBSOURCE public.table_e WITH (EXCLUDE COLUMNS [public.table_f.f2]);
contains:EXCLUDE COLUMNS refers to table not currently being added
detail:the following columns are referenced but not added: public.table_f.f2

# Test adding exclude cols w/o original EXCLUDE COLUMNS

> CREATE SOURCE "mz_source_wo_init_exclude_cols"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.table_a AS t_a2);

> SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_exclude_cols);
<null>

> ALTER SOURCE mz_source_wo_init_exclude_cols ADD SUBSOURCE public.table_f AS t_f2 WITH (EXCLUDE COLUMNS [public.table_f.f2]);

>[version>=14000] SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_exclude_cols);
"public.table_f.f2"

>[version<14000] SELECT regexp_match(create_sql, 'EXCLUDE COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_exclude_cols);
"\"public\".\"table_f\".\"f2\""

# Add a table after having created the source
$ mysql-execute name=mysql
CREATE TABLE t2 (f1 BOOLEAN);

! SELECT COUNT(*) > 0 FROM t2;
contains:unknown catalog item 't2'
