# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE table_a (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_a VALUES (1, 'one');
ALTER TABLE table_a REPLICA IDENTITY FULL;
INSERT INTO table_a VALUES (2, 'two');

CREATE TABLE table_b (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_b VALUES (1, 'one');
ALTER TABLE table_b REPLICA IDENTITY FULL;
INSERT INTO table_b VALUES (2, 'two');

CREATE TABLE table_c (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_c VALUES (1, 'one');
ALTER TABLE table_c REPLICA IDENTITY FULL;
INSERT INTO table_c VALUES (2, 'two');

CREATE TABLE table_d (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_d VALUES (1, 'one');
ALTER TABLE table_d REPLICA IDENTITY FULL;
INSERT INTO table_d VALUES (2, 'two');

CREATE TABLE table_e (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_e VALUES (1, 'one');
ALTER TABLE table_e REPLICA IDENTITY FULL;
INSERT INTO table_e VALUES (2, 'two');

CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE table_f (pk INTEGER PRIMARY KEY, f2 an_enum);
INSERT INTO table_f VALUES (1, 'var0');
ALTER TABLE table_f REPLICA IDENTITY FULL;
INSERT INTO table_f VALUES (2, 'var1');

CREATE TABLE table_g (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_g VALUES (1, 'one');
ALTER TABLE table_g REPLICA IDENTITY FULL;
INSERT INTO table_g VALUES (2, 'two');

> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS (table_f.f2)
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

# Show all tablestodo this should be splittable
> SELECT regexp_match(create_sql, 'FOR TABLES \((.+?)\) EXPOSE')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"postgres\".\"public\".\"table_a\" AS \"materialize\".\"public\".\"table_a\", \"postgres\".\"public\".\"table_b\" AS \"materialize\".\"public\".\"table_b\", \"postgres\".\"public\".\"table_c\" AS \"materialize\".\"public\".\"table_c\", \"postgres\".\"public\".\"table_d\" AS \"materialize\".\"public\".\"table_d\", \"postgres\".\"public\".\"table_e\" AS \"materialize\".\"public\".\"table_e\", \"postgres\".\"public\".\"table_f\" AS \"materialize\".\"public\".\"table_f\", \"postgres\".\"public\".\"table_g\" AS \"materialize\".\"public\".\"table_g\""

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"postgres\".\"public\".\"table_f\".\"f2\""

#
# Error checking
#

! ALTER SOURCE mz_source DROP SUBSOURCE mz_source_progress
contains:SOURCE "mz_source_progress" is a progress collection and cannot be dropped independently of its primary source

! ALTER SOURCE mz_source DROP SUBSOURCE table_a, mz_source_progress
contains:SOURCE "mz_source_progress" is a progress collection and cannot be dropped independently of its primary source

! ALTER SOURCE mz_source DROP SUBSOURCE mz_source_progress, table_a
contains:SOURCE "mz_source_progress" is a progress collection and cannot be dropped independently of its primary source

! ALTER SOURCE mz_source DROP SUBSOURCE mz_source;
contains:SOURCE "mz_source" is a not a subsource of "mz_source"

> CREATE TABLE mz_table (a int);

! ALTER SOURCE mz_source DROP SUBSOURCE mz_table;
contains:"materialize.public.mz_table" is a table not a source

> DROP TABLE mz_table;

> CREATE SOURCE "mz_source_too"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (public.table_a AS t_a);

! ALTER SOURCE mz_source DROP SUBSOURCE t_a;
contains:SOURCE "t_a" is a not a subsource of "mz_source"

! ALTER SOURCE mz_source DROP SUBSOURCE mz_source_too;
contains:SOURCE "mz_source_too" is a not a subsource of "mz_source"

> DROP SOURCE mz_source_too;

! ALTER SOURCE mz_source DROP SUBSOURCE dne;
contains:unknown catalog item 'dne'

> ALTER SOURCE mz_source DROP SUBSOURCE IF EXISTS dne;

> ALTER SOURCE mz_source DROP SUBSOURCE IF EXISTS dne, dne, dne;

#
# State checking
#

> SELECT * FROM table_b;
1 one
2 two

> ALTER SOURCE mz_source DROP SUBSOURCE table_a;

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
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO table_b VALUES (3, 'three');

> SELECT * FROM table_b;
1 one
2 two
3 three

# IF EXISTS works
> ALTER SOURCE mz_source DROP SUBSOURCE IF EXISTS table_a;

# Multiple, repetitive tables work
> ALTER SOURCE mz_source DROP SUBSOURCE table_b, table_c, table_b, table_c, table_b, table_c;

# IF EXISTS works with multiple tables
> ALTER SOURCE mz_source DROP SUBSOURCE IF EXISTS table_c, table_d;

> CREATE MATERIALIZED VIEW mv_e AS SELECT pk + 1 FROM table_e;
> CREATE MATERIALIZED VIEW mv_f AS SELECT pk + 1 FROM table_f;

# Makes progress after dropping subsources
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO table_e VALUES (3, 'three');

> SELECT * FROM mv_e;
2
3
4

> SHOW MATERIALIZED VIEWS
mv_e default
mv_f default

# RESTRICT works
! ALTER SOURCE mz_source DROP SUBSOURCE IF EXISTS table_e;
contains:cannot drop source table_e: still depended upon by materialized view mv_e

# CASCADE works
> ALTER SOURCE mz_source DROP SUBSOURCE table_e CASCADE;

# IF NOT EXISTS + CASCADE works
> ALTER SOURCE mz_source DROP SUBSOURCE IF EXISTS table_e, table_f CASCADE;

# TEXT COLUMNS removed from table_f
> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
""

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_g               subsource

> SHOW MATERIALIZED VIEWS

# PG sources must retain at least one subsource, if nothing else than for parsing reasons, i.e.
# empty input for subsources is invalid.
! ALTER SOURCE mz_source DROP SUBSOURCE table_g;
contains:SOURCE "mz_source" must retain at least one non-progress subsource

# Show that all table definitions have been updated
> SELECT regexp_match(create_sql, 'FOR TABLES \((.+?)\) EXPOSE') FROM (SHOW CREATE SOURCE mz_source);
"{\"postgres\".\"public\".\"table_g\" AS \"materialize\".\"public\".\"table_g\"}"
