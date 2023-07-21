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

# Check empty publication on ALTER

> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source'
  )
  FOR SCHEMAS (public);

> SELECT * FROM table_a;
1 one
2 two

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE table_a CASCADE;

!ALTER SOURCE mz_source ADD SUBSOURCE table_b;
contains:PostgreSQL PUBLICATION mz_source is empty

# Adding a table with the same name as a running table does not allow you to add
# the new table, even though its OID is the different.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE table_a (pk INTEGER PRIMARY KEY, f2 TEXT);
ALTER TABLE table_a REPLICA IDENTITY FULL;
INSERT INTO table_a VALUES (9, 'nine');

# Current table_a is not new table_a. Note that this only works right now
# because we are bad at detecting dropped tables.
> SELECT * FROM table_a;
1 one
2 two

# We are not aware that the new table_a is different
!ALTER SOURCE mz_source ADD SUBSOURCE table_a;
contains:cannot create multiple subsources in the same source that refer to upstream table postgres.public.table_a

> DROP SOURCE mz_source;

# Re-populate tables for rest of test.

$ postgres-execute connection=postgres://postgres:postgres@postgres

DELETE FROM table_a;
INSERT INTO table_a VALUES (1, 'one');
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
    TEXT COLUMNS [table_f.f2]
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

> ALTER SOURCE IF EXISTS dne DROP SUBSOURCE IF EXISTS dne;

> ALTER SOURCE IF EXISTS mz_source DROP SUBSOURCE IF EXISTS dne, dne, dne;

#
# State checking
#

> ALTER SOURCE mz_source DROP SUBSOURCE table_a

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
<null>

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

#
# Add subsources

! ALTER SOURCE mz_source ADD SUBSOURCE table_g;
contains:cannot create multiple subsources in the same source that refer to upstream table postgres.public.table_g

> ALTER SOURCE mz_source ADD SUBSOURCE table_a, table_b AS tb;

> SELECT * FROM table_a;
1 one
2 two

! ALTER SOURCE mz_source ADD SUBSOURCE table_a;
contains:cannot create multiple subsources in the same source that refer to upstream table postgres.public.table_a

> SELECT * FROM tb;
1 one
2 two
3 three

!SELECT * FROM table_b;
contains:unknown catalog item

# We can add tables that didn't exist at the time of publication
$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE table_h (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_h VALUES (1, 'one');
ALTER TABLE table_h REPLICA IDENTITY FULL;
INSERT INTO table_h VALUES (2, 'two');

> ALTER SOURCE mz_source ADD SUBSOURCE table_h;

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

# If your schema change breaks the subsource, you can fix it.
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a DROP COLUMN f2;
INSERT INTO table_a VALUES (3);

! SELECT * FROM table_a;
contains:incompatible schema change

> ALTER SOURCE mz_source DROP SUBSOURCE table_a;

> ALTER SOURCE mz_source ADD SUBSOURCE table_a;

> SELECT * FROM table_a;
1
2
3

# If you add columns you can re-ingest them
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a ADD COLUMN f2 text;
INSERT INTO table_a VALUES (4, 'four');

> SELECT * FROM table_a;
1
2
3

> ALTER SOURCE mz_source DROP SUBSOURCE table_a;
> ALTER SOURCE mz_source ADD SUBSOURCE table_a;

> SELECT * FROM table_a;
1 <null>
2 <null>
3 <null>
4 four

# If you add a NOT NULL constraint, you can propagate it.
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a ADD COLUMN f3 int DEFAULT 1 NOT NULL;
INSERT INTO table_a VALUES (5, 'five', 5);

> ALTER SOURCE mz_source DROP SUBSOURCE table_a;
> ALTER SOURCE mz_source ADD SUBSOURCE table_a;

> SELECT * FROM table_a;
1 <null> 1
2 <null> 1
3 <null> 1
4 four 1
5 five 5

> EXPLAIN SELECT * FROM table_a WHERE f3 IS NULL;
"Explained Query (fast path):\n  Constant <empty>\n"

# Can add tables with text columns
! ALTER SOURCE mz_source ADD SUBSOURCE table_f WITH (TEXT COLUMNS [table_f.f2, table_f.f2]);
contains: invalid TEXT COLUMNS option value: unexpected multiple references to postgres.public.table_f.f2

> ALTER SOURCE mz_source ADD SUBSOURCE table_f WITH (TEXT COLUMNS [table_f.f2]);

> SELECT * FROM table_f
1 var0
2 var1

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"postgres\".\"public\".\"table_f\".\"f2\""

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE table_i (pk INTEGER PRIMARY KEY, f2 an_enum);
INSERT INTO table_i VALUES (1, 'var0');
ALTER TABLE table_i REPLICA IDENTITY FULL;
INSERT INTO table_i VALUES (2, 'var1');

> ALTER SOURCE mz_source ADD SUBSOURCE table_i WITH (TEXT COLUMNS [table_i.f2]);

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"postgres\".\"public\".\"table_f\".\"f2\", \"postgres\".\"public\".\"table_i\".\"f2\""

> ALTER SOURCE mz_source DROP SUBSOURCE table_f, table_i;

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
<null>

! ALTER SOURCE mz_source ADD SUBSOURCE table_e WITH (TEXT COLUMNS (table_z.a));
contains:invalid TEXT COLUMNS option value: table table_z not found in source

! ALTER SOURCE mz_source ADD SUBSOURCE table_e WITH (TEXT COLUMNS [table_f.f2]);
contains:TEXT COLUMNS can only refer to tables being added to source
detail:the following tables were referenced but not added: public.table_f

# Test adding text cols w/o original text columns

> CREATE SOURCE "mz_source_wo_init_text_cols"
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source'
  )
  FOR TABLES (table_a AS t_a);

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_text_cols);
<null>

> ALTER SOURCE mz_source_wo_init_text_cols ADD SUBSOURCE table_f AS t_f WITH (TEXT COLUMNS [table_f.f2]);

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_text_cols);
"\"postgres\".\"public\".\"table_f\".\"f2\""
