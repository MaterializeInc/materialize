

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

> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source'
  )
  FOR SCHEMAS (public);

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_a               subsource

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a ADD COLUMN c TEXT DEFAULT 'c';
INSERT INTO table_a VALUES (3, 'three', 'd');

# Current strategy of ignoring uningested columns works.
> SELECT * FROM table_a;
1 one
2 two
3 three

> SELECT regexp_match(create_sql, 'FOR TABLES \((.+?)\) EXPOSE')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"postgres\".\"public\".\"table_a\" AS \"materialize\".\"public\".\"table_a\""

# New schema, same name
! ALTER SOURCE mz_source ADD SUBSOURCE table_a;
contains:catalog item 'table_a' already exists

# New schema, new name
> ALTER SOURCE mz_source ADD SUBSOURCE table_a AS table_a_3;
> SELECT * FROM table_a_3;
1 one c
2 two c
3 three d

> SELECT * FROM table_a;
1 one
2 two
3 three

# Now only shows `table_a_3` in `SHOW CREATE SOURCE`
> SELECT regexp_match(create_sql, 'FOR TABLES \((.+?)\) EXPOSE')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"postgres\".\"public\".\"table_a\" AS \"materialize\".\"public\".\"table_a_3\""

# Multiple new columns

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a ADD COLUMN d INT DEFAULT 0;
INSERT INTO table_a VALUES (4, 'four', 'e', 1);

> ALTER SOURCE mz_source ADD SUBSOURCE table_a AS table_a_4;
> SELECT * FROM table_a_4;
1 one c 0
2 two c 0
3 three d 0
4 four e 1

> SELECT * FROM table_a_3;
1 one c
2 two c
3 three d
4 four e

> SELECT * FROM table_a;
1 one
2 two
3 three
4 four

# Now only shows `table_a_4` in `SHOW CREATE SOURCE`
> SELECT regexp_match(create_sql, 'FOR TABLES \((.+?)\) EXPOSE')[1] FROM (SHOW CREATE SOURCE mz_source);
"\"postgres\".\"public\".\"table_a\" AS \"materialize\".\"public\".\"table_a_4\""

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_a               subsource
table_a_3             subsource
table_a_4             subsource

> SELECT schema_name, table_name FROM mz_internal.mz_postgres_source_tables;
public table_a
public table_a
public table_a

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a ADD COLUMN e INT DEFAULT 0;
INSERT INTO table_a VALUES (5, 'five', 'f', 2, 1);

> ALTER SOURCE mz_source
    ADD SUBSOURCE table_a AS table_a_5_text_col
    WITH (
        TEXT COLUMNS = [table_a.e]
    );

> SELECT * FROM table_a_5_text_col;
1 one   c   0   0
2 two   c   0   0
3 three d   0   0
4 four  e   1   0
5 five  f   2   1

> SELECT DISTINCT pg_typeof(e) FROM table_a_5_text_col;
text

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a ADD COLUMN f INT DEFAULT 0;

# Missing TEXT COLUMNS
! ALTER SOURCE mz_source ADD SUBSOURCE table_a AS table_a_6;
contains:new subsource schema incompatible with existing subsource referring to same table
detail:current ingestion ("materialize.public.table_a_5_text_col") specifies TEXT COLUMNS on "postgres.public.table_a.e", but new subsource does not

# Too many TEXT COLUMNS
! ALTER SOURCE mz_source
    ADD SUBSOURCE table_a AS table_a_6_text_cols
    WITH (
        TEXT COLUMNS = [table_a.d, table_a.e]
    );
contains:new subsource schema incompatible with existing subsource referring to same table
detail:new subsource specifies TEXT COLUMNS on "postgres.public.table_a.d", but current ingestion ("materialize.public.table_a_5_text_col") does not

# Just right
> ALTER SOURCE mz_source
    ADD SUBSOURCE table_a AS table_a_6_text_cols
    WITH (
        TEXT COLUMNS = [table_a.e]
    );

# If we drop a column, we cannot update the definition
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a DROP COLUMN f;

! ALTER SOURCE mz_source
    ADD SUBSOURCE table_a AS table_a_5_2
    WITH (
        TEXT COLUMNS = [table_a.e]
    );
contains:new subsource schema incompatible with existing subsource referring to same table

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a ADD COLUMN f INT DEFAULT 0;
ALTER TABLE table_a ADD COLUMN g INT DEFAULT 0;

# These schemas will never line up now that we've dropped one of the columns.
! ALTER SOURCE mz_source
    ADD SUBSOURCE table_a AS table_a_7
    WITH (
        TEXT COLUMNS = [table_a.e]
    );
contains:new subsource schema incompatible with existing subsource referring to same table

# Test creating a subsource with the same resulting schema

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE table_a;
CREATE TABLE table_a (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_a VALUES (1, 'one');
ALTER TABLE table_a REPLICA IDENTITY FULL;
INSERT INTO table_a VALUES (2, 'two');

> DROP SOURCE mz_source CASCADE;

> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS = (table_a.f2)
  )
  FOR SCHEMAS (public);

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE table_a DROP COLUMN f2;
ALTER TABLE table_a ADD COLUMN f2 INT;

# This would generate a subsource with the same schema, so we disallow it.
! ALTER SOURCE mz_source
    ADD SUBSOURCE table_a AS table_a_2
    WITH (
        TEXT COLUMNS = (table_a.f2)
    );
contains:new subsource schema incompatible with existing subsource referring to same table

# Test changing a TEXT COLUMN type and ingesting another additional column
$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE enum_table (a an_enum);
INSERT INTO enum_table VALUES ('var1'), ('var0');
ALTER TABLE enum_table REPLICA IDENTITY FULL;

> DROP SOURCE mz_source CASCADE;

> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS = (enum_table.a)
  )
  FOR TABLES (enum_table);

> SELECT * FROM enum_table
var0
var1

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
ALTER TYPE an_enum RENAME TO an_enum_old;
CREATE TYPE an_enum AS ENUM ('var0', 'var1', 'var2');
ALTER TABLE enum_table ALTER COLUMN a TYPE an_enum USING a::text::an_enum;
DROP TYPE an_enum_old;
COMMIT;

INSERT INTO enum_table VALUES ('var2');

> SELECT * FROM enum_table
var0
var1
var2

# This subsource isn't differentiated
! ALTER SOURCE mz_source
    ADD SUBSOURCE enum_table AS enum_table_2
    WITH (
        TEXT COLUMNS = (enum_table.a)
    );
contains:new subsource schema incompatible with existing subsource referring to same table

# Adding a column will sufficiently differentiate it
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE enum_table ADD COLUMN b INT DEFAULT 0;

> ALTER SOURCE mz_source
    ADD SUBSOURCE enum_table AS enum_table_2
    WITH (
        TEXT COLUMNS = (enum_table.a)
    );

> SELECT * FROM enum_table
var0
var1
var2

> SELECT * FROM enum_table_2
var0 0
var1 0
var2 0

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
ALTER TYPE an_enum RENAME TO an_enum_old;
CREATE TYPE an_enum AS ENUM ('var0', 'var1', 'var2', 'var3');
ALTER TABLE enum_table ALTER COLUMN a TYPE an_enum USING a::text::an_enum;
DROP TYPE an_enum_old;
COMMIT;

INSERT INTO enum_table VALUES ('var3', 1);

> SELECT * FROM enum_table
var0
var1
var2
var3

> SELECT * FROM enum_table_2
var0 0
var1 0
var2 0
var3 1
