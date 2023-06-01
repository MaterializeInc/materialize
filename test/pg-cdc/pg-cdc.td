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
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (2, 'two');

INSERT INTO pk_table VALUES (3, 'three');

CREATE TABLE nonpk_table (f1 INTEGER, f2 INTEGER);
INSERT INTO nonpk_table VALUES (1, 1), (1, 1);
ALTER TABLE nonpk_table REPLICA IDENTITY FULL;
INSERT INTO nonpk_table VALUES (2, 2), (2, 2);

CREATE TABLE types_table (char_col char(3), date_col DATE, time_col TIME, timestamp_col TIMESTAMP, uuid_col UUID, double_col DOUBLE PRECISION, numeric NUMERIC(8,4), int4range_col INT4RANGE, int8range_col INT8RANGE, daterange_col DATERANGE, numrange_col NUMRANGE);
INSERT INTO types_table VALUES ('foo', '2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1234.56768, 1234.5678, '(,)', '(,)', '(,)', '(,)');
ALTER TABLE types_table REPLICA IDENTITY FULL;

CREATE TABLE array_types_table (date_col DATE[], time_col TIME[], timestamp_col TIMESTAMP[], uuid_col UUID[], double_col DOUBLE PRECISION[], numeric NUMERIC[], int4range_col INT4RANGE[], int8range_col INT8RANGE[], daterange_col DATERANGE[], numrange_col NUMRANGE[]);
INSERT INTO array_types_table VALUES ('{2011-11-11}', '{11:11:11}', '{2011-11-11 11:11:11}', '{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}', '{1234.56768}', '{1234.5678}', '{"(,)"}', '{"(,)"}', '{"(,)"}', '{"(,)"}');
ALTER TABLE array_types_table REPLICA IDENTITY FULL;

CREATE TABLE large_text (f1 TEXT, f2 TEXT);
INSERT INTO large_text VALUES (REPEAT('x', 16 * 1024 * 1024), REPEAT('y', 1 * 1024 * 1024));
INSERT INTO large_text VALUES (REPEAT('a', 3 * 1024 * 1024),  REPEAT('b', 2 * 1024 * 1024));
ALTER TABLE large_text REPLICA IDENTITY FULL;

CREATE TABLE trailing_space_pk (f1 TEXT PRIMARY KEY);
INSERT INTO trailing_space_pk VALUES ('abc   ');
ALTER TABLE trailing_space_pk REPLICA IDENTITY FULL;

CREATE TABLE trailing_space_nopk (f1 TEXT);
INSERT INTO trailing_space_nopk VALUES ('abc   ');
ALTER TABLE trailing_space_nopk REPLICA IDENTITY FULL;

CREATE TABLE multipart_pk(f1 INTEGER, f2 TEXT, f3 TEXT, PRIMARY KEY (f1, f2));
INSERT INTO multipart_pk VALUES (1, 'abc', 'xyz');
ALTER TABLE multipart_pk REPLICA IDENTITY FULL;

CREATE TABLE nulls_table (f1 TEXT, f2 INTEGER);
INSERT INTO nulls_table VALUES (NULL, NULL);
ALTER TABLE nulls_table REPLICA IDENTITY FULL;

CREATE TABLE utf8_table (f1 TEXT PRIMARY KEY, f2 TEXT);
INSERT INTO utf8_table VALUES ('това е текст', 'това ''е'' "текст"');
ALTER TABLE utf8_table REPLICA IDENTITY FULL;

CREATE TABLE no_replica_identity (f1 INTEGER);
INSERT INTO no_replica_identity VALUES (1), (2);

CREATE TABLE "таблица" ("колона" TEXT);
ALTER TABLE "таблица" REPLICA IDENTITY FULL;
INSERT INTO "таблица" VALUES ('стойност');

CREATE TABLE """literal_quotes""" (a TEXT);
ALTER TABLE """literal_quotes""" REPLICA IDENTITY FULL;
INSERT INTO """literal_quotes""" VALUES ('v');

CREATE TABLE "create" (a TEXT);
ALTER TABLE "create" REPLICA IDENTITY FULL;
INSERT INTO "create" VALUES ('v');

CREATE TABLE escaped_text_table (f1 TEXT, f2 TEXt);
ALTER TABLE escaped_text_table REPLICA IDENTITY FULL;
INSERT INTO escaped_text_table VALUES ('escaped\ntext\twith\nnewlines\tand\ntabs', 'more\tescaped\ntext');
INSERT INTO escaped_text_table VALUES ('second\nrow\twith\tmore\ttabs', 'and\nmore\n\nnewlines\n');

CREATE TABLE conflict_table (f1 INTEGER);
ALTER TABLE conflict_table REPLICA IDENTITY FULL;
INSERT INTO conflict_table VALUES (123);

DROP SCHEMA IF EXISTS conflict_schema CASCADE;
CREATE SCHEMA conflict_schema;
CREATE TABLE conflict_schema.conflict_table (f1 TEXT);
ALTER TABLE conflict_schema.conflict_table REPLICA IDENTITY FULL;
INSERT INTO conflict_schema.conflict_table VALUES ('234');

CREATE TABLE "space table" ("space column" INTEGER);

CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE enum_table (a an_enum);
INSERT INTO enum_table VALUES ('var1'), ('var0');
ALTER TABLE enum_table REPLICA IDENTITY FULL;

CREATE TYPE another_enum AS ENUM ('var2', 'var3');
CREATE TABLE another_enum_table ("колона" another_enum);
INSERT INTO another_enum_table VALUES ('var2'), ('var3');
ALTER TABLE another_enum_table REPLICA IDENTITY FULL;

CREATE TABLE conflict_schema.another_enum_table ("колона" another_enum);
ALTER TABLE conflict_schema.another_enum_table REPLICA IDENTITY FULL;
INSERT INTO conflict_schema.another_enum_table VALUES ('var2'), ('var3');
ALTER TABLE conflict_schema.another_enum_table REPLICA IDENTITY FULL;

DROP PUBLICATION IF EXISTS mz_source_narrow;

CREATE PUBLICATION mz_source_narrow FOR TABLE enum_table, public.another_enum_table, pk_table;

DROP SCHEMA IF EXISTS another_schema CASCADE;
CREATE SCHEMA another_schema;
CREATE TABLE another_schema.another_table (f1 TEXT);
ALTER TABLE another_schema.another_table REPLICA IDENTITY FULL;
INSERT INTO another_schema.another_table VALUES ('123');

DROP PUBLICATION IF EXISTS another_publication;

CREATE PUBLICATION another_publication FOR TABLE another_schema.another_table;

#
# Test that slots created for replication sources are deleted on DROP
#
$ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=false

> CREATE SOURCE "test_slot_source"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES ("pk_table");

$ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=true

> SELECT source, subsource, type FROM mz_internal.mz_get_subsources('test_slot_source');
materialize.public.test_slot_source materialize.public.pk_table subsource
materialize.public.test_slot_source materialize.public.test_slot_source_progress progress

> DROP SOURCE test_slot_source

$ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=false

#
# Error checking
#

> CREATE CONNECTION no_such_host TO POSTGRES (
    HOST 'no_such_postgres.mtrlz.com',
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
! CREATE SOURCE "no_such_host"
  FROM POSTGRES CONNECTION no_such_host (PUBLICATION 'mz_source');
# TODO: assert on `detail` here.
exact:failed to connect to PostgreSQL database

> CREATE CONNECTION no_such_port TO POSTGRES (
    HOST postgres,
    PORT 65534,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
! CREATE SOURCE "no_such_port"
  FROM POSTGRES CONNECTION no_such_port (PUBLICATION 'mz_source');
# TODO: assert on `detail` here.
exact:failed to connect to PostgreSQL database

> CREATE CONNECTION no_such_user TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER no_such_user,
    PASSWORD SECRET pgpass
  )
! CREATE SOURCE "no_such_user"
  FROM POSTGRES CONNECTION no_such_user (PUBLICATION 'mz_source');
# TODO: assert on `detail` here.
exact:failed to connect to PostgreSQL database

> CREATE SECRET badpass AS 'badpass'
> CREATE CONNECTION no_such_password TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET badpass
  )
! CREATE SOURCE "no_such_password"
  FROM POSTGRES CONNECTION no_such_password (PUBLICATION 'mz_source');
# TODO: assert on `detail` here.
exact:failed to connect to PostgreSQL database

> CREATE CONNECTION no_such_dbname TO POSTGRES (
    HOST postgres,
    DATABASE no_such_dbname,
    USER postgres,
    PASSWORD SECRET pgpass
  )
! CREATE SOURCE "no_such_dbname"
  FROM POSTGRES CONNECTION no_such_dbname (PUBLICATION 'mz_source');
# TODO: assert on `detail` here.
exact:failed to connect to PostgreSQL database

! CREATE SOURCE "no_such_publication"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'no_such_publication');
# TODO: assert on `detail` here.
exact:failed to connect to PostgreSQL database

! CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (
    "enum_table"
  );
regex: the following columns contain unsupported types:\npostgres.public.enum_table.a \(OID \d+\)

! CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (
    "enum_table",
    public.another_enum_table
  );
regex: the following columns contain unsupported types:\npostgres\.public\.another_enum_table\."колона" \(OID \d+\)\npostgres\.public\.enum_table\.a \(OID \d+\)

#
# Establish direct replication
#
#
# Note: This implicitly tests that enum_table being part of the publication does not
# prevent us from using other tables as subsources.
#
# TODO: This should produce an error because we included the `no_replica_identity` table
> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (
    "no_replica_identity",
    "pk_table",
    "nonpk_table",
    "types_table",
    "array_types_table",
    "large_text",
    "trailing_space_pk",
    "trailing_space_nopk",
    "multipart_pk",
    "nulls_table",
    "utf8_table",
    "таблица",
    "escaped_text_table",
    conflict_schema.conflict_table AS public.conflict_table,
    "space table",
    """literal_quotes""",
    "create"
  );

> SHOW SOURCES
array_types_table     subsource <null>
conflict_table        subsource <null>
create                subsource <null>
escaped_text_table    subsource <null>
large_text            subsource <null>
multipart_pk          subsource <null>
mz_source             postgres  ${arg.default-storage-size}
mz_source_progress    progress  <null>
no_replica_identity   subsource <null>
nonpk_table           subsource <null>
nulls_table           subsource <null>
pk_table              subsource <null>
"space table"         subsource <null>
trailing_space_nopk   subsource <null>
trailing_space_pk     subsource <null>
"\"literal_quotes\""  subsource <null>
types_table           subsource <null>
utf8_table            subsource <null>
таблица               subsource <null>

> SELECT lsn > 0 FROM mz_source_progress
true

# Ensure we report the write frontier of the progress subsource
$ set-regex match=(\s{12}0|\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)|true|false) replacement=<>
> EXPLAIN TIMESTAMP FOR SELECT * FROM mz_source_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.mz_source_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

$ set-regex match=[0-9]+|_[a-f0-9]+ replacement=<SUPPRESSED>

> SELECT * FROM mz_internal.mz_postgres_sources
id             replication_slot
---------------------------------------
u<SUPPRESSED>  materialize<SUPPRESSED>

$ unset-regex

# Cannot drop subsources independent of primary source
! DROP SOURCE conflict_table
contains: SOURCE materialize.public.conflict_table is a subsource and cannot be dropped independently of its primary source

! DROP SOURCE conflict_table CASCADE
contains: SOURCE materialize.public.conflict_table is a subsource and cannot be dropped independently of its primary source

> SELECT * FROM no_replica_identity;
1
2

#
# Perform sanity checks of the initial snapshot
#

> SELECT * FROM pk_table;
1 one
2 two
3 three

> SELECT * FROM nonpk_table;
1 1
1 1
2 2
2 2

> SELECT * FROM types_table;
"foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678" "(,)" "(,)" "(,)" "(,)"

> SELECT pg_typeof(numeric) FROM types_table;
"numeric"

> SELECT * FROM array_types_table;
"{2011-11-11}" "{11:11:11}" "{2011-11-11 11:11:11}" "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}" "{1234.56768}" "{1234.5678}" "{(,)}" "{(,)}" "{(,)}" "{(,)}"

> SELECT LENGTH(f1), LENGTH(f2) FROM large_text;
16777216 1048576
3145728  2097152

> SELECT LENGTH(f1), f1 FROM trailing_space_pk;
"6" "abc   "

> SELECT LENGTH(f1), f1 FROM trailing_space_nopk;
"6" "abc   "

> SELECT * FROM multipart_pk;
1 abc xyz

> SELECT f1, f2, f1 IS NULL, f2 IS NULL FROM nulls_table;
<null> <null> true true

> SELECT * FROM utf8_table;
"това е текст" "това \'е\' \"текст\""

> SELECT * FROM "таблица";
стойност

> SELECT * FROM escaped_text_table;
"escaped\\ntext\\twith\\nnewlines\\tand\\ntabs" "more\\tescaped\\ntext"
"second\\nrow\\twith\\tmore\\ttabs" "and\\nmore\\n\\nnewlines\\n"

> SELECT * FROM conflict_table;
234

> SELECT * FROM """literal_quotes"""
v

> SELECT * FROM "create"
v

#
# Confirm that the new sources can be used to build upon
#

> CREATE MATERIALIZED VIEW join_view (a, b, c, d) AS SELECT * FROM pk_table, nonpk_table WHERE pk_table.pk = nonpk_table.f1;

> SELECT * FROM join_view;
"1" "one" "1" "1"
"1" "one" "1" "1"
"2" "two" "2" "2"
"2" "two" "2" "2"

#
# Basic sanity check that the timestamps are reasonable
#

> SELECT COUNT(*) > 0 FROM pk_table;
true

> SELECT COUNT(*) > 0 FROM nonpk_table;
true

> SELECT COUNT(*) > 0 FROM join_view;
true

#
# Modify the tables on the Postgres side
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pk_table VALUES (4, 'four');
INSERT INTO pk_table VALUES (5, 'five');
DELETE FROM pk_table WHERE pk = 1;
UPDATE pk_table SET f2 = 'two_two' WHERE pk = 2;
UPDATE pk_table SET pk = pk + 10 WHERE pk BETWEEN 3 AND 4;

INSERT INTO nonpk_table VALUES (3, 3), (3, 3);
DELETE FROM nonpk_table WHERE ctid = '(0,1)';
UPDATE nonpk_table SET f1 = f1 + 10 WHERE ctid = '(0,2)';
UPDATE nonpk_table SET f1 = f1 + 100 WHERE f1 = 3;

INSERT INTO types_table VALUES ('foo', '2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1234.56768, 1234.5678, 'empty', 'empty', 'empty', 'empty');

INSERT INTO array_types_table VALUES ('{2011-11-11}', '{11:11:11}', '{2011-11-11 11:11:11}', '{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}', '{1234.56768}', '{1234.5678}', '{"(,)"}', '{"(,)"}', '{"(,)"}', '{"(,)"}');

INSERT INTO large_text VALUES (REPEAT('x', 16 * 1024 * 1024), 'abc');

INSERT INTO trailing_space_pk VALUES ('klm   ');
UPDATE trailing_space_pk SET f1 = 'xyz   ' WHERE f1 = 'klm   ';
DELETE FROM trailing_space_pk WHERE f1 = 'abc   ';

INSERT INTO trailing_space_nopk VALUES ('klm   ');
UPDATE trailing_space_nopk SET f1 = 'xyz   ' WHERE f1 = 'klm   ';
DELETE FROM trailing_space_nopk WHERE f1 = 'abc   ';

INSERT INTO multipart_pk VALUES (2, 'klm', 'xyz');
DELETE FROM multipart_pk WHERE f1 = 1;

UPDATE nulls_table SET f2 = 3 WHERE f2 IS NULL;
INSERT INTO nulls_table VALUES (NULL, 1), (NULL, 2);
UPDATE nulls_table SET f2 = NULL WHERE f2 = 2;

INSERT INTO utf8_table VALUES ('това е текст 2', 'това ''е'' "текст" 2');
UPDATE utf8_table SET f1 = f1 || f1 , f2 = f2 || f2;
INSERT INTO "таблица" SELECT * FROM "таблица";
#
# Check the updated data on the Materialize side
#

> SELECT * FROM pk_table;
13 three
14 four
2 two_two
5 five

> SELECT * FROM nonpk_table;
103 3
103 3
11 1
2 2
2 2

> SELECT * FROM types_table;
"foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678" "(,)" "(,)" "(,)" "(,)"
"foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678" "empty" "empty" "empty" "empty"

> SELECT * FROM array_types_table;
"{2011-11-11}" "{11:11:11}" "{2011-11-11 11:11:11}" "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}" "{1234.56768}" "{1234.5678}" "{(,)}" "{(,)}" "{(,)}" "{(,)}"
"{2011-11-11}" "{11:11:11}" "{2011-11-11 11:11:11}" "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}" "{1234.56768}" "{1234.5678}" "{(,)}" "{(,)}" "{(,)}" "{(,)}"

> SELECT LENGTH(f1), LENGTH(f2) FROM large_text;
16777216 1048576
3145728  2097152
16777216 3

> SELECT LENGTH(f1), f1 FROM trailing_space_pk;
"6" "xyz   "

> SELECT LENGTH(f1), f1 FROM trailing_space_nopk;
"6" "xyz   "

> SELECT * FROM multipart_pk;
"2" "klm" "xyz"

> SELECT f1, f2, f1 IS NULL, f2 IS NULL FROM nulls_table;
"<null>" "1" "true" "false"
"<null>" "3" "true" "false"
"<null>" "<null>" "true" "true"

> SELECT * FROM utf8_table;
"това е текст 2това е текст 2" "това \'е\' \"текст\" 2това \'е\' \"текст\" 2"
"това е тексттова е текст" "това \'е\' \"текст\"това \'е\' \"текст\""

> SELECT * FROM "таблица";
стойност
стойност

> SELECT * FROM join_view;
"2" "two_two" "2" "2"
"2" "two_two" "2" "2"

#
# Check that the timestamps continue to be reasonable in the face of incoming updates
#

> SELECT COUNT(*) > 0 FROM pk_table;
true

> SELECT COUNT(*) > 0 FROM nonpk_table;
true

> SELECT COUNT(*) > 0 FROM join_view;
true

#
# Remove all data on the Postgres side
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
DELETE FROM pk_table;
DELETE FROM nonpk_table;
DELETE FROM large_text;
DELETE FROM trailing_space_pk;
DELETE FROM trailing_space_nopk;
DELETE FROM multipart_pk;
DELETE FROM nulls_table;
DELETE FROM utf8_table;
DELETE FROM "таблица";
DELETE FROM conflict_schema.conflict_table;

#
# Check that all data sources empty out on the Materialize side
#

> SELECT COUNT(*) FROM pk_table;
0

> SELECT COUNT(*) FROM nonpk_table;
0

> SELECT COUNT(*) FROM large_text;
0

> SELECT COUNT(*) FROM trailing_space_nopk;
0

> SELECT COUNT(*) FROM trailing_space_pk;
0

> SELECT COUNT(*) FROM multipart_pk;
0

> SELECT COUNT(*) FROM nulls_table;
0

> SELECT COUNT(*) FROM utf8_table;
0

> SELECT COUNT(*) FROM join_view;
0

> SELECT COUNT(*) FROM "таблица";
0

> SELECT COUNT(*) FROM conflict_table;
0

#
# Support enum values as strings
#
#
! CREATE SOURCE enum_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [pk_table.col_dne]
  )
  FOR TABLES (
    "enum_table"
  );
contains: invalid TEXT COLUMNS option value: column "pk_table.col_dne" does not exist

! CREATE SOURCE enum_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [table_dne.col_dne]
  )
  FOR TABLES (
    "enum_table"
  );
contains: invalid TEXT COLUMNS option value: table table_dne not found in source

# Reference exists in two schemas, so is not unambiguous
! CREATE SOURCE enum_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [another_enum_table."колона"]
  )
  FOR TABLES(
    conflict_schema.another_enum_table AS conflict_enum,
    public.another_enum_table AS public_enum
  );
contains: invalid TEXT COLUMNS option value: table another_enum_table is ambiguous, consider specifying the schema

! CREATE SOURCE enum_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [foo]
  )
  FOR SCHEMAS (public);
contains: invalid TEXT COLUMNS option value: column name 'foo' must have at least a table qualification

! CREATE SOURCE enum_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [foo.bar.qux.qax.foo]
  )
  FOR SCHEMAS (public);
contains: invalid TEXT COLUMNS option value: qualified name did not have between 1 and 3 components: foo.bar.qux.qax

! CREATE SOURCE enum_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [enum_table.a, enum_table.a]
  )
  FOR SCHEMAS (public);
contains: invalid TEXT COLUMNS option value: unexpected multiple references to postgres.public.enum_table.a

# utf8_table is not part of mz_source_narrow publication
! CREATE SOURCE enum_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source_narrow',
    TEXT COLUMNS [enum_table.a, utf8_table.f1]
  )
  FOR SCHEMAS (public);
contains: invalid TEXT COLUMNS option value: table utf8_table not found in source

# n.b includes a reference to pk_table, which is not a table that's part of the
# source, but is part of the publication.

> CREATE SOURCE enum_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [
      enum_table.a,
      public.another_enum_table."колона",
      pk_table.pk
    ]
  )
  FOR TABLES (
    "enum_table",
    public.another_enum_table
  );

> SELECT * FROM (SHOW SOURCES) WHERE name LIKE '%enum%';
another_enum_table      subsource <null>
enum_source             postgres  ${arg.default-storage-size}
enum_source_progress    progress  <null>
enum_table              subsource <null>

> SELECT * FROM enum_table
var0
var1

> SELECT "колона" FROM another_enum_table
var2
var3

#
# Cleanup
#
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION mz_source;
DROP PUBLICATION mz_source_narrow;
INSERT INTO pk_table VALUES (99999);

# todo: we're purifying Postgres view names incorrectly, fix that and this error!
# should be "materialize.public.pk_table"
! SELECT COUNT(*) = 0 FROM pk_table;
regex:Source error: .*: db error: ERROR: publication "mz_source" does not exist

> DROP SOURCE enum_source CASCADE;
> DROP SOURCE "mz_source" CASCADE;

#
# Check schema scoped tables

> CREATE SOURCE another_source
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'another_publication'
  )
  FOR SCHEMAS (
    another_schema
  );

> SHOW SOURCES
another_source          postgres  ${arg.default-storage-size}
another_source_progress progress  <null>
another_table           subsource <null>

> DROP SOURCE another_source

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA conflict_schema CASCADE;
