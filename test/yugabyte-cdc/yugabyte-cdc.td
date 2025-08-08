# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_yugabyte_connection = true;

> CREATE SECRET pgpass AS 'yugabyte'

> CREATE CONNECTION yb TO YUGABYTE (
    HOST yugabyte,
    PORT 5433,
    DATABASE yugabyte,
    USER yugabyte,
    PASSWORD SECRET pgpass
  )

> CREATE CLUSTER cdc_cluster SIZE '${arg.default-replica-size}'

$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
ALTER USER yugabyte WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (2, 'two');
INSERT INTO pk_table VALUES (3, 'three');

CREATE TABLE types_table (pk INTEGER PRIMARY KEY, char_col char(3), date_col DATE, time_col TIME, timestamp_col TIMESTAMP, uuid_col UUID, double_col DOUBLE PRECISION, numeric NUMERIC(8,4), int4range_col INT4RANGE, int8range_col INT8RANGE, daterange_col DATERANGE, numrange_col NUMRANGE);
INSERT INTO types_table VALUES (1, 'foo', '2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1234.56768, 1234.5678, '(,)', '(,)', '(,)', '(,)');
ALTER TABLE types_table REPLICA IDENTITY FULL;

CREATE TABLE array_types_table (pk INTEGER PRIMARY KEY, date_col DATE[], time_col TIME[], timestamp_col TIMESTAMP[], uuid_col UUID[], double_col DOUBLE PRECISION[], numeric NUMERIC[], int4range_col INT4RANGE[], int8range_col INT8RANGE[], daterange_col DATERANGE[], numrange_col NUMRANGE[]);
INSERT INTO array_types_table VALUES (1, '{2011-11-11}', '{11:11:11}', '{2011-11-11 11:11:11}', '{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}', '{1234.56768}', '{1234.5678}', '{"(,)"}', '{"(,)"}', '{"(,)"}', '{"(,)"}');
ALTER TABLE array_types_table REPLICA IDENTITY FULL;

CREATE TABLE large_text (pk INTEGER PRIMARY KEY, f1 TEXT, f2 TEXT);
INSERT INTO large_text VALUES (1, REPEAT('x', 16 * 1024 * 1024), REPEAT('y', 1 * 1024 * 1024));
INSERT INTO large_text VALUES (2, REPEAT('a', 3 * 1024 * 1024),  REPEAT('b', 2 * 1024 * 1024));
ALTER TABLE large_text REPLICA IDENTITY FULL;

CREATE TABLE trailing_space_pk (f1 TEXT PRIMARY KEY);
INSERT INTO trailing_space_pk VALUES ('abc   ');
ALTER TABLE trailing_space_pk REPLICA IDENTITY FULL;

CREATE TABLE multipart_pk(f1 INTEGER, f2 TEXT, f3 TEXT, PRIMARY KEY (f1, f2));
INSERT INTO multipart_pk VALUES (1, 'abc', 'xyz');
ALTER TABLE multipart_pk REPLICA IDENTITY FULL;

CREATE TABLE nulls_table (pk INTEGER PRIMARY KEY, f2 TEXT, f3 INTEGER);
INSERT INTO nulls_table VALUES (1, NULL, NULL);
ALTER TABLE nulls_table REPLICA IDENTITY FULL;

CREATE TABLE utf8_table (f1 TEXT PRIMARY KEY, f2 TEXT);
INSERT INTO utf8_table VALUES ('това е текст', 'това ''е'' "текст"');
ALTER TABLE utf8_table REPLICA IDENTITY FULL;

CREATE TABLE "таблица" ("колона" TEXT PRIMARY KEY);
ALTER TABLE "таблица" REPLICA IDENTITY FULL;
INSERT INTO "таблица" VALUES ('стойност');

CREATE TABLE tstzrange_table (pk INTEGER PRIMARY KEY, a TSTZRANGE);
ALTER TABLE tstzrange_table REPLICA IDENTITY FULL;
INSERT INTO tstzrange_table VALUES (1, '["2024-02-13 17:01:58.37848+00!?!",)');

CREATE TABLE """literal_quotes""" (a TEXT PRIMARY KEY);
ALTER TABLE """literal_quotes""" REPLICA IDENTITY FULL;
INSERT INTO """literal_quotes""" VALUES ('v');
ALTER TABLE """literal_quotes""" REPLICA IDENTITY FULL;

CREATE TABLE "create" (a TEXT PRIMARY KEY);
ALTER TABLE "create" REPLICA IDENTITY FULL;
INSERT INTO "create" VALUES ('v');

CREATE TABLE escaped_text_table (pk INTEGER PRIMARY KEY, f1 TEXT, f2 TEXt);
ALTER TABLE escaped_text_table REPLICA IDENTITY FULL;
INSERT INTO escaped_text_table VALUES (1, 'escaped\ntext\twith\nnewlines\tand\ntabs', 'more\tescaped\ntext');
INSERT INTO escaped_text_table VALUES (2, 'second\nrow\twith\tmore\ttabs', 'and\nmore\n\nnewlines\n');

CREATE TABLE conflict_table (f1 INTEGER PRIMARY KEY);
ALTER TABLE conflict_table REPLICA IDENTITY FULL;
INSERT INTO conflict_table VALUES (123);

DROP SCHEMA IF EXISTS conflict_schema CASCADE;
CREATE SCHEMA conflict_schema;
CREATE TABLE conflict_schema.conflict_table (f1 TEXT PRIMARY KEY);
ALTER TABLE conflict_schema.conflict_table REPLICA IDENTITY FULL;
INSERT INTO conflict_schema.conflict_table VALUES ('234');

CREATE TABLE "space table" ("space column" INTEGER PRIMARY KEY);
ALTER TABLE "space table" REPLICA IDENTITY FULL;

CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE enum_table (pk INTEGER PRIMARY KEY, a an_enum);
INSERT INTO enum_table VALUES (1, 'var0'), (2, 'var1');
ALTER TABLE enum_table REPLICA IDENTITY FULL;

CREATE TYPE another_enum AS ENUM ('var2', 'var3');
CREATE TABLE another_enum_table (pk INTEGER PRIMARY KEY, "колона" another_enum);
INSERT INTO another_enum_table VALUES (1, 'var2'), (2, 'var3');
ALTER TABLE another_enum_table REPLICA IDENTITY FULL;

CREATE TABLE conflict_schema.another_enum_table (pk INTEGER PRIMARY KEY, "колона" another_enum);
INSERT INTO conflict_schema.another_enum_table VALUES (1, 'var2'), (2, 'var3');
ALTER TABLE conflict_schema.another_enum_table REPLICA IDENTITY FULL;

DROP PUBLICATION IF EXISTS mz_source_narrow;

CREATE PUBLICATION mz_source_narrow FOR TABLE enum_table, public.another_enum_table, pk_table;

DROP SCHEMA IF EXISTS another_schema CASCADE;
CREATE SCHEMA another_schema;
CREATE TABLE another_schema.another_table (f1 TEXT PRIMARY KEY);
ALTER TABLE another_schema.another_table REPLICA IDENTITY FULL;
INSERT INTO another_schema.another_table VALUES ('123');

DROP PUBLICATION IF EXISTS another_publication;

CREATE PUBLICATION another_publication FOR TABLE another_schema.another_table;

#
# Error checking
#

! CREATE CONNECTION no_such_host TO YUGABYTE (
    HOST 'no_such_postgres.mtrlz.com',
    PORT 5433,
    DATABASE yugabyte,
    USER yugabyte,
    PASSWORD SECRET pgpass
  )
contains:failed to lookup address information

! CREATE CONNECTION no_such_port TO YUGABYTE (
    HOST yugabyte,
    PORT 65534,
    DATABASE yugabyte,
    USER yugabyte,
    PASSWORD SECRET pgpass
  )
contains:error connecting to server: Connection refused

! CREATE CONNECTION no_such_user TO YUGABYTE (
    HOST yugabyte,
    PORT 5433,
    DATABASE yugabyte,
    USER no_such_user,
    PASSWORD SECRET pgpass
  )
contains:role "no_such_user" does not exist

! CREATE CONNECTION no_such_dbname TO YUGABYTE (
    HOST yugabyte,
    PORT 5433,
    DATABASE no_such_dbname,
    USER yugabyte,
    PASSWORD SECRET pgpass
  )
contains:database "no_such_dbname" does not exist

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_enforce_external_addresses = true

! CREATE CONNECTION private_address TO YUGABYTE (
    HOST yugabyte,
    PORT 5433,
    DATABASE yugabyte,
    USER yugabyte,
    PASSWORD SECRET pgpass
  )
contains:Address resolved to a private IP

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_enforce_external_addresses = false

! CREATE SOURCE "no_such_publication"
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (PUBLICATION 'no_such_publication');
# TODO: assert on `detail` here.
contains:failed to connect to PostgreSQL database

! CREATE SOURCE "mz_source"
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (PUBLICATION 'mz_source')
  FOR TABLES (
    "enum_table"
  );
contains:referenced tables use unsupported types

! CREATE SOURCE "mz_source"
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (PUBLICATION 'mz_source')
  FOR TABLES (
    "enum_table",
    public.another_enum_table
  );
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    DETAILS 'abc'
  )
  FOR TABLES (
    pk_table
  );
contains: CREATE SOURCE specifies DETAILS option

! CREATE SOURCE mz_source
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION yb (PUBLICATION 'mz_source')
  FOR ALL TABLES;
contains:materialize.public.yb is not a POSTGRES CONNECTION

#
# Establish direct replication
#
#
# Note: This implicitly tests that enum_table being part of the publication does not
# prevent us from using other tables as subsources.
#
> CREATE SOURCE "mz_source"
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (PUBLICATION 'mz_source')
  FOR TABLES (
    "pk_table",
    "types_table",
    "array_types_table",
    "large_text",
    "trailing_space_pk",
    "multipart_pk",
    "nulls_table",
    "utf8_table",
    "таблица",
    "escaped_text_table",
    conflict_schema.conflict_table AS public.conflict_table,
    "space table",
    """literal_quotes""",
    "create",
    tstzrange_table
  );


! ALTER SOURCE mz_source ADD SUBSOURCE foobar;
contains:source mz_source does not support ALTER SOURCE.

> SHOW SOURCES
array_types_table     subsource  cdc_cluster  ""
conflict_table        subsource  cdc_cluster  ""
create                subsource  cdc_cluster  ""
escaped_text_table    subsource  cdc_cluster  ""
large_text            subsource  cdc_cluster  ""
multipart_pk          subsource  cdc_cluster  ""
mz_source             postgres   cdc_cluster  ""
mz_source_progress    progress   <null>       ""
nulls_table           subsource  cdc_cluster  ""
pk_table              subsource  cdc_cluster  ""
"space table"         subsource  cdc_cluster  ""
trailing_space_pk     subsource  cdc_cluster  ""
"\"literal_quotes\""  subsource  cdc_cluster  ""
tstzrange_table       subsource  cdc_cluster  ""
types_table           subsource  cdc_cluster  ""
utf8_table            subsource  cdc_cluster  ""
таблица               subsource  cdc_cluster  ""

> SELECT schema_name, table_name FROM mz_internal.mz_postgres_source_tables
public          create
public          pk_table
public          large_text
public          utf8_table
public          types_table
public          nulls_table
public          multipart_pk
public          "\"space table\""
public          tstzrange_table
public          "\"таблица\""
public          array_types_table
public          trailing_space_pk
public          escaped_text_table
public          "\"\"\"literal_quotes\"\"\""
conflict_schema conflict_table

# Ensure all ingestion export subsources have an ID greater than the primary source ID
> SELECT bool_and(primary_source_id < subsource_id)
  FROM
      (SELECT id AS primary_source_id FROM mz_sources WHERE type = 'postgres')
          CROSS JOIN (SELECT id AS subsource_id FROM mz_sources WHERE type = 'subsource');
true

# Ensure progress subsources have an ID less than the primary source ID
> SELECT progress_source_id < primary_source_id
    FROM (
        SELECT
            (SELECT id FROM mz_sources WHERE type = 'postgres') AS primary_source_id,
            (SELECT id FROM mz_sources WHERE type = 'progress') AS progress_source_id
    );
true

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source_progress';
running

> SELECT lsn > 0 FROM mz_source_progress
true

# Ensure we report the write frontier of the progress subsource
$ set-regex match=(\s{12}0|\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d\.\d\d\d\)|true|false) replacement=<>
> EXPLAIN TIMESTAMP FOR SELECT * FROM mz_source_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.mz_source_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n\nbinding constraints:\nlower:\n  (IsolationLevel(StrictSerializable)): [<> <>]\n"

$ set-regex match=[0-9]+|_[a-f0-9]+ replacement=<SUPPRESSED>

> SELECT * FROM mz_internal.mz_postgres_sources
id             replication_slot         timeline_id
---------------------------------------------------
u<SUPPRESSED>  materialize<SUPPRESSED>  <SUPPRESSED>

$ unset-regex

#
# Perform sanity checks of the initial snapshot
#

> SELECT * FROM pk_table;
1 one
2 two
3 three

> SELECT * FROM types_table;
1 "foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678" "(,)" "(,)" "(,)" "(,)"

> SELECT pg_typeof(numeric) FROM types_table;
"numeric"

> SELECT * FROM array_types_table;
1 "{2011-11-11}" "{11:11:11}" "{2011-11-11 11:11:11}" "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}" "{1234.56768}" "{1234.5678}" "{(,)}" "{(,)}" "{(,)}" "{(,)}"

> SELECT LENGTH(f1), LENGTH(f2) FROM large_text;
16777216 1048576
3145728  2097152

> SELECT LENGTH(f1), f1 FROM trailing_space_pk;
"6" "abc   "

> SELECT * FROM multipart_pk;
1 abc xyz

> SELECT f2, f3, f2 IS NULL, f3 IS NULL FROM nulls_table;
<null> <null> true true

> SELECT * FROM utf8_table;
"това е текст" "това \'е\' \"текст\""

> SELECT * FROM "таблица";
стойност

> SELECT * FROM escaped_text_table;
1 "escaped\\ntext\\twith\\nnewlines\\tand\\ntabs" "more\\tescaped\\ntext"
2 "second\\nrow\\twith\\tmore\\ttabs" "and\\nmore\\n\\nnewlines\\n"

> SELECT * FROM conflict_table;
234

> SELECT * FROM """literal_quotes"""
v

> SELECT * FROM "create"
v

> SELECT * FROM tstzrange_table
1 "[2024-02-13 17:01:58.378480 UTC,)"

#
# Basic sanity check that the timestamps are reasonable
#

> SELECT COUNT(*) > 0 FROM pk_table;
true

#
# Modify the tables on the Yugabyte side
#

$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
INSERT INTO pk_table VALUES (4, 'four');
INSERT INTO pk_table VALUES (5, 'five');
DELETE FROM pk_table WHERE pk = 1;
UPDATE pk_table SET f2 = 'two_two' WHERE pk = 2;
UPDATE pk_table SET pk = pk + 10 WHERE pk BETWEEN 3 AND 4;

INSERT INTO types_table VALUES (2, 'foo', '2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1234.56768, 1234.5678, 'empty', 'empty', 'empty', 'empty');

INSERT INTO array_types_table VALUES (2, '{2011-11-11}', '{11:11:11}', '{2011-11-11 11:11:11}', '{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}', '{1234.56768}', '{1234.5678}', '{"(,)"}', '{"(,)"}', '{"(,)"}', '{"(,)"}');

INSERT INTO large_text VALUES (3, REPEAT('x', 16 * 1024 * 1024), 'abc');

INSERT INTO trailing_space_pk VALUES ('klm   ');
UPDATE trailing_space_pk SET f1 = 'xyz   ' WHERE f1 = 'klm   ';
DELETE FROM trailing_space_pk WHERE f1 = 'abc   ';

INSERT INTO multipart_pk VALUES (2, 'klm', 'xyz');
DELETE FROM multipart_pk WHERE f1 = 1;

UPDATE nulls_table SET f3 = 3 WHERE f3 IS NULL;
INSERT INTO nulls_table VALUES (2, NULL, 1), (3, NULL, 2);
UPDATE nulls_table SET f3 = NULL WHERE f3 = 2;

INSERT INTO utf8_table VALUES ('това е текст 2', 'това ''е'' "текст" 2');
UPDATE utf8_table SET f1 = f1 || f1 , f2 = f2 || f2;

INSERT INTO "таблица" VALUES ('наздраве');

#
# Check the updated data on the Materialize side
#

> SELECT * FROM pk_table;
13 three
14 four
2 two_two
5 five

> SELECT * FROM types_table;
1 "foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678" "(,)" "(,)" "(,)" "(,)"
2 "foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678" "empty" "empty" "empty" "empty"

> SELECT * FROM array_types_table;
1 "{2011-11-11}" "{11:11:11}" "{2011-11-11 11:11:11}" "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}" "{1234.56768}" "{1234.5678}" "{(,)}" "{(,)}" "{(,)}" "{(,)}"
2 "{2011-11-11}" "{11:11:11}" "{2011-11-11 11:11:11}" "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}" "{1234.56768}" "{1234.5678}" "{(,)}" "{(,)}" "{(,)}" "{(,)}"

> SELECT LENGTH(f1), LENGTH(f2) FROM large_text;
16777216 1048576
3145728  2097152
16777216 3

> SELECT LENGTH(f1), f1 FROM trailing_space_pk;
"6" "xyz   "

> SELECT * FROM multipart_pk;
"2" "klm" "xyz"

> SELECT f2, f3, f2 IS NULL, f3 IS NULL FROM nulls_table;
"<null>" "1" "true" "false"
"<null>" "3" "true" "false"
"<null>" "<null>" "true" "true"

> SELECT * FROM utf8_table;
"това е текст 2това е текст 2" "това \'е\' \"текст\" 2това \'е\' \"текст\" 2"
"това е тексттова е текст" "това \'е\' \"текст\"това \'е\' \"текст\""

> SELECT * FROM "таблица";
стойност
наздраве

#
# Check that the timestamps continue to be reasonable in the face of incoming updates
#

> SELECT COUNT(*) > 0 FROM pk_table;
true

#
# Ensure we can start a source with more workers than the default max_wal_senders param (10)
#

> CREATE CLUSTER large_cluster SIZE 'scale=1,workers=16';

> CREATE SOURCE large_cluster_source
  IN CLUSTER large_cluster
  FROM YUGABYTE CONNECTION yb (PUBLICATION 'mz_source')
  FOR TABLES ("pk_table" AS large_cluster_source_pk_table);

> SELECT * FROM large_cluster_source_pk_table;
13 three
14 four
2 two_two
5 five

> SELECT status = 'running' FROM mz_internal.mz_source_statuses WHERE name = 'large_cluster_source_pk_table';
true

> DROP SOURCE large_cluster_source CASCADE;

#
# Remove all data on the Yugabyte side
#

$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
DELETE FROM pk_table;
DELETE FROM large_text;
DELETE FROM trailing_space_pk;
DELETE FROM multipart_pk;
DELETE FROM nulls_table;
DELETE FROM utf8_table;
DELETE FROM "таблица";
DELETE FROM conflict_schema.conflict_table;
DELETE FROM tstzrange_table;

#
# Check that all data sources empty out on the Materialize side
#

> SELECT COUNT(*) FROM pk_table;
0

> SELECT COUNT(*) FROM large_text;
0

> SELECT COUNT(*) FROM trailing_space_pk;
0

> SELECT COUNT(*) FROM multipart_pk;
0

> SELECT COUNT(*) FROM nulls_table;
0

> SELECT COUNT(*) FROM utf8_table;
0

> SELECT COUNT(*) FROM "таблица";
0

> SELECT COUNT(*) FROM conflict_table;
0

> SELECT COUNT(*) FROM tstzrange_table;
0

#
# Support enum values as strings
#
#
! CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [pk_table.col_dne]
  )
  FOR TABLES (
    "enum_table"
  );
contains: invalid TEXT COLUMNS option value: column "pk_table.col_dne" does not exist

! CREATE SOURCE case_sensitive_names
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [pk_table."F2"]
  )
  FOR TABLES (
    "enum_table"
  );
contains: invalid TEXT COLUMNS option value: column "pk_table.F2" does not exist
hint: The similarly named column "pk_table.f2" does exist.

! CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [table_dne.col_dne]
  )
  FOR TABLES (
    "enum_table"
  );
contains: reference to table_dne not found in source

# Reference exists in two schemas, so is not unambiguous
! CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [another_enum_table."колона"]
  )
  FOR TABLES(
    conflict_schema.another_enum_table AS conflict_enum,
    public.another_enum_table AS public_enum
  );
contains: reference another_enum_table is ambiguous, consider specifying an additional layer of qualification

! CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [foo]
  )
  FOR TABLES (pk_table);
contains: column name 'foo' must have at least a table qualification

! CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [foo.bar.qux.qax.foo]
  )
  FOR TABLES (pk_table);
contains: reference to foo.bar.qux.qax not found in source

! CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [enum_table.a, enum_table.a]
  )
  FOR TABLES (enum_table);
contains: unexpected multiple references to yugabyte.public.enum_table.a

# utf8_table is not part of mz_source_narrow publication
! CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source_narrow',
    TEXT COLUMNS [enum_table.a, utf8_table.f1]
  )
  FOR TABLES (enum_table);
contains: reference to utf8_table not found in source

# n.b includes a reference to pk_table, which is not a table that's part of the
# source, but is part of the publication.

! CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
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
contains:TEXT COLUMNS refers to table not currently being added

> CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'mz_source',
    TEXT COLUMNS [
      enum_table.a,
      public.another_enum_table."колона"
    ]
  )
  FOR TABLES (
    "enum_table",
    public.another_enum_table
  );

> SELECT * FROM (SHOW SOURCES) WHERE name LIKE '%enum%';
another_enum_table      subsource cdc_cluster  ""
enum_source             postgres  cdc_cluster  ""
enum_source_progress    progress  <null>       ""
enum_table              subsource cdc_cluster  ""

> SELECT * FROM enum_table
1 var0
2 var1

> SHOW CREATE SOURCE enum_table
materialize.public.enum_table "CREATE SUBSOURCE materialize.public.enum_table (pk pg_catalog.int4 NOT NULL, a pg_catalog.text, CONSTRAINT enum_table_pkey PRIMARY KEY (pk)) OF SOURCE materialize.public.enum_source WITH (EXTERNAL REFERENCE = yugabyte.public.enum_table, TEXT COLUMNS = (a));"

> SELECT "колона" FROM another_enum_table
var2
var3

#
# Cleanup
#
#

> DROP SOURCE enum_source CASCADE;
> DROP SOURCE "mz_source" CASCADE;

#
# Check schema scoped tables

> CREATE SOURCE another_source
  IN CLUSTER cdc_cluster
  FROM YUGABYTE CONNECTION yb (
    PUBLICATION 'another_publication'
  )
  FOR SCHEMAS (
    another_schema
  );

> SHOW SOURCES
another_source          postgres  cdc_cluster  ""
another_source_progress progress  <null>       ""
another_table           subsource cdc_cluster  ""

> DROP SOURCE another_source CASCADE

$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
DROP SCHEMA conflict_schema CASCADE;
