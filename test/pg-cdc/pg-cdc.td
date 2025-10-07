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

! CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    BROKER '${testdrive.kafka-addr}'
  )
contains:POSTGRES connections do not support BROKER values

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE CLUSTER cdc_cluster SIZE '${arg.default-replica-size}'

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

CREATE TABLE nonpk_table (f1 INTEGER, f2 INTEGER, f3 INTEGER GENERATED ALWAYS AS (f1 * 2) STORED);
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

CREATE TABLE tstzrange_table (a TSTZRANGE);
ALTER TABLE tstzrange_table REPLICA IDENTITY FULL;
INSERT INTO tstzrange_table VALUES ('["2024-02-13 17:01:58.37848+00!?!",)');

CREATE TABLE """literal_quotes""" (a TEXT);
ALTER TABLE """literal_quotes""" REPLICA IDENTITY FULL;
INSERT INTO """literal_quotes""" VALUES ('v');
ALTER TABLE """literal_quotes""" REPLICA IDENTITY FULL;

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
ALTER TABLE "space table" REPLICA IDENTITY FULL;

CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE enum_table (a an_enum);
INSERT INTO enum_table VALUES ('var1'), ('var0');
ALTER TABLE enum_table REPLICA IDENTITY FULL;

CREATE TYPE another_enum AS ENUM ('var2', 'var3');
CREATE TABLE another_enum_table ("колона" another_enum);
INSERT INTO another_enum_table VALUES ('var2'), ('var3');
ALTER TABLE another_enum_table REPLICA IDENTITY FULL;

CREATE TABLE conflict_schema.another_enum_table ("колона" another_enum);
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
# TODO: enable once we land database-issues#7327
# $ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=false

# Sneak in a test for pg_source_snapshot_statement_timeout, pg_source_wal_sender_timeout
$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
$ postgres-execute connection=mz_system
ALTER SYSTEM SET pg_source_snapshot_statement_timeout = 1000;
ALTER SYSTEM SET pg_source_wal_sender_timeout = 0;

> BEGIN
> CREATE SOURCE "test_slot_source"
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE pk_table FROM SOURCE "test_slot_source" (REFERENCE pk_table);
> COMMIT

# TODO: enable once we land database-issues#7327
# $ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=true

> SHOW SUBSOURCES ON test_slot_source

> DROP SOURCE test_slot_source CASCADE

# TODO: enable once we land database-issues#7327
# $ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=false

$ postgres-execute connection=mz_system
ALTER SYSTEM SET pg_source_snapshot_statement_timeout = 0

#
# Error checking
#

! CREATE CONNECTION no_such_host TO POSTGRES (
    HOST 'no_such_postgres.mtrlz.com',
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
contains:failed to lookup address information

! CREATE CONNECTION no_such_port TO POSTGRES (
    HOST postgres,
    PORT 65534,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
contains:error connecting to server: Connection refused

! CREATE CONNECTION no_such_user TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER no_such_user,
    PASSWORD SECRET pgpass
  )
contains:password authentication failed for user "no_such_user"

> CREATE SECRET badpass AS 'badpass'
! CREATE CONNECTION no_such_password TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET badpass
  )
contains:password authentication failed for user "postgres"

! CREATE CONNECTION no_such_dbname TO POSTGRES (
    HOST postgres,
    DATABASE no_such_dbname,
    USER postgres,
    PASSWORD SECRET pgpass
  )
contains:database "no_such_dbname" does not exist

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_enforce_external_addresses = true

! CREATE CONNECTION private_address TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
contains:Address resolved to a private IP

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_enforce_external_addresses = false

! CREATE SOURCE "no_such_publication"
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'no_such_publication');
# TODO: assert on `detail` here.
contains:failed to connect to PostgreSQL database

> CREATE SOURCE "mz_source"
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

! CREATE TABLE enum_table FROM SOURCE "mz_source" (REFERENCE enum_table);
contains:referenced tables use unsupported types

! CREATE TABLE another_enum_table FROM SOURCE "mz_source" (REFERENCE public.another_enum_table);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source_2
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source',
    DETAILS 'abc'
  );
contains: CREATE SOURCE specifies DETAILS option

! CREATE TABLE no_replica_identity FROM SOURCE "mz_source" (REFERENCE no_replica_identity);
contains:referenced items not tables with REPLICA IDENTITY FULL
detail:referenced items: public.no_replica_identity

#
# Establish direct replication
#
#
# Note: This implicitly tests that enum_table being part of the publication does not
# prevent us from using other tables as subsources.
#

> CREATE TABLE "pk_table" FROM SOURCE mz_source (REFERENCE "pk_table");
> CREATE TABLE "nonpk_table" FROM SOURCE mz_source (REFERENCE "nonpk_table");
> CREATE TABLE "types_table" FROM SOURCE mz_source (REFERENCE "types_table");
> CREATE TABLE "array_types_table" FROM SOURCE mz_source (REFERENCE "array_types_table");
> CREATE TABLE "large_text" FROM SOURCE mz_source (REFERENCE "large_text");
> CREATE TABLE "trailing_space_pk" FROM SOURCE mz_source (REFERENCE "trailing_space_pk");
> CREATE TABLE "trailing_space_nopk" FROM SOURCE mz_source (REFERENCE "trailing_space_nopk");
> CREATE TABLE "multipart_pk" FROM SOURCE mz_source (REFERENCE "multipart_pk");
> CREATE TABLE "nulls_table" FROM SOURCE mz_source (REFERENCE "nulls_table");
> CREATE TABLE "utf8_table" FROM SOURCE mz_source (REFERENCE "utf8_table");
> CREATE TABLE "таблица" FROM SOURCE mz_source (REFERENCE "таблица");
> CREATE TABLE "escaped_text_table" FROM SOURCE mz_source (REFERENCE "escaped_text_table");
> CREATE TABLE conflict_table FROM SOURCE mz_source (REFERENCE conflict_schema.conflict_table);
> CREATE TABLE "space table" FROM SOURCE mz_source (REFERENCE "space table");
> CREATE TABLE """literal_quotes""" FROM SOURCE mz_source (REFERENCE """literal_quotes""");
> CREATE TABLE "create" FROM SOURCE mz_source (REFERENCE "create");
> CREATE TABLE tstzrange_table FROM SOURCE mz_source (REFERENCE "tstzrange_table");

> SHOW SOURCES
mz_source             postgres   cdc_cluster  ""

> SHOW TABLES
array_types_table     ""
conflict_table        ""
create                ""
escaped_text_table    ""
large_text            ""
multipart_pk          ""
nonpk_table           ""
nulls_table           ""
pk_table              ""
"space table"         ""
trailing_space_nopk   ""
trailing_space_pk     ""
"\"literal_quotes\""  ""
tstzrange_table       ""
types_table           ""
utf8_table            ""
таблица               ""

> SELECT schema_name, table_name FROM mz_internal.mz_postgres_source_tables
public          create
public          pk_table
public          large_text
public          utf8_table
public          nonpk_table
public          types_table
public          nulls_table
public          multipart_pk
public          "\"space table\""
public          tstzrange_table
public          "\"таблица\""
public          array_types_table
public          trailing_space_pk
public          escaped_text_table
public          trailing_space_nopk
public          "\"\"\"literal_quotes\"\"\""
conflict_schema conflict_table

# Ensure all ingestion export subsources have an ID greater than the primary source ID
> SELECT bool_and(primary_source_id < subsource_id)
  FROM
      (SELECT id AS primary_source_id FROM mz_sources WHERE type = 'postgres')
          CROSS JOIN (SELECT id AS subsource_id FROM mz_tables WHERE source_id IS NOT NULL);
true

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running

> SELECT lsn > 0 FROM mz_source
true

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

> SELECT * FROM tstzrange_table
"[2024-02-13 17:01:58.378480 UTC,)"

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
# Ensure we can start a source with more workers than the default max_wal_senders param (10)
#

> CREATE CLUSTER large_cluster SIZE 'scale=1,workers=16';

> CREATE SOURCE large_cluster_source
  IN CLUSTER large_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE large_cluster_source_pk_table
  FROM SOURCE large_cluster_source
  (REFERENCE "pk_table");

> SELECT * FROM large_cluster_source_pk_table;
13 three
14 four
2 two_two
5 five

> SELECT status = 'running' FROM mz_internal.mz_source_statuses WHERE name = 'large_cluster_source_pk_table' AND type = 'table';
true

> DROP SOURCE large_cluster_source CASCADE;

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
DELETE FROM tstzrange_table;

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

> SELECT COUNT(*) FROM tstzrange_table;
0

#
# Support enum values as strings
#
#
> CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

! CREATE TABLE "enum_table"
  FROM SOURCE enum_source
  (REFERENCE "enum_table")
  WITH (TEXT COLUMNS [col_dne]);
contains: invalid TEXT COLUMNS option value: column "enum_table.col_dne" does not exist

! CREATE TABLE "enum_table"
  FROM SOURCE enum_source
  (REFERENCE "enum_table")
  WITH (TEXT COLUMNS ["F2"]);
contains: invalid TEXT COLUMNS option value: column "enum_table.F2" does not exist

! CREATE TABLE "enum_table"
  FROM SOURCE enum_source
  (REFERENCE "enum_table")
  WITH (TEXT COLUMNS [col_dne]);
contains: invalid TEXT COLUMNS option value: column "enum_table.col_dne" does not exist

! CREATE TABLE pk_table
  FROM SOURCE enum_source
  (REFERENCE pk_table)
  WITH (TEXT COLUMNS [foo]);
contains: invalid TEXT COLUMNS option value: column "pk_table.foo" does not exist

! CREATE TABLE pk_table
  FROM SOURCE enum_source
  (REFERENCE pk_table)
  WITH (TEXT COLUMNS [foo]);
contains: invalid TEXT COLUMNS option value: column "pk_table.foo" does not exist

! CREATE TABLE enum_table
  FROM SOURCE enum_source
  (REFERENCE enum_table)
  WITH (TEXT COLUMNS [a, a]);
contains: invalid TEXT COLUMNS option value: unexpected multiple references to postgres.public.enum_table.a

# utf8_table is not part of mz_source_narrow publication
! CREATE TABLE enum_table
  FROM SOURCE enum_source
  (REFERENCE enum_table)
  WITH (TEXT COLUMNS [a, f1]);
contains: invalid TEXT COLUMNS option value: column "enum_table.f1" does not exist

# n.b includes a reference to pk_table, which is not a table that's part of the
# source, but is part of the publication.

! CREATE TABLE enum_table
  FROM SOURCE enum_source
  (REFERENCE enum_table)
  WITH (TEXT COLUMNS [a, "колона", pk]);
contains:invalid TEXT COLUMNS option value: column "enum_table.колона" does not exist

> CREATE TABLE enum_table
  FROM SOURCE enum_source
  (REFERENCE enum_table)
  WITH (TEXT COLUMNS [a]);

> CREATE TABLE another_enum_table
  FROM SOURCE enum_source
  (REFERENCE public.another_enum_table)
  WITH (TEXT COLUMNS ["колона"]);

> SELECT * FROM (SHOW SOURCES) WHERE name LIKE '%enum%';
enum_source             postgres  cdc_cluster  ""

> SELECT * FROM (SHOW TABLES) WHERE name LIKE '%enum%';
another_enum_table      ""
enum_table              ""

> SELECT * FROM enum_table
var0
var1

$ set-regex match="DETAILS = '[a-f0-9]+'" replacement=<DETAILS>

> SHOW CREATE TABLE enum_table
materialize.public.enum_table "CREATE TABLE materialize.public.enum_table (a pg_catalog.text) FROM SOURCE materialize.public.enum_source (REFERENCE = postgres.public.enum_table) WITH (TEXT COLUMNS = (a), <DETAILS>);"

# Test that TEXT COLUMN types can change
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

# Ensure that source + all subsources have error
> SELECT bool_and(error ~* 'publication .+ does not exist')
  FROM mz_internal.mz_source_statuses
  WHERE id IN ( SELECT id FROM mz_sources );
true

> DROP SOURCE enum_source CASCADE;
> DROP SOURCE "mz_source" CASCADE;

#
# Check schema scoped tables

> BEGIN
> CREATE SOURCE another_source
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'another_publication'
  );

> CREATE TABLE another_table FROM SOURCE another_source (REFERENCE another_schema.another_table);
> COMMIT

> SHOW SOURCES
another_source          postgres  cdc_cluster  ""

> SHOW TABLES
another_table           ""

> DROP SOURCE another_source CASCADE

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA conflict_schema CASCADE;
