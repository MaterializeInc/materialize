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
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

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

CREATE TABLE types_table (char_col char(3), date_col DATE, time_col TIME, timestamp_col TIMESTAMP, uuid_col UUID, double_col DOUBLE PRECISION, numeric NUMERIC(8,4));
INSERT INTO types_table VALUES ('foo', '2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1234.56768, 1234.5678);
ALTER TABLE types_table REPLICA IDENTITY FULL;

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

#
# Test that slots created for replication sources are deleted on DROP
#
$ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=false

> CREATE SOURCE "test_slot_source"
  FROM POSTGRES
    CONNECTION pg
    PUBLICATION 'mz_source';

$ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=true

> DROP SOURCE test_slot_source

$ postgres-verify-slot connection=postgres://postgres:postgres@postgres slot=materialize_% active=false

#
# Error checking
#

> CREATE CONNECTION no_such_host
  FOR POSTGRES
  HOST 'no_such_postgres.mtrlz.com',
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass
! CREATE SOURCE "no_such_host"
  FROM POSTGRES CONNECTION no_such_host PUBLICATION 'mz_source';
contains:failed to lookup address information

> CREATE CONNECTION no_such_port
  FOR POSTGRES
  HOST postgres,
  PORT 65534,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass
! CREATE SOURCE "no_such_port"
  FROM POSTGRES CONNECTION no_such_port
  PUBLICATION 'mz_source';
contains:error connecting to server: Connection refused (os error 111)

> CREATE CONNECTION no_such_user
  FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER no_such_user,
  PASSWORD SECRET pgpass
! CREATE SOURCE "no_such_user"
  FROM POSTGRES CONNECTION no_such_user
  PUBLICATION 'mz_source';
contains:db error: FATAL: role "no_such_user" does not exist

> CREATE SECRET badpass AS 'badpass'
> CREATE CONNECTION no_such_password
  FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET badpass
! CREATE SOURCE "no_such_password"
  FROM POSTGRES CONNECTION no_such_password
  PUBLICATION 'mz_source';
contains:db error: FATAL: password authentication failed for user "postgres"

> CREATE CONNECTION no_such_dbname FOR POSTGRES
  HOST postgres,
  DATABASE no_such_dbname,
  USER postgres,
  PASSWORD SECRET pgpass
! CREATE SOURCE "no_such_dbname"
  FROM POSTGRES CONNECTION no_such_dbname
  PUBLICATION 'mz_source';
contains:db error: FATAL: database "no_such_dbname" does not exist

! CREATE SOURCE "no_such_publication"
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'no_such_publication';
contains:publication "no_such_publication" does not exist

> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE "mz_source" ("no_replica_identity")
# TODO: This should produce an error

#
# This succeeds on the Mz side but updates are prevented on the Postgres side
#

> SELECT * FROM no_replica_identity;
1
2

#
# Establish direct replication
#

> CREATE VIEWS FROM SOURCE "mz_source" ("pk_table");

> CREATE VIEWS FROM SOURCE "public"."mz_source" ("nonpk_table");

> CREATE VIEWS FROM SOURCE "materialize"."public"."mz_source" ("types_table");

> CREATE VIEWS FROM SOURCE mz_source ("large_text");

> CREATE VIEWS FROM SOURCE public.mz_source ("trailing_space_pk");

> CREATE VIEWS FROM SOURCE materialize.public.mz_source ("trailing_space_nopk");

> CREATE VIEWS FROM SOURCE public."mz_source" ("multipart_pk");

> CREATE VIEWS FROM SOURCE "materialize".public."mz_source" ("nulls_table");

> CREATE VIEWS FROM SOURCE "materialize"."public".mz_source ("utf8_table");

> CREATE VIEWS FROM SOURCE materialize."public".mz_source ("таблица");

> CREATE VIEWS FROM SOURCE mz_source ("escaped_text_table");

> CREATE VIEWS FROM SOURCE "mz_source" (conflict_schema.conflict_table AS public.conflict_table);

> CREATE VIEWS FROM SOURCE "mz_source" ("space table");

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
"foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678"

> SELECT pg_typeof(numeric) FROM types_table;
"numeric"

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

INSERT INTO types_table VALUES ('foo', '2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1234.56768, 1234.5678);

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
"foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678"
"foo" "2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768" "1234.5678"

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

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION mz_source;
INSERT INTO pk_table VALUES (99999);

# todo: we're purifying Postgres view names incorrectly, fix that and this error!
# should be "materialize.public.pk_table"
! SELECT COUNT(*) = 0 FROM pk_table;
regex:Source error: .*: db error: ERROR: publication "mz_source" does not exist

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA conflict_schema CASCADE;
