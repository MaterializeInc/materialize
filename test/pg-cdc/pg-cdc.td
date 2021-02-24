# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP USER IF EXISTS no_replication;
CREATE USER no_replication WITH PASSWORD 'no_replication';
ALTER USER no_replication WITH LOGIN;

ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

CREATE TABLE numbers (number int PRIMARY KEY, is_prime bool, name text);
ALTER TABLE numbers REPLICA IDENTITY FULL;

CREATE SCHEMA other_schema;
CREATE TABLE other_schema.numbers (number int PRIMARY KEY, is_prime bool NOT NULL, name text NOT NULL);

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;
INSERT INTO numbers VALUES (5, true, 'five');

CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (2, 'two');

INSERT INTO pk_table VALUES (3, 'three');

CREATE TABLE nonpk_table (f1 INTEGER, f2 INTEGER);
INSERT INTO nonpk_table VALUES (1, 1), (1, 1);
ALTER TABLE nonpk_table REPLICA IDENTITY FULL;
INSERT INTO nonpk_table VALUES (2, 2), (2, 2);

CREATE TABLE types_table (date_col DATE, time_col TIME, timestamp_col TIMESTAMP, uuid_col UUID, double_col DOUBLE PRECISION);
INSERT INTO types_table VALUES ('2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1234.56768);
ALTER TABLE types_table REPLICA IDENTITY FULL;

CREATE TABLE large_text (f1 TEXT);
INSERT INTO large_text VALUES (REPEAT('x', 16 * 1024 * 1024));
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

## CREATE SOURCE with correct information should pass purification and fail afterwards.

> CREATE SOURCE "schema_omitted"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers';

> CREATE SOURCE "empty_schema2"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers' ();
# TODO: This should be an error

> CREATE SOURCE "correct_schema_explicit_null"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers' (number int NOT NULL, is_prime bool NULL, name text NULL);

> CREATE MATERIALIZED SOURCE "correct_schema"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers'(number int NOT NULL, is_prime bool, name text)

## CREATE SOURCE with incorrect information should fail purification.

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers' (number int NOT NULL)
incorrect column specification: 1 columns were specified, upstream has 3: number, is_prime, name

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers' (number int NOT NULL, is_prime bool null, name text, extra numeric NULL)
incorrect column specification: 4 columns were specified, upstream has 3: number, is_prime, name

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers' (number int NOT NULL, is_prime int, name text)
incorrect column specification: specified column does not match upstream source, specified: is_prime int4 NULL, upstream: is_prime bool NULL

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers' (number int NULL, is_prime bool null, name text)
incorrect column specification: specified column does not match upstream source, specified: number int4 NULL, upstream: number int4 NOT NULL

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLE 'numbers' (number int NOT NULL, is_prime bool, name text NOT NULL)
incorrect column specification: specified column does not match upstream source, specified: name text NOT NULL, upstream: name text NULL

## CREATE SOURCES with correct information should pass purification and fail afterwards.

! CREATE SOURCES
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLES ('numbers' AS "numbers")
CREATE SOURCES not yet supported

! CREATE SOURCES
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLES ('numbers' AS "numbers", 'numbers' AS "second_numbers" (number int NOT NULL, is_prime bool NULL, name text NULL))
CREATE SOURCES not yet supported

## CREATE SOURCES with incorrect information should fail purification.

! CREATE SOURCES
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLES ('numbers' AS "numbers" (number int NULL))
incorrect column specification: 1 columns were specified, upstream has 3: number, is_prime, name

! CREATE SOURCES
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source'
    NAMESPACE 'public'
    TABLES ('numbers' AS "numbers" (number int NULL, is_prime bool NULL, name text))
incorrect column specification: specified column does not match upstream source, specified: number int4 NULL, upstream: number int4 NOT NULL

#
# Error checking
#

! CREATE SOURCE "no_such_host"
  FROM POSTGRES HOST 'host=no_such_postgres.mtrlz.com port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);
error connecting to server: failed to lookup address information: Name or service not known

! CREATE SOURCE "no_such_port"
  FROM POSTGRES HOST 'host=postgres port=65534 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);
error connecting to server: Connection refused (os error 111)

! CREATE SOURCE "no_such_user"
  FROM POSTGRES HOST 'host=postgres port=5432 user=no_such_user password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);
db error: FATAL: role "no_such_user" does not exist

> CREATE SOURCE "no_such_password"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=no_such_password dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);
# TODO: This should produce an error

> CREATE SOURCE "no_replication"
  FROM POSTGRES HOST 'host=postgres port=5432 user=no_replication password=no_replication dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);
# TODO: This should produce an error

! CREATE SOURCE "no_such_dbname"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=no_such_dbname'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);
db error: FATAL: database "no_such_dbname" does not exist

> CREATE SOURCE "no_such_publication"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'no_such_publication' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);
# TODO: This should produce an error

! CREATE SOURCE "no_such_namespace"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'no_such_namespace' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);
table not found in the upstream catalog

! CREATE SOURCE "no_such_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'no_such_table' (pk INTEGER, f2 TEXT);
table not found in the upstream catalog

! CREATE SOURCE "no_such_column"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (no_such_column INTEGER, f2 TEXT);
column does not match upstream source, specified: no_such_column int4 NULL, upstream: pk int4 NOT NULL

! CREATE SOURCE "column_type_mismatch"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (f1 TIMESTAMP, f2 TEXT);
column does not match upstream source, specified: f1 timestamp NULL, upstream: pk int4 NOT NULL

! CREATE SOURCE "missing_column"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL);
incorrect column specification: 1 columns were specified, upstream has 2: pk, f2

! CREATE SOURCE "extra_column"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT, f3 INTEGER);
incorrect column specification: 3 columns were specified, upstream has 2: pk, f2

> CREATE MATERIALIZED SOURCE "no_replica_identity"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'no_replica_identity' (f1 INTEGER);
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

> CREATE MATERIALIZED SOURCE "pk_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER NOT NULL, f2 TEXT);

> CREATE MATERIALIZED SOURCE "nonpk_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'nonpk_table' (f1 INTEGER, f2 INTEGER);

> CREATE MATERIALIZED SOURCE "types_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'types_table' (date_col DATE, time_col TIME, timestamp_col TIMESTAMP, uuid_col UUID, double_col DOUBLE PRECISION);

> CREATE MATERIALIZED SOURCE "large_text"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'large_text' (f1 TEXT);

> CREATE MATERIALIZED SOURCE "trailing_space_pk"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'trailing_space_pk' (f1 TEXT NOT NULL);

> CREATE MATERIALIZED SOURCE "trailing_space_nopk"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'trailing_space_nopk' (f1 TEXT);

> CREATE MATERIALIZED SOURCE "multipart_pk"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'multipart_pk' (f1 INTEGER NOT NULL, f2 TEXT NOT NULL, f3 TEXT);

> CREATE MATERIALIZED SOURCE "nulls_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'nulls_table' (f1 TEXT, f2 INTEGER);

> CREATE MATERIALIZED SOURCE "utf8_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'utf8_table' (f1 TEXT NOT NULL, f2 TEXT);

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
"2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768"

> SELECT LENGTH(f1) FROM large_text;
16777216

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

#
# Confirm that the new sources can be used to build upon
#

> CREATE MATERIALIZED VIEW join_view AS SELECT * FROM pk_table, nonpk_table WHERE pk_table.pk = nonpk_table.f1;

> SELECT * FROM join_view;
"1" "one" "1" "1"
"1" "one" "1" "1"
"2" "two" "2" "2"
"2" "two" "2" "2"

#
# Basic sanity check that the timestamps are reasonable
#

> SELECT COUNT(*) > 0 FROM pk_table AS OF NOW();
true

> SELECT COUNT(*) > 0 FROM nonpk_table AS OF NOW();
true

> SELECT COUNT(*) > 0 FROM join_view AS OF NOW();
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

INSERT INTO types_table VALUES ('2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1234.56768);

INSERT INTO large_text VALUES (REPEAT('x', 16 * 1024 * 1024));

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
"2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768"
"2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768"

> SELECT LENGTH(f1) FROM large_text;
16777216
16777216

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

> SELECT * FROM join_view;
"2" "two_two" "2" "2"
"2" "two_two" "2" "2"

#
# Check that the timestamps continue to be reasonable in the face of incoming updates
#

> SELECT COUNT(*) > 0 FROM pk_table AS OF NOW();
true

> SELECT COUNT(*) > 0 FROM nonpk_table AS OF NOW();
true

> SELECT COUNT(*) > 0 FROM join_view AS OF NOW();
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

#
# Check that all data sources empty out on the Materialize side
#

> SELECT COUNT(*) = 0 FROM pk_table;
true

> SELECT COUNT(*) = 0 FROM nonpk_table;
true

> SELECT COUNT(*) = 0 FROM large_text;
true

> SELECT COUNT(*) = 0 FROM trailing_space_nopk;
true

> SELECT COUNT(*) = 0 FROM trailing_space_pk;
true

> SELECT COUNT(*) = 0 FROM multipart_pk;
true

> SELECT COUNT(*) = 0 FROM nulls_table;
true

> SELECT COUNT(*) = 0 FROM utf8_table;
true

> SELECT COUNT(*) = 0 FROM join_view;
true

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION mz_source;
INSERT INTO pk_table VALUES (99999);

! SELECT COUNT(*) = 0 FROM pk_table;
Source error: File IO: db error: ERROR: publication "mz_source" does not exist
