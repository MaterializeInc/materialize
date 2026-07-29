# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s


> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

# TODO: database-issues#7490 (misleading error message)
# ! CREATE CONNECTION mysql_conn TO MYSQL (
#     HOST mysql,
#     USER root,
#     PASSWORD SECRET mysqlpass
#     BROKER '${testdrive.kafka-addr}'
#   )
# contains:mysql connections do not support BROKER values

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE CLUSTER cdc_cluster SIZE '${arg.default-replica-size}'

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
INSERT INTO pk_table VALUES (2, 'two');

INSERT INTO pk_table VALUES (3, 'three');

CREATE TABLE nonpk_table (f1 INTEGER, f2 INTEGER);
INSERT INTO nonpk_table VALUES (1, 1), (1, 1);
INSERT INTO nonpk_table VALUES (2, 2), (2, 2);

# no range types in MySQL

CREATE TABLE types_table (char_col char(3), date_col DATE, time_col TIME, timestamp_col TIMESTAMP, uuid_col VARCHAR(36), double_col DOUBLE PRECISION, numeric_col DECIMAL(8,4));
INSERT INTO types_table VALUES ('foo', '2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1234.56768, 1234.5678);

# no array_types_table: MySQL does not support array types

# needed for MySQL 5.7
SET GLOBAL max_allowed_packet=67108864;

# reconnect after setting the max_allowed_packet var
$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
USE public;

# explicitly set the value to have the same across different versions of MySQL
ALTER DATABASE public CHARSET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE mysql_version (version TEXT);
INSERT INTO mysql_version SELECT version();

CREATE TABLE large_text (f1 LONGTEXT, f2 LONGTEXT, f3 TEXT);
INSERT INTO large_text VALUES (REPEAT('x', 16 * 1024 * 1024), REPEAT('y', 1 * 1024 * 1024), REPEAT('z', 65535));
INSERT INTO large_text VALUES (REPEAT('a', 3 * 1024 * 1024),  REPEAT('b', 2 * 1024 * 1024), REPEAT('c', 65535));

CREATE TABLE trailing_space_pk (f1 VARCHAR(32) PRIMARY KEY);
INSERT INTO trailing_space_pk VALUES ('abc   ');

CREATE TABLE trailing_space_nopk (f1 TEXT);
INSERT INTO trailing_space_nopk VALUES ('abc   ');

CREATE TABLE multipart_pk(f1 INTEGER, f2 VARCHAR(32), f3 TEXT, PRIMARY KEY (f1, f2));
INSERT INTO multipart_pk VALUES (1, 'abc', 'xyz');

CREATE TABLE nulls_table (f1 TEXT, f2 INTEGER);
INSERT INTO nulls_table VALUES (NULL, NULL);

CREATE TABLE utf8_table (f1 VARCHAR(32) PRIMARY KEY, f2 TEXT);
INSERT INTO utf8_table VALUES ('това е текст', 'това ''е'' "текст"');

CREATE TABLE no_replica_identity (f1 INTEGER);
INSERT INTO no_replica_identity VALUES (1), (2);

CREATE TABLE `таблица` (`колона` TEXT);
INSERT INTO `таблица` VALUES ('стойност');

CREATE TABLE `"literal_quotes"` (a TEXT);
INSERT INTO `"literal_quotes"` VALUES ('v');

CREATE TABLE `create` (a TEXT);
INSERT INTO `create` VALUES ('v');

CREATE TABLE escaped_text_table (f1 TEXT, f2 TEXT);
INSERT INTO escaped_text_table VALUES ('escaped\\ntext\\twith\\nnewlines\\tand\\ntabs', 'more\\tescaped\\ntext');
INSERT INTO escaped_text_table VALUES ('second\\nrow\\twith\\tmore\\ttabs', 'and\\nmore\\n\\nnewlines\\n');

CREATE TABLE mixED_CAse (spECialCase INTEGER);
INSERT INTO mixED_CAse VALUES (1), (2);

CREATE TABLE conflict_table (f1 INTEGER);
INSERT INTO conflict_table VALUES (123);

DROP SCHEMA IF EXISTS conflict_schema;
CREATE SCHEMA conflict_schema;
CREATE TABLE conflict_schema.conflict_table (f1 TEXT);
INSERT INTO conflict_schema.conflict_table VALUES ('234');

CREATE TABLE `space table` (`space column` INTEGER);

CREATE TABLE enum_table (a ENUM ('var0', 'var1'));
INSERT INTO enum_table VALUES ('var1'), ('var0');

CREATE TABLE another_enum_table (`колона` ENUM ('var2', 'var3'));
INSERT INTO another_enum_table VALUES ('var2'), ('var3');

CREATE TABLE conflict_schema.another_enum_table (`колона` ENUM ('var2', 'var3'));
INSERT INTO conflict_schema.another_enum_table VALUES ('var2'), ('var3');

DROP SCHEMA IF EXISTS another_schema;
CREATE SCHEMA another_schema;
CREATE TABLE another_schema.another_table (f1 TEXT);
INSERT INTO another_schema.another_table VALUES ('123');

# Sneak in a test for mysql_source_snapshot_max_execution_time
$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
$ postgres-execute connection=mz_system
ALTER SYSTEM SET mysql_source_snapshot_max_execution_time = 10000

> CREATE SOURCE "test_slot_source"
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE pk_table FROM SOURCE test_slot_source (REFERENCE public.pk_table);

> SHOW SUBSOURCES ON test_slot_source

> SHOW TABLES
pk_table ""

> DROP SOURCE test_slot_source CASCADE;

$ postgres-execute connection=mz_system
ALTER SYSTEM SET mysql_source_snapshot_max_execution_time = 0

# Validate mysql_source_snapshot_lock_wait_timeout
$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
$ postgres-execute connection=mz_system
ALTER SYSTEM SET mysql_source_snapshot_lock_wait_timeout = 1

$ mysql-execute name=mysql
USE public;
BEGIN;
INSERT INTO pk_table VALUES (4, 'four');

> CREATE SOURCE "test_slot_source"
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE pk_table FROM SOURCE test_slot_source (REFERENCE public.pk_table);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'test_slot_source';
stalled

$ mysql-execute name=mysql
ROLLBACK;

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'test_slot_source';
running

> DROP SOURCE test_slot_source CASCADE;

$ postgres-execute connection=mz_system
ALTER SYSTEM SET mysql_source_snapshot_lock_wait_timeout = 3600

# Validate mysql_source_snapshot_wait_timeout
$ postgres-execute connection=mz_system
ALTER SYSTEM SET mysql_source_snapshot_wait_timeout = '60s'

> CREATE SOURCE "test_slot_source"
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE pk_table FROM SOURCE test_slot_source (REFERENCE public.pk_table);

> SELECT COUNT(*) > 0 FROM pk_table;
true

> DROP SOURCE test_slot_source CASCADE;

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET mysql_source_snapshot_wait_timeout


#
# Error checking
#

! CREATE CONNECTION no_such_host TO MYSQL (
    HOST 'no_such_mysql.mtrlz.com',
    USER root,
    PASSWORD SECRET mysqlpass
  )
contains:failed to lookup address information
# TODO: database-issues#7490 (misleading error message)
# contains:error connecting to server: failed to lookup address information: Name or service not known: failed to lookup address

! CREATE CONNECTION no_such_port TO MYSQL (
    HOST mysql,
    PORT 65534,
    USER root,
    PASSWORD SECRET mysqlpass
  )
contains:Connection refused
# TODO: database-issues#7490 (misleading error message)
# contains:error connecting to server: Connection refused

# TODO: https://linear.app/materializeinc/issue/SS-315 (error not stable)
# ! CREATE CONNECTION no_such_user TO MYSQL (
#     HOST mysql,
#     USER no_such_user,
#     PASSWORD SECRET mysqlpass
#   )
# TODO: database-issues#7490 (misleading error message)
# contains:password authentication failed for user "no_such_user"

> CREATE SECRET badpass AS 'badpass'
! CREATE CONNECTION no_such_password TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET badpass
  )
contains:Access denied for user 'root'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_enforce_external_addresses = true

! CREATE CONNECTION private_address TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )
contains:Address resolved to a private IP

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_enforce_external_addresses = false

> CREATE SOURCE "mz_source"
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn;

! CREATE TABLE enum_table FROM SOURCE mz_source (REFERENCE public.enum_table);
contains:referenced tables use unsupported types

! CREATE TABLE another_enum_table FROM SOURCE mz_source (REFERENCE public.another_enum_table);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source_2
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn (
    PUBLICATION 'mz_source'
  );
contains: found PUBLICATION

! CREATE SOURCE mz_source_2
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn (
    DETAILS 'abc'
  );
contains: CREATE SOURCE specifies DETAILS option

#
# Establish direct replication
#
#
# Note: This implicitly tests that enum_table being part of the publication does not
# prevent us from using other tables as subsources.
#

> CREATE TABLE "pk_table" FROM SOURCE mz_source (REFERENCE public."pk_table");
> CREATE TABLE "nonpk_table" FROM SOURCE mz_source (REFERENCE public."nonpk_table");
> CREATE TABLE "types_table" FROM SOURCE mz_source (REFERENCE public."types_table");
> CREATE TABLE "large_text" FROM SOURCE mz_source (REFERENCE public."large_text");
> CREATE TABLE "trailing_space_pk" FROM SOURCE mz_source (REFERENCE public."trailing_space_pk");
> CREATE TABLE "trailing_space_nopk" FROM SOURCE mz_source (REFERENCE public."trailing_space_nopk");
> CREATE TABLE "multipart_pk" FROM SOURCE mz_source (REFERENCE public."multipart_pk");
> CREATE TABLE "nulls_table" FROM SOURCE mz_source (REFERENCE public."nulls_table");
> CREATE TABLE "utf8_table" FROM SOURCE mz_source (REFERENCE public."utf8_table");
> CREATE TABLE "escaped_text_table" FROM SOURCE mz_source (REFERENCE public."escaped_text_table");
> CREATE TABLE "mixED_CAse" FROM SOURCE mz_source (REFERENCE public."mixED_CAse");
> CREATE TABLE conflict_table FROM SOURCE mz_source (REFERENCE conflict_schema.conflict_table);
> CREATE TABLE "create" FROM SOURCE mz_source (REFERENCE public."create");
> CREATE TABLE "space table" FROM SOURCE mz_source (REFERENCE public."space table");
> CREATE TABLE "таблица" FROM SOURCE mz_source (REFERENCE public."таблица");
> CREATE TABLE """literal_quotes""" FROM SOURCE mz_source (REFERENCE public."""literal_quotes""");

> SHOW SOURCES
mz_source             mysql      cdc_cluster  ""

> SHOW TABLES
conflict_table        ""
create                ""
escaped_text_table    ""
large_text            ""
mixED_CAse            ""
multipart_pk          ""
nonpk_table           ""
nulls_table           ""
pk_table              ""
trailing_space_nopk   ""
trailing_space_pk     ""
types_table           ""
utf8_table            ""
"\"literal_quotes\""  ""
"space table"         ""
таблица               ""

> SELECT schema_name, table_name FROM mz_internal.mz_mysql_source_tables
public          create
public          pk_table
public          large_text
public          utf8_table
public          nonpk_table
public          types_table
public          nulls_table
public          multipart_pk
public          mixED_CAse
public          "space table"
public          таблица
public          trailing_space_pk
public          escaped_text_table
public          trailing_space_nopk
public          "\"literal_quotes\""
conflict_schema conflict_table

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running

> SELECT count(*) FROM mz_source WHERE transaction_id > 0
1

# There is no such thing like mz_internal.mz_mysql_sources because MySQL does not have replication slots.

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

> SELECT pg_typeof(numeric_col) FROM types_table;
"numeric"

# no array_types_table: MySQL does not support array types

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

> SELECT * FROM "mixED_CAse"
1
2

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
# Modify the tables on the MySQL side
#

$ mysql-execute name=mysql
INSERT INTO pk_table VALUES (4, 'four');
INSERT INTO pk_table VALUES (5, 'five');
DELETE FROM pk_table WHERE pk = 1;
UPDATE pk_table SET f2 = 'two_two' WHERE pk = 2;
UPDATE pk_table SET pk = pk + 10 WHERE pk BETWEEN 3 AND 4;

INSERT INTO nonpk_table VALUES (3, 3), (3, 3);
DELETE FROM nonpk_table WHERE f1 = 1 LIMIT 1;
UPDATE nonpk_table SET f1 = f1 + 10 WHERE f1 = 1;
UPDATE nonpk_table SET f1 = f1 + 100 WHERE f1 = 3;

INSERT INTO types_table VALUES ('foo', '2011-11-11', '11:11:11', '2011-11-11 11:11:11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1234.56768, 1234.5678);

INSERT INTO large_text VALUES (REPEAT('x', 16 * 1024 * 1024), 'abc', 'abc');

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
UPDATE utf8_table SET f1 = concat(f1, f1), f2 = concat(f2, f2);

INSERT INTO `таблица` SELECT * FROM `таблица`;

INSERT INTO mixED_CAse SELECT * FROM mixED_CAse;

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

> SELECT LENGTH(f1), LENGTH(f2), LENGTH(f3) FROM large_text;
16777216 1048576 65535
3145728  2097152 65535
16777216 3 3

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

> SELECT * FROM "mixED_CAse";
1
2
1
2

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
# Ensure we can start a source with more workers
#

> CREATE CLUSTER large_cluster SIZE 'scale=1,workers=16'

> CREATE SOURCE large_cluster_source
  IN CLUSTER large_cluster
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE large_cluster_source_pk_table
  FROM SOURCE large_cluster_source
  (REFERENCE public."pk_table");

> SELECT * FROM large_cluster_source_pk_table;
13 three
14 four
2 two_two
5 five

> SELECT status = 'running' FROM mz_internal.mz_source_statuses WHERE name = 'large_cluster_source_pk_table' and type = 'table';
true

> DROP SOURCE large_cluster_source CASCADE;

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_range_test;
CREATE TABLE pk_range_test (id BIGINT PRIMARY KEY, val BIGINT);
SET @i := 0;
INSERT INTO pk_range_test SELECT @i := @i + 1, @i * 7 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000;

> CREATE CLUSTER pk_range_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_range_source
  IN CLUSTER pk_range_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_range_table FROM SOURCE pk_range_source (REFERENCE public.pk_range_test);

# Exact row count
> SELECT COUNT(*) FROM pk_range_table;
1000

# No duplicates: distinct count must equal total count
> SELECT COUNT(DISTINCT id) = COUNT(*) FROM pk_range_table;
true

# Full key range present (ids 1..1000)
> SELECT MIN(id), MAX(id) FROM pk_range_table;
1 1000

# Checksum: val = id * 7, so SUM(val) = 7 * SUM(1..1000) = 7 * 500500
> SELECT SUM(val) FROM pk_range_table;
3503500

> DROP SOURCE pk_range_source CASCADE;
> DROP CLUSTER pk_range_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_range_test;

#
# Same, but for a single-column non-integer (char) PK.
#

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_char_test;
CREATE TABLE pk_char_test (id CHAR(26) PRIMARY KEY, val BIGINT);
SET @i := 0;
INSERT INTO pk_char_test SELECT LPAD(CONV(@i := @i + 1, 10, 36), 26, '0'), @i FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000;

> CREATE CLUSTER pk_char_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_char_source
  IN CLUSTER pk_char_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_char_table FROM SOURCE pk_char_source (REFERENCE public.pk_char_test);

# Exact count and no duplicate/missing keys
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_char_table;
1000 1000

# Checksum: val = row number, so SUM(val) = SUM(1..1000) = 500500
> SELECT SUM(val) FROM pk_char_table;
500500

> DROP SOURCE pk_char_source CASCADE;
> DROP CLUSTER pk_char_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_char_test;

#
# Same, but for a composite (char, int) primary key.
#

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_comp_test;
CREATE TABLE pk_comp_test (region CHAR(2) NOT NULL, id INT NOT NULL, val BIGINT, PRIMARY KEY (region, id));
SET @i := 0;
INSERT INTO pk_comp_test SELECT ELT((@i := @i + 1) % 5 + 1, 'aa', 'bb', 'cc', 'dd', 'ee'), @i, @i FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000;

> CREATE CLUSTER pk_comp_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_comp_source
  IN CLUSTER pk_comp_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_comp_table FROM SOURCE pk_comp_source (REFERENCE public.pk_comp_test);

# Exact count and no duplicate/missing keys (id alone is unique here)
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_comp_table;
1000 1000

# Every region partition is present and complete
> SELECT region, COUNT(*) FROM pk_comp_table GROUP BY region ORDER BY region;
aa 200
bb 200
cc 200
dd 200
ee 200

# Checksum: val = row number, so SUM(val) = SUM(1..1000) = 500500
> SELECT SUM(val) FROM pk_comp_table;
500500

> DROP SOURCE pk_comp_source CASCADE;
> DROP CLUSTER pk_comp_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_comp_test;

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_surplus_test;
CREATE TABLE pk_surplus_test (id CHAR(26) PRIMARY KEY, val BIGINT);
SET @i := 0;
INSERT INTO pk_surplus_test SELECT LPAD(CONV(@i := @i + 1, 10, 36), 26, '0'), @i FROM mysql.time_zone t1 LIMIT 3;

> CREATE CLUSTER pk_surplus_cluster SIZE 'scale=1,workers=16'

> CREATE SOURCE pk_surplus_source
  IN CLUSTER pk_surplus_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_surplus_table FROM SOURCE pk_surplus_source (REFERENCE public.pk_surplus_test);

# All rows present exactly once
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_surplus_table;
3 3

# Checksum: val = row number, so SUM(val) = 1 + 2 + 3 = 6
> SELECT SUM(val) FROM pk_surplus_table;
6

> DROP SOURCE pk_surplus_source CASCADE;
> DROP CLUSTER pk_surplus_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_surplus_test;

#
# Empty table.
#

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_empty_test;
CREATE TABLE pk_empty_test (id CHAR(26) PRIMARY KEY, val BIGINT);

> CREATE CLUSTER pk_empty_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_empty_source
  IN CLUSTER pk_empty_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_empty_table FROM SOURCE pk_empty_source (REFERENCE public.pk_empty_test);

> SELECT COUNT(*) FROM pk_empty_table;
0

> DROP SOURCE pk_empty_source CASCADE;
> DROP CLUSTER pk_empty_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_empty_test;

#
# BIGINT UNSIGNED PK with keys spanning the full unsigned domain, including
# values above i64::MAX.
#

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_uint_test;
CREATE TABLE pk_uint_test (id BIGINT UNSIGNED PRIMARY KEY, val BIGINT);
SET @i := 0;
INSERT INTO pk_uint_test SELECT @i := @i + 1, @i FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 500;
SET @j := 0;
INSERT INTO pk_uint_test SELECT 18446744073709551115 + (@j := @j + 1), -@j FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 500;

> CREATE CLUSTER pk_uint_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_uint_source
  IN CLUSTER pk_uint_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_uint_table FROM SOURCE pk_uint_source (REFERENCE public.pk_uint_test);

# Exact count and no duplicate/missing keys
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_uint_table;
1000 1000

# Keys span from 1 to the top of the u64 domain
> SELECT MIN(id), MAX(id) FROM pk_uint_table;
1 18446744073709551615

# Checksum cancels: val = 1..500 for the low keys and -(1..500) for the high keys
> SELECT SUM(val) FROM pk_uint_table;
0

> DROP SOURCE pk_uint_source CASCADE;
> DROP CLUSTER pk_uint_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_uint_test;

#
# BIT primary key. A BIT column maps to a Materialize uint64, but carries the
# MySqlColumnMeta::Bit marker. The PK-range sampler renders a boundary from the
# Materialize scalar type alone (uint64 takes the integer path, CAST(id AS CHAR)),
# discarding that marker. For a BIT column CAST(.. AS CHAR) returns the value's
# raw bytes, not a decimal literal, and that text is spliced verbatim into the
# worker range predicates. The BIT(64) value X'30204F5220313D31' is the eight
# ASCII bytes "0 OR 1=1", so the sampled boundary turns the two worker reads into
#   WHERE id <  0 OR 1=1
#   WHERE id >= 0 OR 1=1
# both of which match every row. Every key must still be emitted exactly once.
#

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_bit_test;
CREATE TABLE pk_bit_test (id BIT(64) PRIMARY KEY, val BIGINT);
INSERT INTO pk_bit_test VALUES (1, 100), (0x30204F5220313D31, 200);

> CREATE CLUSTER pk_bit_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_bit_source
  IN CLUSTER pk_bit_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_bit_table FROM SOURCE pk_bit_source (REFERENCE public.pk_bit_test);

# Exact count and no duplicate keys. Under the bug both workers read the whole
# table, so COUNT(*) is 4 while COUNT(DISTINCT id) stays 2.
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_bit_table;
2 2

# Both keys, each exactly once. X'30204F5220313D31' decodes big-endian to
# 3467858927221292337.
> SELECT id, val FROM pk_bit_table ORDER BY id;
1 100
3467858927221292337 200

> DROP SOURCE pk_bit_source CASCADE;
> DROP CLUSTER pk_bit_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_bit_test;

#
# PK-range splitting disabled via the mysql_source_snapshot_parallelism dyncfg.
# The same shape as the pk_range_test above, but every table must fall back to a
# single-worker whole-table read and still produce a complete, duplicate-free
# snapshot.
#

$ postgres-execute connection=mz_system
ALTER SYSTEM SET mysql_source_snapshot_parallelism = false

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_serial_test;
CREATE TABLE pk_serial_test (id BIGINT PRIMARY KEY, val BIGINT);
SET @i := 0;
INSERT INTO pk_serial_test SELECT @i := @i + 1, @i * 7 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000;

> CREATE CLUSTER pk_serial_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_serial_source
  IN CLUSTER pk_serial_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_serial_table FROM SOURCE pk_serial_source (REFERENCE public.pk_serial_test);

# Exact row count and no duplicates
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_serial_table;
1000 1000

# Full key range present (ids 1..1000)
> SELECT MIN(id), MAX(id) FROM pk_serial_table;
1 1000

# Checksum: val = id * 7, so SUM(val) = 7 * SUM(1..1000) = 7 * 500500
> SELECT SUM(val) FROM pk_serial_table;
3503500

> DROP SOURCE pk_serial_source CASCADE;
> DROP CLUSTER pk_serial_cluster CASCADE;

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET mysql_source_snapshot_parallelism

$ mysql-execute name=mysql
DROP TABLE pk_serial_test;

#
# Multibyte utf8mb4 PK with a binary collation. Keys mix 2-, 3-, and 4-byte code
# points (accented Latin, Greek, CJK, and emoji), assembled from their utf8 bytes
# so the test file stays ASCII.
#

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_charset_test;
CREATE TABLE pk_charset_test (id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, val BIGINT);
SET @i := 0;
INSERT INTO pk_charset_test SELECT CONCAT(ELT((n % 6) + 1, CHAR(0xC3B1 USING utf8mb4), CHAR(0xCEA9 USING utf8mb4), CHAR(0xE4B8AD USING utf8mb4), CHAR(0xE7958C USING utf8mb4), CHAR(0xF09F9880 USING utf8mb4), CHAR(0xF09D958F USING utf8mb4)), LPAD(CONV(n, 10, 36), 8, '0')), n FROM (SELECT @i := @i + 1 AS n FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000) seq;

> CREATE CLUSTER pk_charset_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_charset_source
  IN CLUSTER pk_charset_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_charset_table FROM SOURCE pk_charset_source (REFERENCE public.pk_charset_test);

# Exact count and no duplicate/missing keys
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_charset_table;
1000 1000

# Checksum: val = row number, so SUM(val) = SUM(1..1000) = 500500
> SELECT SUM(val) FROM pk_charset_table;
500500

> DROP SOURCE pk_charset_source CASCADE;
> DROP CLUSTER pk_charset_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_charset_test;

#
# Non-utf8mb4 charset PK: a latin1 column with a binary collation. Keys carry
# high-byte latin1 characters (the 0xC0-0xDF range), built from their bytes.
#

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_latin1_test;
CREATE TABLE pk_latin1_test (id VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_bin PRIMARY KEY, val BIGINT);
SET @i := 0;
INSERT INTO pk_latin1_test SELECT CONCAT(CHAR(0xC0 + (n % 32) USING latin1), CONVERT(LPAD(CONV(n, 10, 36), 8, '0') USING latin1)), n FROM (SELECT @i := @i + 1 AS n FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000) seq;

> CREATE CLUSTER pk_latin1_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_latin1_source
  IN CLUSTER pk_latin1_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_latin1_table FROM SOURCE pk_latin1_source (REFERENCE public.pk_latin1_test);

# Exact count and no duplicate/missing keys
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_latin1_table;
1000 1000

# Checksum: val = row number, so SUM(val) = SUM(1..1000) = 500500
> SELECT SUM(val) FROM pk_latin1_table;
500500

> DROP SOURCE pk_latin1_source CASCADE;
> DROP CLUSTER pk_latin1_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_latin1_test;

#
# Concurrent DML on a multibyte-keyed table. Inserts and deletes are issued right
# after the source starts, so they race the snapshot. Whatever the interleaving,
# the output must converge to the final upstream state exactly once per key.
#

$ mysql-execute name=mysql
DROP TABLE IF EXISTS pk_conc_test;
CREATE TABLE pk_conc_test (id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, val BIGINT);
SET @i := 0;
INSERT INTO pk_conc_test SELECT CONCAT(ELT((n % 6) + 1, CHAR(0xC3B1 USING utf8mb4), CHAR(0xCEA9 USING utf8mb4), CHAR(0xE4B8AD USING utf8mb4), CHAR(0xE7958C USING utf8mb4), CHAR(0xF09F9880 USING utf8mb4), CHAR(0xF09D958F USING utf8mb4)), LPAD(CONV(n, 10, 36), 8, '0')), n FROM (SELECT @i := @i + 1 AS n FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000) seq;

> CREATE CLUSTER pk_conc_cluster SIZE 'scale=1,workers=4'

> CREATE SOURCE pk_conc_source
  IN CLUSTER pk_conc_cluster
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE pk_conc_table FROM SOURCE pk_conc_source (REFERENCE public.pk_conc_test);

# Insert 1000 more rows (sequence continues at 1001..2000) and delete the first 500,
# all while the snapshot is likely still in flight.
$ mysql-execute name=mysql
INSERT INTO pk_conc_test SELECT CONCAT(ELT((n % 6) + 1, CHAR(0xC3B1 USING utf8mb4), CHAR(0xCEA9 USING utf8mb4), CHAR(0xE4B8AD USING utf8mb4), CHAR(0xE7958C USING utf8mb4), CHAR(0xF09F9880 USING utf8mb4), CHAR(0xF09D958F USING utf8mb4)), LPAD(CONV(n, 10, 36), 8, '0')), n FROM (SELECT @i := @i + 1 AS n FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000) seq;
DELETE FROM pk_conc_test WHERE val <= 500;

# Final upstream state is val 501..2000, each key exactly once.
> SELECT COUNT(*), COUNT(DISTINCT id) FROM pk_conc_table;
1500 1500

# Checksum: SUM(501..2000) = SUM(1..2000) - SUM(1..500) = 2001000 - 125250
> SELECT SUM(val) FROM pk_conc_table;
1875750

> DROP SOURCE pk_conc_source CASCADE;
> DROP CLUSTER pk_conc_cluster CASCADE;

$ mysql-execute name=mysql
DROP TABLE pk_conc_test;

#
# Remove all data on the Postgres side
#

$ mysql-execute name=mysql
DELETE FROM pk_table;
DELETE FROM nonpk_table;
DELETE FROM large_text;
DELETE FROM trailing_space_pk;
DELETE FROM trailing_space_nopk;
DELETE FROM multipart_pk;
DELETE FROM nulls_table;
DELETE FROM utf8_table;
DELETE FROM conflict_schema.conflict_table;
DELETE FROM `таблица`;
DELETE FROM mixED_CAse;

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

> SELECT COUNT(*) FROM "mixED_CAse";
0

> SELECT COUNT(*) FROM conflict_table;
0

> CREATE SOURCE enum_source
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn;

# Reference exists in two schemas, so is not unambiguous
# TODO: database-issues#7397 (explicit schema definition required)
# ! CREATE TABLE conflict_enum
#   FROM SOURCE enum_source
#   REFERENCE (conflict_schema.another_enum_table)
#   WITH (TEXT COLUMNS = [another_enum_table."колона"]);
# contains: invalid TEXT COLUMNS option value: table another_enum_table is ambiguous, consider specifying the schema

# TODO: database-issues#7397 (explicit schema definition required)
# ! CREATE TABLE pk_table
#   FROM SOURCE enum_source
#   REFERENCE (pk_table)
#   WITH (TEXT COLUMNS = [foo]);
# contains: invalid TEXT COLUMNS option value: column name 'foo' must have at least a table qualification

# TODO: database-issues#7397 (explicit schema definition required)
# ! CREATE TABLE pk_table
#   FROM SOURCE enum_source
#   REFERENCE (pk_table)
#   WITH (TEXT COLUMNS = [foo.bar.qux.qax.foo]);
# contains: invalid TEXT COLUMNS option value: qualified name did not have between 1 and 3 components: foo.bar.qux.qax

# TODO: database-issues#7397 (explicit schema definition required)
# ! CREATE TABLE enum_table
#   FROM SOURCE enum_source
#   REFERENCE (enum_table)
#   WITH (TEXT COLUMNS = [enum_table.a, enum_table.a]);
# contains: invalid TEXT COLUMNS option value: unexpected multiple references to postgres.public.enum_table.a

# TODO: database-issues#7397 (explicit schema definition required)
# utf8_table is not part of mz_source_narrow publication
# ! CREATE TABLE enum_table
#   FROM SOURCE enum_source
#   REFERENCE (enum_table)
#   WITH (TEXT COLUMNS = [enum_table.a, utf8_table.f1]);
# contains: invalid TEXT COLUMNS option value: table utf8_table not found in source

# n.b includes a reference to pk_table, which is not a table that's part of the
# source, but is part of the publication.

! CREATE TABLE another_enum_table
  FROM SOURCE enum_source
  (REFERENCE public.another_enum_table)
  WITH (TEXT COLUMNS = ["колона", pk]);
contains:TEXT COLUMNS refers to table not currently being added

> CREATE TABLE enum_table
  FROM SOURCE enum_source
  (REFERENCE public."enum_table")
  WITH (TEXT COLUMNS = [a]);

> CREATE TABLE another_enum_table
  FROM SOURCE enum_source
  (REFERENCE public."another_enum_table")
  WITH (TEXT COLUMNS = ["колона"]);

> SELECT * FROM (SHOW SOURCES) WHERE name LIKE '%enum%';
enum_source             mysql     cdc_cluster  ""

> SELECT * FROM (SHOW TABLES) WHERE name LIKE '%enum%';
another_enum_table      ""
enum_table              ""

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

$ mysql-execute name=mysql
INSERT INTO pk_table VALUES (99999, 'abc');

> DROP SOURCE enum_source CASCADE;
> DROP SOURCE "mz_source" CASCADE;

#
# Check schema scoped tables

> BEGIN
> CREATE SOURCE another_source
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE another_table FROM SOURCE another_source (REFERENCE another_schema.another_table);
> COMMIT

> SHOW SOURCES
another_source          mysql  cdc_cluster     ""

> SHOW TABLES
another_table           ""

> DROP SOURCE another_source CASCADE;

$ mysql-execute name=mysql
DROP SCHEMA conflict_schema;
DROP SCHEMA another_schema;
