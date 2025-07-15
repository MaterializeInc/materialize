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
ALTER SYSTEM SET mysql_source_snapshot_max_execution_time = 1000

> CREATE SOURCE "test_slot_source"
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE pk_table FROM SOURCE test_slot_source (REFERENCE public.pk_table);

> SHOW SUBSOURCES ON test_slot_source
test_slot_source_progress progress

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

# TODO: database-issues#7587 (error not stable)
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
mz_source_progress    progress   <null>        ""

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
public          "\"mixED_CAse\""
public          "\"space table\""
public          "\"таблица\""
public          trailing_space_pk
public          escaped_text_table
public          trailing_space_nopk
public          "\"\"\"literal_quotes\"\"\""
conflict_schema conflict_table

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source_progress';
running

> SELECT count(*) FROM mz_source_progress WHERE transaction_id > 0
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

> CREATE CLUSTER large_cluster SIZE '16'

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
enum_source_progress    progress  <null>       ""

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

> CREATE SOURCE another_source
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE another_table FROM SOURCE another_source (REFERENCE another_schema.another_table);

> SHOW SOURCES
another_source          mysql  cdc_cluster     ""
another_source_progress progress <null>        ""

> SHOW TABLES
another_table           ""

> DROP SOURCE another_source CASCADE;

$ mysql-execute name=mysql
DROP SCHEMA conflict_schema;
DROP SCHEMA another_schema;
