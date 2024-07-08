# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=20


#
# Test the various character data types
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

# Insert data pre-snapshot
# explicitly set a character set and collate (one that also exists in MySQL 5.7)
CREATE TABLE t1 (f1 VARCHAR(10), f2 CHAR(10), f3 TEXT) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t1 VALUES ('abc', 'abc', 'abc');
INSERT INTO t1 VALUES ('ABC', 'ABC', 'ABC');
INSERT INTO t1 VALUES ('abc ', 'abc ', 'abc ');

CREATE TABLE t2 (f_latin1_1 VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_bin, f_latin1_2 VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_general_ci, f_latin1_3 VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_general_cs, f_latin1_4 VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_german2_ci, f_utf32_1 VARCHAR(20) CHARACTER SET utf32 COLLATE utf32_bin, f_utf32_2 VARCHAR(20) CHARACTER SET utf32 COLLATE utf32_general_ci, f_utf32_3 VARCHAR(20) CHARACTER SET utf32 COLLATE utf32_german2_ci, f_utf32_4 VARCHAR(20) CHARACTER SET utf32 COLLATE utf32_unicode_ci, f_utf8_1 VARCHAR(20) CHARACTER SET utf8 COLLATE utf8_bin, f_utf8_2 VARCHAR(20) CHARACTER SET utf8 COLLATE utf8_general_ci, f_utf8_3 VARCHAR(20) CHARACTER SET utf8 COLLATE utf8_german2_ci, f_utf8_4 VARCHAR(20) CHARACTER SET utf8 COLLATE utf8_icelandic_ci, f_utf8_5 VARCHAR(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci, f_utf8mb4_1 VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, f_utf8mb4_2 VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, f_utf8mb4_3 VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_german2_ci, f_utf8mb4_4 VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, f_utf8mb4_5 VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_vietnamese_ci);
INSERT INTO t2 VALUES (' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ', ' abc @öÜèßï ');

CREATE TABLE t3 (f1 TINYTEXT, f2 MEDIUMTEXT, f3 LONGTEXT) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
INSERT INTO t3 VALUES ('abc ', 'abc ', 'abc ');

CREATE TABLE t4 (f1 TEXT) CHARACTER SET ascii COLLATE ascii_general_ci;
INSERT INTO t4 VALUES ('aaa');
INSERT INTO t4 VALUES ('abc');

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3) FROM t1 LIMIT 1;
"character varying" "character" "text"

> SELECT * FROM t1;
"abc" "abc       " "abc"
"abc" "abc       " "abc"
"ABC" "ABC       " "ABC"
"ABC" "ABC       " "ABC"
"abc " "abc       " "abc "
"abc " "abc       " "abc "

$ mysql-execute name=mysql
# use LIKE instead of '=' to due to 'PAD SPACE' in the use collation
UPDATE t1 SET f1 = 'klm', f2 = 'klm', f3 = 'klm' WHERE lower(f1) LIKE 'abc';
UPDATE t1 SET f1 = 'xyz ', f2 = 'xyz ', f3 = 'xyz ' WHERE lower(f1) LIKE 'abc ';

> SELECT * FROM t1;
"klm" "klm       " "klm"
"klm" "klm       " "klm"
"klm" "klm       " "klm"
"klm" "klm       " "klm"
"xyz " "xyz       " "xyz "
"xyz " "xyz       " "xyz "

> SELECT * FROM t2;
" abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï " " abc @öÜèßï "

> SELECT * FROM t3;
"abc " "abc " "abc "

> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3) FROM t3 LIMIT 1;
"text" "text" "text"

> SELECT * FROM t4;
"aaa"
"abc"

$ mysql-execute name=mysql
ALTER TABLE t4 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
UPDATE t4 SET f1 = 'äöü' WHERE f1 = 'aaa';

> SELECT * FROM t4;
"äöü"
"abc"
