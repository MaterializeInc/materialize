# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s

#
# Ingest strings at the upstream VARCHAR character-length limit using
# multi-codepoint graphemes — flag emoji, which a human sees as a single
# grapheme but MySQL counts as 2 characters (each Regional Indicator Symbol
# is its own code point). Then widen the column by one character and try to
# ingest a value at the new limit. Exercises the character-vs-grapheme
# distinction end-to-end and asserts that widening a tracked VARCHAR column
# surfaces as an incompatible schema change.
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
# Disable strict mode so that over-length VARCHAR inserts are silently truncated
# rather than rejected with an error. Scoped to this session.
SET SESSION sql_mode = '';
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

# A utf8mb4 VARCHAR(40) holds up to 40 characters regardless of how many bytes
# each character occupies (1–4 bytes per character in utf8mb4).
CREATE TABLE t1 (f1 VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, f2 VARCHAR(5) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'en') DEFAULT CHARSET = utf8mb3 COLLATE = utf8mb3_unicode_ci;

# 40-character value composed of 20 US-flag graphemes. Each 🇺🇸 is a single
# visual grapheme but MySQL counts it as 2 characters — the flag is encoded
# as two Regional Indicator Symbol code points (U+1F1FA + U+1F1F8), each 4
# bytes in UTF-8.
# Total: 40 characters, 160 bytes. Pre-snapshot row.
INSERT INTO t1 (f1) VALUES (REPEAT('🇺🇸', 20));

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);

> SELECT pg_typeof(f1) FROM t1 LIMIT 1;
"character varying"

> SELECT char_length(f1), octet_length(f1) FROM t1;
40 160

> SELECT f1 FROM t1;
🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸🇺🇸

# Insert the same 40-character value post-snapshot to exercise the binlog path.
$ mysql-execute name=mysql
INSERT INTO t1 VALUES (REPEAT('🇺🇸', 20), 'bc');
UPDATE t1 SET f1 = REPEAT('🇺🇸', 19) WHERE f2 = 'en';

> SELECT char_length(f1), octet_length(f1), count(*) FROM t1 GROUP BY 1, 2;
40 160 1
38 152 1

# Widen the upstream column to VARCHAR(41) and insert a 41-character value
# (20 × 🇺🇸 + 'a' = 40 + 1 = 41 chars, 161 bytes). An odd character count
# can't be made up of only 2-character graphemes, so one ASCII char pads
# the end.
$ mysql-execute name=mysql
ALTER TABLE t1 MODIFY f1 VARCHAR(41) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
INSERT INTO t1 VALUES (CONCAT(REPEAT('🇺🇸', 20), 'a'), 'cd');

# Narrowing the tracked VARCHAR's max_length is what makes the schemas
# incompatible — the source's cached descriptor pins max_length at 40, and
# schema verification rejects any value whose declared width differs.
! SELECT * FROM t1;
contains:incompatible schema change

> DROP SOURCE mz_source CASCADE;
