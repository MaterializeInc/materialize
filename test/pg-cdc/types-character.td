# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the various character data types
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

# Insert data pre-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (f1 VARCHAR(10), f2 CHAR(10), f3 TEXT, f4 varchar);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 VALUES ('abc', 'abc', 'abc', 'abc');
INSERT INTO t1 VALUES ('abc ', 'abc ', 'abc ', 'abc ');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) > 0 FROM mz_source;
true

> CREATE VIEWS FROM SOURCE mz_source;

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3), pg_typeof(f4) FROM t1 LIMIT 1;
"character varying" "character" "text" "character varying"

> SELECT * FROM t1;
"abc" "abc       " "abc" "abc"
"abc" "abc       " "abc" "abc"
"abc " "abc       " "abc " "abc "
"abc " "abc       " "abc " "abc "

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE t1 SET f1 = 'klm', f2 = 'klm', f3 = 'klm', f4 = 'klm' WHERE f1 = 'abc';
UPDATE t1 SET f1 = 'xyz ', f2 = 'xyz ', f3 = 'xyz ', f4 = 'xyz ' WHERE f1 = 'abc ';

> SELECT * FROM t1;
"klm" "klm       " "klm" "klm"
"klm" "klm       " "klm" "klm"
"xyz " "xyz       " "xyz " "xyz "
"xyz " "xyz       " "xyz " "xyz "
