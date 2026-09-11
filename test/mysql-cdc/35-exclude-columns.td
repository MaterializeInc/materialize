# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test mysql TEXT COLUMNS support
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'


> CREATE CONNECTION mysqc TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 INTEGER, f2 GEOMETRY, f3 POINT, f4 VARCHAR(64));

INSERT INTO t1 VALUES (1, ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), ST_GeomFromText('POINT(1 1)'), 'test');

CREATE TABLE accounts (id INTEGER NOT NULL, title_id INTEGER, country_id INTEGER NOT NULL, email varchar(80) NOT NULL, alternate_email varchar(80), password varchar(80), first_name varchar(40), middle_name varchar(40), last_name varchar(40), display_name varchar(100), profile_image varchar(80) NOT NULL, language varchar(5) NOT NULL, gender text NOT NULL, mobile_phone varchar(20), birthdate date, id_phrase varchar(45), admin int4, verified int4, theme text, status text, created timestamp NOT NULL, modified timestamp NOT NULL, deleted timestamp);

INSERT INTO accounts VALUES (1, 10, 1, 'alice@example.com', 'alice.alt@example.com', 'hashed_pw_1', 'Alice', 'M', 'Smith', 'Alice Smith', 'images/alice.jpg', 'en', 'female', '555-1234', '1990-03-15', 'phrase1', 0, 1, 'default', 'active', '2024-01-10 08:00:00', '2024-01-10 08:00:00', NULL);
INSERT INTO accounts VALUES (2, 11, 2, 'bob@example.com', NULL, 'hashed_pw_2', 'Bob', NULL, 'Jones', 'Bob Jones', 'images/bob.jpg', 'fr', 'male', '555-5678', '1985-07-22', 'phrase2', 0, 1, 'dark', 'active', '2024-02-14 09:30:00', '2024-02-14 09:30:00', NULL);
INSERT INTO accounts VALUES (3, 12, 3, 'carol@example.com', 'carol.work@example.com', 'hashed_pw_3', 'Carol', 'A', 'Williams', 'Carol Williams', 'images/carol.jpg', 'de', 'female', NULL, '1992-11-05', NULL, 1, 0, 'light', 'inactive', '2024-03-20 14:15:00', '2024-04-01 10:00:00', '2024-04-01 10:00:00');

> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
! CREATE TABLE t1 FROM SOURCE da (REFERENCE public.t1);
contains: unsupported type

! CREATE TABLE t1 FROM SOURCE da (REFERENCE public.t1) WITH (TEXT COLUMNS (f2), EXCLUDE COLUMNS (f2, f3));
contains: duplicated column name references in table

> CREATE TABLE t1 FROM SOURCE da (REFERENCE public.t1) WITH (EXCLUDE COLUMNS (f2, f3));

> CREATE TABLE accounts FROM SOURCE da (REFERENCE public.accounts) WITH (EXCLUDE COLUMNS (password));

> SELECT id, email, first_name, last_name, birthdate, deleted FROM accounts ORDER BY id;
1 "alice@example.com" "Alice" "Smith" "1990-03-15" <null>
2 "bob@example.com" "Bob" "Jones" "1985-07-22" <null>
3 "carol@example.com" "Carol" "Williams" "1992-11-05" "2024-04-01 10:00:00"

! SELECT password FROM accounts;
contains: column "password" does not exist

# Insert new accounts post-snapshot
$ mysql-execute name=mysql
USE public;
INSERT INTO accounts VALUES (4, 13, 4, 'dave@example.com', NULL, 'hashed_pw_4', 'Dave', NULL, 'Brown', 'Dave Brown', 'images/dave.jpg', 'es', 'male', '555-9012', '1988-06-30', 'phrase4', 0, 1, 'default', 'active', '2024-05-01 11:00:00', '2024-05-01 11:00:00', NULL);

> SELECT id, email, first_name, last_name, birthdate, deleted FROM accounts ORDER BY id;
1 "alice@example.com" "Alice" "Smith" "1990-03-15" <null>
2 "bob@example.com" "Bob" "Jones" "1985-07-22" <null>
3 "carol@example.com" "Carol" "Williams" "1992-11-05" "2024-04-01 10:00:00"
4 "dave@example.com" "Dave" "Brown" "1988-06-30" <null>

# Insert the same data post-snapshot
$ mysql-execute name=mysql
USE public;
INSERT INTO t1 SELECT * FROM t1;

> SELECT * FROM t1;
1 "test"
1 "test"

> SELECT f4 FROM t1;
"test"
"test"

$ set-regex match="DETAILS = '[a-f0-9]+'" replacement=<DETAILS>

>[version<2600700] SHOW CREATE TABLE t1;
materialize.public.t1 "CREATE TABLE materialize.public.t1 (f1 pg_catalog.int4, f4 pg_catalog.varchar(64)) FROM SOURCE materialize.public.da (REFERENCE = public.t1) WITH (EXCLUDE COLUMNS = (f2, f3), <DETAILS>);"

>[2600700<=version<2603000] SHOW CREATE TABLE t1;
materialize.public.t1 "CREATE TABLE materialize.public.t1 (f1 pg_catalog.int4, f4 pg_catalog.varchar(64))\nFROM SOURCE materialize.public.da (REFERENCE = public.t1)\nWITH (\n    EXCLUDE COLUMNS = (f2, f3),\n    <DETAILS>\n);"

>[version>=2603000] SHOW CREATE TABLE t1;
materialize.public.t1 "CREATE TABLE materialize.public.t1\nFROM SOURCE materialize.public.da (REFERENCE = public.t1)\nWITH (EXCLUDE COLUMNS = (f2, f3));"

# Regression test for SQL-392: the full `SHOW CREATE TABLE` output (no derived
# column list, bare `EXCLUDE COLUMNS`, `REFERENCE = ...`) must round-trip.
>[version>=2603000] CREATE TABLE t1_roundtrip FROM SOURCE da (REFERENCE = public.t1) WITH (EXCLUDE COLUMNS = (f2, f3));

>[version>=2603000] DROP TABLE t1_roundtrip;

! SELECT f2 FROM t1;
contains:column "f2" does not exist

# Remove one of the ignored columns, and we should still error
$ mysql-execute name=mysql
ALTER TABLE t1 DROP COLUMN f2;

> select * from t1;
1 "test"
1 "test"

# A key over an excluded column is not recorded as a Materialize key, so the
# table can be created and the upstream index can later be dropped without
# stalling it.
$ mysql-execute name=mysql
USE public;
CREATE TABLE t2 (f1 INTEGER PRIMARY KEY, f2 INTEGER NOT NULL, f3 VARCHAR(64), UNIQUE KEY uq_f2 (f2), UNIQUE KEY uq_f1_f2 (f1, f2));
INSERT INTO t2 VALUES (1, 10, 'a');

> CREATE TABLE t2 FROM SOURCE da (REFERENCE public.t2) WITH (EXCLUDE COLUMNS (f2));

> SELECT * FROM t2;
1 a

> CREATE DEFAULT INDEX ON t2;
> SELECT key FROM (SHOW INDEXES ON t2);
{f1}

$ mysql-execute name=mysql
USE public;
ALTER TABLE t2 DROP INDEX uq_f2;
ALTER TABLE t2 DROP INDEX uq_f1_f2;
INSERT INTO t2 VALUES (2, 20, 'b');

> SELECT * FROM t2;
1 a
2 b

# The same holds when the key also spans a nullable included column, a shape
# that constraint planning does not otherwise validate.
$ mysql-execute name=mysql
USE public;
CREATE TABLE t3 (f1 INTEGER PRIMARY KEY, a INTEGER, b INTEGER NOT NULL, UNIQUE KEY uq_ab (a, b));
INSERT INTO t3 VALUES (1, NULL, 10);

> CREATE TABLE t3 FROM SOURCE da (REFERENCE public.t3) WITH (EXCLUDE COLUMNS (b));

> SELECT * FROM t3;
1 <null>

> CREATE DEFAULT INDEX ON t3;
> SELECT key FROM (SHOW INDEXES ON t3);
{f1}

$ mysql-execute name=mysql
USE public;
ALTER TABLE t3 DROP INDEX uq_ab;
INSERT INTO t3 VALUES (2, NULL, 20);

> SELECT * FROM t3;
1 <null>
2 <null>
