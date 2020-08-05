# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

! INSERT INTO t VALUES (1, 'a');
unknown catalog item 't'

> CREATE TABLE t (a int, b text NOT NULL)

! CREATE TABLE s (a int DEFAULT 1)
CREATE TABLE with column constraint: DEFAULT 1 not yet supported

> SELECT * FROM t;

> SHOW TABLES;
TABLES
------
t

> SHOW SOURCES;
SOURCES
-------
t

> SHOW INDEXES FROM t;
Source_or_view        Key_name                          Column_name  Expression  Null   Seq_in_index
----------------------------------------------------------------------------------------------------------------
materialize.public.t  materialize.public.t_primary_idx  a            <null>      true              1
materialize.public.t  materialize.public.t_primary_idx  b            <null>      false             2

> SHOW COLUMNS in t;
Field      Nullable  Type
-------------------------
a          YES       int4
b          NO        text

> INSERT INTO t VALUES (1, 'a');

> SELECT * FROM t;
a    b
---------
1    "a"

> INSERT INTO t VALUES (2, 'b'), (3, 'c');

> SELECT * FROM t;
a    b
---------
1    "a"
2    "b"
3    "c"

! INSERT INTO t DEFAULT VALUES;
INSERT DEFAULT VALUES not yet supported

! INSERT INTO t SELECT * FROM t;
INSERT body SELECT * FROM t not yet supported

! INSERT INTO t VALUES (1);
INSERT statement specifies 2 columns, but table has 1 columns

! INSERT INTO t VALUES (1, NULL);
NULL value in column b violates not-null constraint

! INSERT INTO t VALUES ('d', 4);
expected type string for column b, found i32

> INSERT INTO t VALUES (NULL, 'd');

> INSERT INTO t VALUES ('4', 'e')

> SELECT * FROM t;
a       b
------------
1       "a"
2       "b"
3       "c"
<null>  "d"
4       "e"

> CREATE TABLE s (a int NOT NULL);

! INSERT INTO s VALUES (1 + NULL);
NULL value in column a violates not-null constraint

> CREATE TABLE v (a timestamptz);

> INSERT INTO v VALUES (now());

! INSERT INTO v VALUES (mz_logical_timestamp());
expected type timestamptz for column a, found decimal(38, 0)

> DROP TABLE IF EXISTS s;

> DROP TABLE t;

> DROP TABLE IF EXISTS t;

! SELECT * from t;
unknown catalog item 't'

! SHOW INDEXES FROM t;
unknown catalog item 't'

> SHOW TABLES;
TABLES
------
v

> SHOW SOURCES;
SOURCES
-------
v

> CREATE TABLE t (a int, b text NOT NULL)

> SELECT * FROM t;

! CREATE TABLE s (a int primary key);
CREATE TABLE with column constraint: PRIMARY KEY
