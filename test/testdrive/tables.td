# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
expected to insert 2 values, found 1

! INSERT INTO t VALUES (1, NULL);
trying to insert NULL value into non-nullable column b

! INSERT INTO t VALUES ('d', 4);
value 'd' is not compatible with type Int32

> INSERT INTO t VALUES (NULL, 'd');

> SELECT * FROM t;
a       b
------------
1       "a"
2       "b"
3       "c"
<null>  "d"

> DROP TABLE t;

> DROP TABLE IF EXISTS t;

! SELECT * from t;
unknown catalog item 't'

! SHOW INDEXES FROM t;
unknown catalog item 't'

> SHOW TABLES;

> SHOW SOURCES;

> CREATE TABLE t (a int, b text NOT NULL)

> SELECT * FROM t;

! CREATE TABLE s (a int primary key);
CREATE TABLE with column constraint: PRIMARY KEY
