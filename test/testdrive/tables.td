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

! SHOW CREATE TABLE t;
unknown catalog item 't'

> CREATE TABLE t (a int, b text NOT NULL)

> SHOW CREATE TABLE t;
Table   "Create Table"
------------------------
materialize.public.t  "CREATE TABLE \"materialize\".\"public\".\"t\" (\"a\" int, \"b\" text NOT NULL)"

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

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "id", "type": "string"},
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"id": "valid1", "a": 2, "b": 1}}

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM t CROSS JOIN data
1  a  valid1  2  1

# We don't actually care about these results. We just want to ensure that
# joining a table with a logging source selects some reasonable timestamp,
# rather than e.g. blocking forever, or producing an error about being
# unable to select a timestamp.
> SELECT * FROM t CROSS JOIN mz_dataflow_operators LIMIT 0

> DROP SOURCE data

! INSERT INTO t (a) VALUES (1, 'a');
INSERT statement with specified columns not yet supported

! INSERT INTO t (b, a) VALUES (1, 'a');
INSERT statement with specified columns not yet supported

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

> CREATE TABLE t (a int, b text NOT NULL)

> SELECT * FROM t;

! CREATE TABLE s (a int primary key);
CREATE TABLE with column constraint: PRIMARY KEY
