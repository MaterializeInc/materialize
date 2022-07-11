# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that filter and demand information are properly progatated from a view
# down to an unmaterialized source.

$ set schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": ["long", "null"]},
      {"name": "b", "type": ["long", "null"]},
      {"name": "c", "type": ["long", "null"]},
      {"name": "d", "type": ["long", "null"]}
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema}
{"a": {"long": 1}, "b": {"long": 1}, "c": {"long": 3}, "d": {"long": 4}}
{"a": {"long": 2}, "b": {"long": 1}, "c": {"long": 5}, "d": {"long": 4}}
{"a": {"long": 3}, "b": {"long": 1}, "c": {"long": 3}, "d": {"long": 5}}
{"a": {"long": 1}, "b": {"long": 2}, "c": {"long": 2}, "d": {"long": 3}}

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

$ set-regex match=u\d+ replacement=UID

# basic test: pushing filters down to sources

> CREATE MATERIALIZED VIEW mv as SELECT * from data where a = 1 and d = 3;

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Filter (#0 = 1), (#3 = 3)
| Project (#0..=#3)

Query:
%0 =
| Get materialize.public.data (UID)
| Filter (#0 = 1), (#3 = 3)

> SELECT * FROM mv
1 2 2 3

> DROP VIEW mv;

# basic test: pushing demand down to sources

> CREATE MATERIALIZED VIEW mv as SELECT b from data where b = 1;

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Filter (#1 = 1)
| Project (#1)

Query:
%0 =
| Get materialize.public.data (UID)
| Filter (#1 = 1)
| Project (#1)

> SELECT * FROM mv
1
1
1

> DROP VIEW mv;

> CREATE VIEW inner_view as SELECT a, b, d from data where d = 4;

# Filter gets pushed through intervening view.

> CREATE MATERIALIZED VIEW mv as SELECT b from inner_view where a = 1

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Filter (#0 = 1), (#3 = 4)
| Project (#0, #1, #3)

Query:
%0 =
| Get materialize.public.data (UID)
| Filter (#0 = 1), (#3 = 4)
| Project (#1)

> SELECT * FROM mv
1

> DROP VIEW mv;

# Demand gets pushed through intervening view.

> CREATE MATERIALIZED VIEW mv as SELECT d from inner_view where a = 1

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Filter (#0 = 1), (#3 = 4)
| Project (#0, #3)

Query:
%0 =
| Get materialize.public.data (UID)
| Filter (#0 = 1), (#3 = 4)
| Project (#3)

> SELECT * FROM mv
4

> DROP VIEW mv;

> CREATE MATERIALIZED VIEW mv as SELECT s1.a from data s1, data s2 where s1.a = s2.b and s2.d = 4

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Project (#0, #1, #3)

Query:
%0 =
| Get materialize.public.data (UID)
| Filter !(isnull(#0))
| Project (#0)
| ArrangeBy (#0)

%1 =
| Get materialize.public.data (UID)
| Filter (#3 = 4), !(isnull(#1))
| Project (#1)

%2 =
| Join %0 %1 (= #0 #1)
| | implementation = Differential %1 %0.(#0)
| Project (#0)

> SELECT * FROM mv
1
1
1
1

> DROP VIEW mv;

# filters and demand can be inferred in more complicated queries

> CREATE MATERIALIZED VIEW mv as SELECT s2.a from data s1, data s2 where s1.a = s2.b and s2.d = 4 and s1.d = 4

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Filter (#3 = 4)
| Project (#0, #1, #3)

Query:
%0 =
| Get materialize.public.data (UID)
| Filter (#3 = 4), !(isnull(#0))
| Project (#0)
| ArrangeBy (#0)

%1 =
| Get materialize.public.data (UID)
| Filter (#3 = 4), !(isnull(#1))
| Project (#0, #1)

%2 =
| Join %0 %1 (= #0 #2)
| | implementation = Differential %1 %0.(#0)
| Project (#1)

> SELECT * FROM mv
1
2

> DROP VIEW mv;

> CREATE MATERIALIZED VIEW mv as SELECT s2.c from data s1, data s2 where s1.a = s2.a

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Filter !(isnull(#0))
| Project (#0, #2)

Query:
%0 = Let l0 =
| Get materialize.public.data (UID)
| Filter !(isnull(#0))

%1 =
| Get %0 (l0)
| Project (#0)
| ArrangeBy (#0)

%2 =
| Get %0 (l0)
| Project (#0, #2)

%3 =
| Join %1 %2 (= #0 #1)
| | implementation = Differential %2 %1.(#0)
| Project (#2)

> SELECT * FROM mv
3
5
3
2
3
2

> DROP VIEW mv;

> CREATE MATERIALIZED VIEW mv as SELECT * FROM (SELECT a, sum(b) FROM data GROUP BY a UNION ALL SELECT a, (a + c)::numeric FROM data) WHERE a = 1

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Filter (#0 = 1)
| Project (#0..=#2)

Query:
%0 = Let l0 =
| Get materialize.public.data (UID)
| Filter (#0 = 1)

%1 =
| Get %0 (l0)
| Project (#1)
| Reduce group=()
| | agg sum(#0)
| Map 1
| Project (#1, #0)

%2 =
| Get %0 (l0)
| Map bigint_to_numeric((1 + #2))
| Project (#0, #4)

%3 =
| Union %1 %2

> SELECT * FROM mv
1 3
1 3
1 4

> DROP VIEW mv;

# multiple source test

$ kafka-create-topic topic=data2

$ kafka-ingest format=avro topic=data2 schema=${schema}
{"a": {"long": 3}, "b": {"long": 2}, "c": null, "d": {"long": 4}}
{"a": {"long": 2}, "b": {"long": 1}, "c": {"long": 5}, "d": null}

> CREATE SOURCE data2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data2-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED VIEW mv as SELECT a, c FROM data EXCEPT ALL SELECT a, c FROM data2 where d is null

? EXPLAIN PLAN FOR VIEW mv;
Source materialize.public.data (UID):
| Project (#0, #2)

Source materialize.public.data2 (UID):
| Filter isnull(#3)
| Project (#0, #2, #3)

Query:
%0 =
| Get materialize.public.data (UID)
| Project (#0, #2)

%1 =
| Get materialize.public.data2 (UID)
| Filter isnull(#3)
| Project (#0, #2)
| Negate

%2 =
| Union %0 %1
| Threshold

> SELECT * FROM mv
1 2
1 3
3 3

> DROP VIEW mv;
