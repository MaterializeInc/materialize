# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

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

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')

> CREATE TABLE data_tbl FROM SOURCE data (REFERENCE "testdrive-data-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}'
> COMMIT

$ set-regex match=u\d+ replacement=UID

# basic test: pushing filters down to sources

> CREATE VIEW v as SELECT * from data_tbl where a = 1 and d = 3;

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  Filter (#0{a} = 1) AND (#3{d} = 3)
    ReadStorage materialize.public.data_tbl

Source materialize.public.data_tbl
  filter=((#0{a} = 1) AND (#3{d} = 3))

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1 2 2 3

> DROP VIEW v;

# basic test: pushing demand down to sources

> CREATE VIEW v as SELECT b from data_tbl where b = 1;

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  Project (#1)
    Filter (#1{b} = 1)
      ReadStorage materialize.public.data_tbl

Source materialize.public.data_tbl
  filter=((#1{b} = 1))

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1
1
1

> DROP VIEW v;

> CREATE VIEW inner_view as SELECT a, b, d from data_tbl where d = 4;

# Filter gets pushed through intervening view.

> CREATE VIEW v as SELECT b from inner_view where a = 1

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  Project (#1)
    Filter (#0{a} = 1) AND (#3{d} = 4)
      ReadStorage materialize.public.data_tbl

Source materialize.public.data_tbl
  filter=((#0{a} = 1) AND (#3{d} = 4))

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1

> DROP VIEW v;

# Demand gets pushed through intervening view.

> CREATE VIEW v as SELECT d from inner_view where a = 1;

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  Project (#3)
    Filter (#0{a} = 1) AND (#3{d} = 4)
      ReadStorage materialize.public.data_tbl

Source materialize.public.data_tbl
  filter=((#0{a} = 1) AND (#3{d} = 4))

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
4

> DROP VIEW v;

> CREATE VIEW v as SELECT s1.a from data_tbl s1, data_tbl s2 where s1.a = s2.b and s2.d = 4;

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  Project (#0)
    Join on=(#0{a} = #1{b}) type=differential
      ArrangeBy keys=[[#0{a}]]
        Project (#0)
          Filter (#0{a}) IS NOT NULL
            ReadStorage materialize.public.data_tbl
      ArrangeBy keys=[[#0{b}]]
        Project (#1)
          Filter (#3{d} = 4) AND (#1{b}) IS NOT NULL
            ReadStorage materialize.public.data_tbl

Source materialize.public.data_tbl

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1
1
1
1

> DROP VIEW v;

# filters and demand can be inferred in more complicated queries

> CREATE VIEW v as SELECT s2.a from data_tbl s1, data_tbl s2 where s1.a = s2.b and s2.d = 4 and s1.d = 4;

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  Project (#1)
    Join on=(#0{a} = #2{b}) type=differential
      ArrangeBy keys=[[#0{a}]]
        Project (#0)
          Filter (#3{d} = 4) AND (#0{a}) IS NOT NULL
            ReadStorage materialize.public.data_tbl
      ArrangeBy keys=[[#1{b}]]
        Project (#0, #1)
          Filter (#3{d} = 4) AND (#1{b}) IS NOT NULL
            ReadStorage materialize.public.data_tbl

Source materialize.public.data_tbl
  filter=((#3{d} = 4))

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1
2

> DROP VIEW v;

> CREATE VIEW v as SELECT s2.c from data_tbl s1, data_tbl s2 where s1.a = s2.a

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  With
    cte l0 =
      Project (#0, #2)
        Filter (#0{a}) IS NOT NULL
          ReadStorage materialize.public.data_tbl
  Return
    Project (#2)
      Join on=(#0{a} = #1{a}) type=differential
        ArrangeBy keys=[[#0{a}]]
          Project (#0)
            Get l0
        ArrangeBy keys=[[#0{a}]]
          Get l0

Source materialize.public.data_tbl
  filter=((#0{a}) IS NOT NULL)

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
3
5
3
2
3
2

> DROP VIEW v;

> CREATE VIEW v as SELECT * FROM (SELECT a, sum(b) FROM data_tbl GROUP BY a UNION ALL SELECT a, (a + c)::numeric FROM data_tbl) WHERE a = 1;

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  Union
    Project (#1, #0)
      Map (1)
        Reduce aggregates=[sum(#0{b})]
          Project (#1)
            Filter (#0 = 1)
              ReadStorage materialize.public.data_tbl
    Project (#0, #4)
      Filter (#0 = 1)
        Map (bigint_to_numeric((1 + #2{c})))
          ReadStorage materialize.public.data_tbl

Source materialize.public.data_tbl
  filter=((#0 = 1))

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1 3
1 3
1 4

> DROP VIEW v;

# multiple source test

$ kafka-create-topic topic=data2

$ kafka-ingest format=avro topic=data2 schema=${schema}
{"a": {"long": 3}, "b": {"long": 2}, "c": null, "d": {"long": 4}}
{"a": {"long": 2}, "b": {"long": 1}, "c": {"long": 5}, "d": null}

> BEGIN
> CREATE SOURCE data2
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data2-${testdrive.seed}')

> CREATE TABLE data2_tbl FROM SOURCE data2 (REFERENCE "testdrive-data2-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}'
> COMMIT

> CREATE VIEW v as SELECT a, c FROM data_tbl EXCEPT ALL SELECT a, c FROM data2_tbl where d is null

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM v;
Explained Query:
  Threshold
    Union
      Project (#0, #2)
        ReadStorage materialize.public.data_tbl
      Negate
        Project (#0, #2)
          Filter (#3{d}) IS NULL
            ReadStorage materialize.public.data2_tbl

Source materialize.public.data_tbl
Source materialize.public.data2_tbl
  filter=((#3{d}) IS NULL)

Target cluster: quickstart

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1 2
1 3
3 3

> DROP VIEW v;
