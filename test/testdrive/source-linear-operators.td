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

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE data
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'

$ set-regex match=u\d+ replacement=UID

# basic test: pushing filters down to sources

> CREATE VIEW v as SELECT * from data where a = 1 and d = 3;
> CREATE DEFAULT INDEX ON v;

? EXPLAIN VIEW v;
materialize.public.v:
  Filter (#0 = 1) AND (#3 = 3)
    Get materialize.public.data

Source materialize.public.data
  filter=((#0 = 1) AND (#3 = 3))

> SELECT * FROM v
1 2 2 3

> DROP VIEW v;

# basic test: pushing demand down to sources

> CREATE VIEW v as SELECT b from data where b = 1;
> CREATE DEFAULT INDEX ON v;

? EXPLAIN VIEW v;
materialize.public.v:
  Project (#1)
    Filter (#1 = 1)
      Get materialize.public.data

Source materialize.public.data
  filter=((#1 = 1))


> SELECT * FROM v
1
1
1

> DROP VIEW v;

> CREATE VIEW inner_view as SELECT a, b, d from data where d = 4;

# Filter gets pushed through intervening view.

> CREATE VIEW v as SELECT b from inner_view where a = 1
> CREATE DEFAULT INDEX ON v

? EXPLAIN VIEW v;
materialize.public.v:
  Project (#1)
    Filter (#0 = 1) AND (#3 = 4)
      Get materialize.public.data

Source materialize.public.data
  filter=((#0 = 1) AND (#3 = 4))

> SELECT * FROM v
1

> DROP VIEW v;

# Demand gets pushed through intervening view.

> CREATE VIEW v as SELECT d from inner_view where a = 1;
> CREATE DEFAULT INDEX ON v;

? EXPLAIN VIEW v;
materialize.public.v:
  Project (#3)
    Filter (#0 = 1) AND (#3 = 4)
      Get materialize.public.data

Source materialize.public.data
  filter=((#0 = 1) AND (#3 = 4))

> SELECT * FROM v
4

> DROP VIEW v;

> CREATE VIEW v as SELECT s1.a from data s1, data s2 where s1.a = s2.b and s2.d = 4;
> CREATE DEFAULT INDEX ON v;

? EXPLAIN VIEW v;
materialize.public.v:
  Project (#0)
    Join on=(#0 = #1) type=differential
      ArrangeBy keys=[[#0]]
        Project (#0)
          Filter (#0) IS NOT NULL
            Get materialize.public.data
      ArrangeBy keys=[[#0]]
        Project (#1)
          Filter (#3 = 4) AND (#1) IS NOT NULL
            Get materialize.public.data

> SELECT * FROM v
1
1
1
1

> DROP VIEW v;

# filters and demand can be inferred in more complicated queries

> CREATE VIEW v as SELECT s2.a from data s1, data s2 where s1.a = s2.b and s2.d = 4 and s1.d = 4;
> CREATE DEFAULT INDEX ON v;

? EXPLAIN VIEW v;
materialize.public.v:
  Project (#1)
    Join on=(#0 = #2) type=differential
      ArrangeBy keys=[[#0]]
        Project (#0)
          Filter (#3 = 4) AND (#0) IS NOT NULL
            Get materialize.public.data
      ArrangeBy keys=[[#1]]
        Project (#0, #1)
          Filter (#3 = 4) AND (#1) IS NOT NULL
            Get materialize.public.data

Source materialize.public.data
  filter=((#3 = 4))

> SELECT * FROM v
1
2

> DROP VIEW v;

> CREATE VIEW v as SELECT s2.c from data s1, data s2 where s1.a = s2.a
> CREATE DEFAULT INDEX ON v

? EXPLAIN VIEW v;
materialize.public.v:
  Return
    Project (#2)
      Join on=(#0 = #1) type=differential
        ArrangeBy keys=[[#0]]
          Project (#0)
            Get l0
        ArrangeBy keys=[[#0]]
          Project (#0, #2)
            Get l0
  With
    cte l0 =
      Filter (#0) IS NOT NULL
        Get materialize.public.data

Source materialize.public.data
  filter=((#0) IS NOT NULL)

> SELECT * FROM v
3
5
3
2
3
2

> DROP VIEW v;

> CREATE VIEW v as SELECT * FROM (SELECT a, sum(b) FROM data GROUP BY a UNION ALL SELECT a, (a + c)::numeric FROM data) WHERE a = 1;
> CREATE DEFAULT INDEX ON v;

? EXPLAIN VIEW v;
materialize.public.v:
  Union
    Project (#1, #0)
      Map (1)
        Reduce aggregates=[sum(#0)] monotonic
          Project (#1)
            Filter (#0 = 1)
              Get materialize.public.data
    Project (#0, #4)
      Filter (#0 = 1)
        Map (bigint_to_numeric((1 + #2)))
          Get materialize.public.data

Source materialize.public.data
  filter=((#0 = 1))

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

> CREATE SOURCE data2
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data2-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE VIEW v as SELECT a, c FROM data EXCEPT ALL SELECT a, c FROM data2 where d is null
> CREATE DEFAULT INDEX ON v

? EXPLAIN VIEW v;
materialize.public.v:
  Threshold
    Union
      Project (#0, #2)
        Get materialize.public.data
      Negate
        Project (#0, #2)
          Filter (#3) IS NULL
            Get materialize.public.data2

Source materialize.public.data2
  filter=((#3) IS NULL)

> SELECT * FROM v
1 2
1 3
3 3

> DROP VIEW v;
