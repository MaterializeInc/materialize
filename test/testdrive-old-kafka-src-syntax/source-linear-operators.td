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

> CREATE SOURCE data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'

$ set-regex match=u\d+ replacement=UID

# basic test: pushing filters down to sources

> CREATE VIEW v as SELECT * from data where a = 1 and d = 3;

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1 2 2 3

> DROP VIEW v;

# basic test: pushing demand down to sources

> CREATE VIEW v as SELECT b from data where b = 1;

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1
1
1

> DROP VIEW v;

> CREATE VIEW inner_view as SELECT a, b, d from data where d = 4;

# Filter gets pushed through intervening view.

> CREATE VIEW v as SELECT b from inner_view where a = 1

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1

> DROP VIEW v;

# Demand gets pushed through intervening view.

> CREATE VIEW v as SELECT d from inner_view where a = 1;

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
4

> DROP VIEW v;

> CREATE VIEW v as SELECT s1.a from data s1, data s2 where s1.a = s2.b and s2.d = 4;

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1
1
1
1

> DROP VIEW v;

# filters and demand can be inferred in more complicated queries

> CREATE VIEW v as SELECT s2.a from data s1, data s2 where s1.a = s2.b and s2.d = 4 and s1.d = 4;

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1
2

> DROP VIEW v;

> CREATE VIEW v as SELECT s2.c from data s1, data s2 where s1.a = s2.a

> CREATE DEFAULT INDEX ON v;
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
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data2-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE VIEW v as SELECT a, c FROM data EXCEPT ALL SELECT a, c FROM data2 where d is null

> CREATE DEFAULT INDEX ON v;
> SELECT * FROM v
1 2
1 3
3 3

> DROP VIEW v;
