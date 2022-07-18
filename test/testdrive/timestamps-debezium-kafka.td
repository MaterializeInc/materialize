# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema=[
  {
    "type": "array",
    "items": {
      "type": "record",
      "name": "update",
      "namespace": "com.materialize.cdc",
      "fields": [
        {
          "name": "data",
          "type": {
            "type": "record",
            "name": "data",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          }
        },
        {
          "name": "time",
          "type": "long"
        },
        {
          "name": "diff",
          "type": "long"
        }
      ]
    }
  },
  {
    "type": "record",
    "name": "progress",
    "namespace": "com.materialize.cdc",
    "fields": [
      {
        "name": "lower",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "upper",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "counts",
        "type": {
          "type": "array",
          "items": {
            "type": "record",
            "name": "counts",
            "fields": [
              {
                "name": "time",
                "type": "long"
              },
              {
                "name": "count",
                "type": "long"
              }
            ]
          }
        }
      }
    ]
  }
  ]

$ kafka-create-topic topic=foo

$ kafka-create-topic topic=bar

$ kafka-ingest format=avro topic=foo schema=${schema}
{"array":[{"data":{"a":1,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":2,"b":2},"time":1,"diff":1}]}

$ kafka-ingest format=avro topic=bar schema=${schema}
{"array":[{"data":{"a":10,"b":1},"time":1,"diff":1}]}

> CREATE SOURCE data_foo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-foo-${testdrive.seed}'
    WITH (timeline='user')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE MATERIALIZE

> CREATE SOURCE data_bar
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bar-${testdrive.seed}'
    WITH (timeline='user')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE MATERIALIZE


> CREATE MATERIALIZED VIEW foo AS SELECT b, sum(a) FROM data_foo GROUP BY b

> CREATE MATERIALIZED VIEW bar AS SELECT b, sum(a) FROM data_bar GROUP BY b

> CREATE MATERIALIZED VIEW join (b, foo_sum, bar_sum) AS SELECT * FROM foo JOIN bar USING (b);

# Verify that we don't see any data yet. We can't use `set-sql-timeout` here
# because the SELECT is blocked in the coordinator. For the same reason we
# can't use `DECLARE c CURSOR FOR SELECT ...` (see #10763).

> BEGIN

> DECLARE c CURSOR FOR TAIL (SELECT * FROM bar)

> FETCH ALL c WITH (timeout = '2s');

> ROLLBACK

> BEGIN

> DECLARE c CURSOR FOR TAIL (SELECT * FROM foo)

> FETCH ALL c WITH (timeout = '2s');

> ROLLBACK

> BEGIN

> DECLARE c CURSOR FOR TAIL (SELECT * FROM join)

> FETCH ALL c WITH (timeout = '2s');

> ROLLBACK

$ kafka-ingest format=avro topic=foo schema=${schema}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[2],"counts":[{"time":1,"count":2}]}}

$ kafka-ingest format=avro topic=bar schema=${schema}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[2],"counts":[{"time":1,"count":1}]}}

> SELECT * FROM data_foo;
a  b
----
1  1
2  2


> SELECT * FROM foo;
b  sum
------
1  1
2  2

> SELECT * FROM data_bar;
a   b
-----
10  1

> SELECT * FROM bar;
b  sum
------
1  10

> SELECT * FROM join;
b foo_sum bar_sum
-----------------
1  1  10

$ kafka-ingest format=avro topic=foo schema=${schema}
{"array":[{"data":{"a":3,"b":3},"time":2,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[2],"upper":[3],"counts":[{"time":2,"count":2}]}}

$ kafka-ingest format=avro topic=bar schema=${schema}
{"array":[{"data":{"a":30,"b":3},"time":2,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[2],"upper":[3],"counts":[{"time":2,"count":1}]}}

> SELECT * FROM foo;
b  sum
------
1  1
2  2

> SELECT * FROM bar;
b  sum
------
1  10

# Under weaker isolation levels we can see the row at time 2 before all objects have closed time 2.
> SET transaction_isolation = SERIALIZABLE

> SELECT * FROM bar;
b  sum
------
1  10
3  30

> SET transaction_isolation = STRICT_SERIALIZABLE

> SELECT * FROM join;
b foo_sum bar_sum
-----------------
1  1  10

$ kafka-ingest format=avro topic=foo schema=${schema}
{"array":[{"data":{"a":4,"b":4},"time":2,"diff":1}]}

> SELECT * FROM foo;
b  sum
------
1  1
2  2
3  3
4  4

> SELECT * FROM bar;
b  sum
------
1  10
3  30

> SELECT * FROM join;
b foo_sum bar_sum
-----------------
1  1  10
3  3  30
