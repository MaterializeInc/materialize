# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT jsonb_agg(1)
jsonb_agg
-----------
[1.0]

$ set aggs-schema={
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
              {"name": "num", "type": "int"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=aggs

$ kafka-ingest format=avro topic=aggs schema=${aggs-schema} timestamp=1
{"before": null, "after": {"num": 1}}
{"before": null, "after": {"num": 2}}
{"before": null, "after": {"num": 3}}

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-aggs-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${aggs-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM data
num
----
1
2
3

> SELECT jsonb_agg(data.num) from data;
[2.0,3.0,1.0]

! SELECT jsonb_agg(1, 2)
jsonb_agg function only takes one argument

# todo@jldlaughlin: Fix test when #2414 is implemented
! SELECT jsonb_agg((1, 2))
Parse error:

> DROP SOURCE data

# Test Avro arrays are interpretable as Json

$ set nested-avro-schema={
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
                {
                  "name": "a", "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "inner",
                      "fields": [
                        {
                          "name": "f1",
                          "type": "long"
                        },
                        {
                          "name": "f2",
                          "type": "string"
                        }
                      ]
                    }
                  }
                }
              ]
            },
            "null"
          ]
        },
        { "name": "after", "type": ["row", "null"] }
      ]
    }

$ kafka-create-topic topic=nested_avro

$ kafka-ingest format=avro topic=nested_avro schema=${nested-avro-schema} timestamp=1
{"before": null, "after": {"a": [{"f1": 123, "f2": "hello"}, {"f1": 456, "f2": "world"}]}}

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-nested_avro-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${nested-avro-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM data
a
---
"[{\"f1\":123.0,\"f2\":\"hello\"},{\"f1\":456.0,\"f2\":\"world\"}]"


> DROP SOURCE data
