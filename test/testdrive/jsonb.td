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
jsonb_agg function requires exactly one non-star argument

# todo@jldlaughlin: Fix test when #2414 is implemented
! SELECT jsonb_agg((1, 2))
Parse error:

> DROP SOURCE data
