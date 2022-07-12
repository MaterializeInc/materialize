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

$ kafka-create-topic topic=input

> CREATE SOURCE input
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE MATERIALIZE

> CREATE SINK output FROM input
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-ingest format=avro topic=input schema=${schema}
{"array":[{"data":{"a":1,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":2,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":3,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":1,"b":2},"time":1,"diff":1}]}
{"array":[{"data":{"a":11,"b":11},"time":2,"diff":1}]}
{"array":[{"data":{"a":22,"b":11},"time":2,"diff":1}]}
{"array":[{"data":{"a":3,"b":4},"time":3,"diff":1}]}
{"array":[{"data":{"a":5,"b":6},"time":3,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[4],"counts":[{"time":1,"count":4},{"time":2,"count":2},{"time":3,"count":2}]}}

$ kafka-verify format=avro sink=materialize.public.output sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}, "transaction": {"id": "1"}}

$ kafka-verify format=avro sink=materialize.public.output sort-messages=true
{"before": null, "after": {"row": {"a": 11, "b": 11}}, "transaction": {"id": "2"}}
{"before": null, "after": {"row": {"a": 22, "b": 11}}, "transaction": {"id": "2"}}

$ kafka-verify format=avro sink=materialize.public.output sort-messages=true
{"before": null, "after": {"row": {"a": 3, "b": 4}}, "transaction": {"id": "3"}}
{"before": null, "after": {"row": {"a": 5, "b": 6}}, "transaction": {"id": "3"}}

# Wait a bit to allow timestamp compaction to happen. We need to ensure that we
# get correct results even with compaction, which re-timestamps earlier data
# at later timestamps upon restarting.

> SELECT mz_internal.mz_sleep(5);
<null>

# TODO: enable when possible
# max-size=0 requires that we compact past the max timestamp when this is first invoked
# $ verify-timestamp-compaction source=input max-size=0 permit-progress=true
