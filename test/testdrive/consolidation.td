# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set consistency={
     "name": "materialize.byo.consistency",
     "type": "record",
     "fields": [
         {
           "name": "source",
           "type": "string"
         },
         {
           "name": "partition_count",
           "type": "int"
         },
         {
           "name": "partition_id",
           "type": ["int","string"]
         },
         {
            "name": "timestamp",
            "type": "long"
         },
         {
           "name": "offset",
           "type": "long"
         }
     ]
  }


$ set names-schema={
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
              {"name": "num", "type": "long"},
              {"name": "name", "type": "string"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data-consistency

$ kafka-create-topic topic=names

$ kafka-ingest format=avro topic=data-consistency schema=${consistency}
{"source": "names", "partition_count": 1, "partition_id": 0, "timestamp": 1, "offset": 3}


$ kafka-ingest format=avro topic=names schema=${names-schema} timestamp=1
{"before": null, "after": {"num": 1, "name": "one"}}
{"before": {"num": 1, "name": "one"}, "after": {"num": 2, "name": "two"}}
{"before": {"num": 2, "name": "two"}, "after": {"num": 3, "name": "three"}}

> CREATE MATERIALIZED SOURCE names
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-names-${testdrive.seed}'
  with (consistency='data-consistency')
  FORMAT AVRO USING SCHEMA '${names-schema}'
  ENVELOPE DEBEZIUM

> CREATE SINK snk1 FROM names
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.snk1
{"before": null, "after": {"num": 3, "name": "three"}}
