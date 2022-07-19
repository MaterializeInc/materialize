# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Must be a subset of the keys in the rows AND
# in a different order than the value.
$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "b", "type": "string"},
        {"name": "a", "type": "long"}
    ]
  }

$ set schema={
    "type" : "record",
    "name" : "envelope",
    "fields" : [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {
                  "name": "a",
                  "type": "long"
              },
              {
                "name": "data",
                "type": "string"
              },
              {
                  "name": "b",
                  "type": "string"
              }]
           },
           "null"
         ]
      },
      {
        "name": "after",
        "type": ["row", "null"]
      }
    ]
  }

$ kafka-create-topic topic=dbzupsert partitions=1

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"b": "bdata", "a": 1} {"before": {"row": {"a": 1, "data": "fish", "b": "bdata"}}, "after": {"row": {"a": 1, "data": "fish2", "b": "bdata"}}}

> CREATE SOURCE doin_upsert
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM doin_upsert
a data b
-----------
1 fish2 bdata

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"b": "bdata", "a": 1} {"before": {"row": {"a": 1, "data": "fish2", "b": "bdata"}}, "after": {"row": {"a": 1, "data": "fish3", "b": "bdata"}}}

> SELECT * FROM doin_upsert
a data b
-----------
1 fish3 bdata
