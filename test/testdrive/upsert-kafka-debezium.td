# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set keyschema={
    "type": "record",
    "name": "row",
    "fields": [
        {"name": "key", "type": "string"}
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
                "name": "animal",
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

$ kafka-create-topic topic=dbzupsert

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"key": "scale"} {"before": {"row": {"animal": "fish"}}, "after": {"row": {"animal": "lizard"}}}


> CREATE MATERIALIZED SOURCE doin_upsert
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DBZUPSERT FORMAT AVRO USING SCHEMA '${keyschema}'

> SELECT * FROM doin_upsert
animal
----
lizard
