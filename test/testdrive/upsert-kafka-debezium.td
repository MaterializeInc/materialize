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
                "name": "creature",
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
{"key": "scale"} {"before": {"row": {"creature": "fish"}}, "after": {"row": {"creature": "lizard"}}}


> CREATE MATERIALIZED SOURCE doin_upsert
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DBZUPSERT FORMAT AVRO USING SCHEMA '${keyschema}'

> SELECT * FROM doin_upsert
creature
----
lizard

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"key": "scale"} {"before": {"row": {"creature": "lizard"}}, "after": {"row": {"creature": "dino"}}}

> SELECT * FROM doin_upsert
creature
----
dino

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"key": "feather"} {"before": null, "after": {"row": {"creature": "archeopteryx"}}}
{"key": "feather"} {"before": {"row": {"creature": "archeopteryx"}}, "after": {"row": {"creature": "velociraptor"}}}

> SELECT * FROM doin_upsert ORDER BY creature
creature
----
dino
velociraptor

> CREATE MATERIALIZED SOURCE upsert_full_dedupe
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  WITH (deduplication = 'full')
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DBZUPSERT FORMAT AVRO USING SCHEMA '${keyschema}'

> SELECT * FROM upsert_no_dedupe ORDER BY creature
creature
----
dino
velociraptor
