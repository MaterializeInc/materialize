# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This testdrive file uses the deprecated syntax, but is otherwise identical to upsert-kafka-new.td
#
# This file can be deleted when/if we finish the deprecation and perform the removal of the old syntax.

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"},
        {"name": "key2", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=avroavro

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish", "key2": "key2" } {"f1": "fishval", "f2": 1000}

> CREATE SOURCE avroavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-avroavro-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE KEY AS named
  ENVELOPE UPSERT

> SELECT (named).key, (named).key2, f1, f2 from avroavro
key     key2  f1       f2
---------------------------
fish    key2  fishval  1000

$ set keyschemasingle={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ kafka-create-topic topic=single

$ kafka-ingest format=avro topic=single key-format=avro key-schema=${keyschemasingle} schema=${schema}
{"key": "fish" } {"f1": "fishval", "f2": 1000}

> CREATE SOURCE single
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-single-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschemasingle}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE KEY AS named
  ENVELOPE UPSERT

> SELECT named, f1, f2 from single
named   f1       f2
---------------------------
fish    fishval  1000
