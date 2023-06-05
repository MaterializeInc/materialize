# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
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

$ kafka-create-topic topic=upsert

$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "MUCHMUCHMUCHLONGERVALUE", "f2": 1}

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE CONNECTION c_conn
  FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}'

> CREATE SOURCE upsert
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION c_conn
  ENVELOPE UPSERT WITH (SIZE = '1')

> SELECT f2 from upsert
1

$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "s", "f2": 2}

> SELECT f2 from upsert
2
