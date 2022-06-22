# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Ingest records before restart and then delete some of them post-restart
#

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "long"},
        {"name": "key2", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=include-key

$ kafka-ingest format=avro topic=include-key key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": ${kafka-ingest.iteration}, "key2": 1} {"f2": ${kafka-ingest.iteration}}

> CREATE MATERIALIZED SOURCE include_key
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-include-key-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  INCLUDE KEY AS named
  ENVELOPE UPSERT

> SELECT COUNT(*) FROM include_key;
10000
