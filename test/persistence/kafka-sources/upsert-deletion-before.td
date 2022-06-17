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
        {"name": "f1", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=upsert-deletion

$ kafka-ingest format=avro topic=upsert-deletion key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}

> CREATE MATERIALIZED SOURCE upsert_deletion
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-upsert-deletion-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> SELECT COUNT(*) FROM upsert_deletion;
10000
