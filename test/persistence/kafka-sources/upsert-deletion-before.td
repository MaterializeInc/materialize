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

$ kafka-ingest format=avro topic=upsert-deletion key-format=avro key-schema=${keyschema} schema=${schema} repeat=10000
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE upsert_deletion
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-upsert-deletion-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> SELECT COUNT(*) FROM upsert_deletion;
10000
