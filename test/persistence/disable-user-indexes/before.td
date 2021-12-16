# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Insert 10K values prior to restart
#

$ set count=10000

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"string"}
        ]
    }

$ kafka-create-topic topic=disable-user-indexes

$ kafka-ingest format=avro topic=disable-user-indexes key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=${count}
{"f1": "a${kafka-ingest.iteration}"} {"f2": "a${kafka-ingest.iteration}"}

> CREATE MATERIALIZED SOURCE disable_user_indexes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-disable-user-indexes-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> SELECT COUNT(*) = ${count} FROM disable_user_indexes;
true
