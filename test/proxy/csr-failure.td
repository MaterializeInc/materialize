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
            {"name":"f1", "type":"string"}
        ]
    }

$ kafka-create-topic topic=denykey

$ kafka-ingest format=avro topic=denykey key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"key": "fish"} {"f1": "fishval"}

# the specific error here doesn't matter, we just want to make sure we hit the error code path,
# not the key = None codepath
! CREATE MATERIALIZED SOURCE denykey
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-denykey-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT
exact:server error 403: unable to decode error details

$ kafka-create-topic topic=allowkey

# also test the key = None codepath
$ kafka-ingest format=avro topic=allowkey schema=${schema} publish=true
{"f1": "fishval"}

> CREATE MATERIALIZED SOURCE allowkey
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-allowkey-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

> SELECT * from allowkey
f1
-------
fishval
