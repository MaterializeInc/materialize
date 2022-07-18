
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Attempt to write a union into a non-union source
#

$ set union-int={"type": "record", "name": "schema_union", "fields": [ {"name": "f1", "type": [ "int" ] } ] }
$ set int={"type": "record", "name": "schema_union", "fields": [ {"name": "f1", "type": "int" } ] }

$ kafka-create-topic topic=resolution-unions

$ kafka-ingest format=avro topic=resolution-unions schema=${int} publish=true timestamp=1
{"f1": 234 }

> CREATE SOURCE resolution_unions
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-resolution-unions-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=resolution-unions schema=${union-int} publish=true timestamp=2
{"f1": {"int": 123 } }

> SELECT f1 FROM resolution_unions
234
123
