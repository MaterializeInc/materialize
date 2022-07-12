# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Allow a writer schema that has a smaller number of columns if the reader schema has specified a default
#

$ set 2columns={"type": "record", "name": "schema_less_columns", "fields": [ {"name": "f1", "type": "string", "default": "default_f1"} , {"name": "f2", "type": "string", "default": "default_f2"}] }
$ set 1column={"type": "record", "name": "schema_less_columns", "fields": [ {"name": "f1", "type": "string"} ] }

$ kafka-create-topic topic=resolution-2to1

$ kafka-ingest format=avro topic=resolution-2to1 schema=${2columns} publish=true timestamp=1
{"f1": "val_f1a", "f2": "val_f2a"}

> CREATE SOURCE resolution_2to1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-resolution-2to1-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=resolution-2to1 schema=${1column} publish=true timestamp=2
{"f1": "val_f1b"}

> SELECT * FROM resolution_2to1
f1 f2
---
val_f1a val_f2a
val_f1b default_f2
