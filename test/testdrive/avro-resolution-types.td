# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that we reject changes of basic column types even if the schema registry reports that the two
# schemas are compatible. It is up to Materialize to reject the value
#

$ set int-col={"type": "record", "name": "schema_int_double", "fields": [ {"name": "f1", "type": "int"} ] }
$ set double-col={"type": "record", "name": "schema_int_double", "fields": [ {"name": "f1", "type": "double"} ] }

$ kafka-create-topic topic=resolution-int2double

$ kafka-ingest format=avro topic=resolution-int2double schema=${int-col} publish=true timestamp=1
{"f1": 123}

> CREATE SOURCE resolution_int2double
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-resolution-int2double-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=resolution-int2double schema=${double-col} publish=true timestamp=2
{"f1": 234.456}

! SELECT * FROM resolution_int2double
contains:Writer schema has type `Double`, but reader schema has type `Int` for field `schema_int_double.f1`

! SELECT * FROM resolution_int2double
contains:failed to resolve Avro schema (id =
