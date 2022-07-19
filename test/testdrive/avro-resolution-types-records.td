# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Attempt to push a double value inside a record into a int source. According to the schema registry
# this is allowed, so it is up to Materialize to reject it.
#

$ set int-col={"type": "record", "name": "outer", "fields": [ {"name": "f1", "type": {"type": "record", "name": "inner", "fields": [ {"name": "f1", "type": "int" } ] } } ] }
$ set double-col={"type": "record", "name": "outer", "fields": [ {"name": "f1", "type": {"type": "record", "name": "inner", "fields": [ {"name": "f1", "type": "double" } ] } } ] }

$ kafka-create-topic topic=resolution-records-int2double

$ kafka-ingest format=avro topic=resolution-records-int2double schema=${int-col} publish=true timestamp=1
{"f1": { "f1": 123 }}

> CREATE SOURCE resolution_records_int2double
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-resolution-records-int2double-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=resolution-records-int2double schema=${double-col} publish=true timestamp=2
{"f1": { "f1": 123.234 }}

! SELECT * FROM resolution_records_int2double
contains:Writer schema has type `Double`, but reader schema has type `Int` for field `inner.f1`

! SELECT * FROM resolution_records_int2double
contains:failed to resolve Avro schema (id =
