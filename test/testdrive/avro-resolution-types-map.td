# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that schema incompatibility issues within maps are detected and reported
#

$ set map-double={"type": "record", "name": "schema_map", "fields": [ {"name": "f1", "type": { "type": "map", "values": "double" } } ] }
$ set map-int={"type": "record", "name": "schema_map", "fields": [ {"name": "f1", "type": { "type": "map", "values": "int" } } ] }

$ kafka-create-topic topic=resolution-maps

$ kafka-ingest format=avro topic=resolution-maps schema=${map-int} timestamp=1
{"f1": { "key1": 123 } }

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE resolution_maps
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-resolution-maps-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

$ kafka-ingest format=avro topic=resolution-maps schema=${map-double} timestamp=2
{"f1": { "key1": 234.345 } }

! SELECT f1 -> 'key1' FROM resolution_maps
contains:Writer schema has type `Double`, but reader schema has type `Int` for field `schema_map.f1`

! SELECT f1 -> 'key1' FROM resolution_maps
contains:failed to resolve Avro schema (id =
