
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Attempt to introduce a new element in a union - should fail
#

$ set union-int-string={"type": "record", "name": "schema_union", "fields": [ {"name": "f1", "type": ["int", "string"] } ] }
$ set union-int={"type": "record", "name": "schema_union", "fields": [ {"name": "f1", "type": ["int"] } ] }

$ kafka-create-topic topic=resolution-unions

$ kafka-ingest format=avro topic=resolution-unions schema=${union-int} timestamp=1
{"f1": {"int": 123 } }

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE resolution_unions
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-resolution-unions-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

$ kafka-ingest format=avro topic=resolution-unions schema=${union-int-string} timestamp=2
{"f1": {"int": 123 } }
{"f1": {"string": "abc" } }

! SELECT f1 FROM resolution_unions
contains:Failed to match writer union variant `String` against any variant in the reader for field `schema_union.f1`

! SELECT f1 FROM resolution_unions
contains:unable to decode row (Avro schema id =
