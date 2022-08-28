# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that compatible writer and reader enums work
#

$ set enum-reader={"type": "record", "name": "schema_enum", "fields": [ {"name": "f1", "type": { "type": "enum", "name": "enum1", "symbols" : ["E1", "E2", "E3", "E4", "E_DEFAULT"], "default": "E_DEFAULT" } } ] }
$ set enum-writer={"type": "record", "name": "schema_enum", "fields": [ {"name": "f1", "type": { "type": "enum", "name": "enum1", "symbols" : ["E2", "E3", "E4", "E5", "E_DEFAULT"], "default": "E_DEFAULT" } } ] }

$ kafka-create-topic topic=resolution-enums

$ kafka-ingest format=avro topic=resolution-enums schema=${enum-reader} publish=true timestamp=1
{"f1": "E1" }

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE resolution_enums
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-resolution-enums-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

# E5 will be recorded as E_DEFAULT
$ kafka-ingest format=avro topic=resolution-enums schema=${enum-writer} publish=true timestamp=2
{"f1": "E5" }

$ kafka-ingest format=avro topic=resolution-enums schema=${enum-reader} publish=true timestamp=1
{"f1": "E1" }

> SHOW COLUMNS FROM resolution_enums
f1 false text

> SELECT f1 FROM resolution_enums
E1
E1
E_DEFAULT
