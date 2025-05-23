# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

#
# Make sure that compatible writer and reader enums work
#

$ set enum-reader={"type": "record", "name": "schema_enum", "fields": [ {"name": "f1", "type": { "type": "enum", "name": "enum1", "symbols" : ["E1", "E2", "E3", "E4", "E_DEFAULT"], "default": "E_DEFAULT" } } ] }
$ set enum-writer={"type": "record", "name": "schema_enum", "fields": [ {"name": "f1", "type": { "type": "enum", "name": "enum1", "symbols" : ["E2", "E3", "E4", "E5", "E_DEFAULT"], "default": "E_DEFAULT" } } ] }

$ kafka-create-topic topic=resolution-enums

$ kafka-ingest format=avro topic=resolution-enums schema=${enum-reader} timestamp=1
{"f1": "E1" }

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE resolution_enums
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-resolution-enums-${testdrive.seed}')

> CREATE TABLE resolution_enums_tbl FROM SOURCE resolution_enums (REFERENCE "testdrive-resolution-enums-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

# E5 will be recorded as E_DEFAULT
$ kafka-ingest format=avro topic=resolution-enums schema=${enum-writer} timestamp=2
{"f1": "E5" }

$ kafka-ingest format=avro topic=resolution-enums schema=${enum-reader} timestamp=1
{"f1": "E1" }

> SHOW COLUMNS FROM resolution_enums_tbl
f1 false text ""

> SELECT f1 FROM resolution_enums_tbl
E1
E1
E_DEFAULT
