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
# Make sure that schema incompatibility issues within arrays are detected and reported
#

$ set array-double={"type": "record", "name": "schema_array", "fields": [ {"name": "f1", "type": { "type": "array", "items": "double" } } ] }
$ set array-int={"type": "record", "name": "schema_array", "fields": [ {"name": "f1", "type": { "type": "array", "items": "int" } } ] }

$ kafka-create-topic topic=resolution-arrays

$ kafka-ingest format=avro topic=resolution-arrays schema=${array-int} timestamp=1
{"f1": [ 123 ] }

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE resolution_arrays
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-resolution-arrays-${testdrive.seed}')

> CREATE TABLE resolution_arrays_tbl FROM SOURCE resolution_arrays (REFERENCE "testdrive-resolution-arrays-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
> COMMIT

$ kafka-ingest format=avro topic=resolution-arrays schema=${array-double} timestamp=2
{"f1": [ 234.345 ] }

! SELECT f1[0] FROM resolution_arrays_tbl
contains:Writer schema has type `Double`, but reader schema has type `Int` for field `schema_array.f1`

! SELECT f1[0] FROM resolution_arrays_tbl
contains:failed to resolve Avro schema (id =
