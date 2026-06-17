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
# A nullable column whose type is promoted from `int` to `double` is a
# non-breaking Avro change: `int` is promotable to `double`, so a record
# written with the old schema must still decode against the new (reader)
# schema. Because a nullable column is encoded as a union (`["null", T]`),
# this exercises union-vs-union schema resolution, which must honor numeric
# promotion when matching the writer's `int` variant to the reader's `double`
# variant.
#
# Regression test: this previously failed with
#   "Failed to match writer union variant `Int` against any variant in the
#    reader"
# because union variant matching ignored numeric promotion.
#

$ set writer-union-int={"type": "record", "name": "schema_promote", "fields": [ {"name": "f1", "type": ["null", "int"] } ] }
$ set reader-union-double={"type": "record", "name": "schema_promote", "fields": [ {"name": "f1", "type": ["null", "double"] } ] }

$ kafka-create-topic topic=resolution-union-promote

# Write a record with the OLD schema (f1: ["null", "int"]). This registers the
# int schema as version 1.
$ kafka-ingest format=avro topic=resolution-union-promote schema=${writer-union-int} timestamp=1
{"f1": {"int": 123 } }
{"f1": null }

# Write a record with the NEW schema (f1: ["null", "double"]). This registers
# the double schema as the latest version, which the source below will adopt
# as its reader schema.
$ kafka-ingest format=avro topic=resolution-union-promote schema=${reader-union-double} timestamp=2
{"f1": {"double": 234.5 } }

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE resolution_union_promote
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-resolution-union-promote-${testdrive.seed}')

> CREATE TABLE resolution_union_promote_tbl FROM SOURCE resolution_union_promote (REFERENCE "testdrive-resolution-union-promote-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
> COMMIT

# The int-written record (123) must promote to double; the null and the
# double-written record must decode unchanged.
> SELECT f1 FROM resolution_union_promote_tbl
123
234.5
<null>
