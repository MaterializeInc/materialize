# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test is a variant of kafka-upsert-sources-named.td that uses the
# object storage (SlateDB) backend for upsert instead of RocksDB.
# It verifies that upsert sources work correctly with the object storage backend.

$ set-sql-timeout duration=60s

$ set-arg-default single-replica-cluster=quickstart

# Enable the object storage backend for upsert (using in-memory storage for testing)
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_upsert_use_object_storage = true
ALTER SYSTEM SET storage_upsert_object_storage_in_memory = true

# Test 1: Basic upsert with composite key (AVRO key and value)

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"},
        {"name": "key2", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=avroavro-objstore

# Insert initial record
$ kafka-ingest format=avro topic=avroavro-objstore key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish", "key2": "key2" } {"f1": "fishval", "f2": 1000}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE avroavro_objstore
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-avroavro-objstore-${testdrive.seed}')

> CREATE TABLE avroavro_objstore_tbl FROM SOURCE avroavro_objstore (REFERENCE "testdrive-avroavro-objstore-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE KEY AS named
  ENVELOPE UPSERT
> COMMIT

# Verify initial insert
> SELECT (named).key, (named).key2, f1, f2 from avroavro_objstore_tbl
key     key2  f1       f2
---------------------------
fish    key2  fishval  1000

# Test update: change value for existing key
$ kafka-ingest format=avro topic=avroavro-objstore key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish", "key2": "key2" } {"f1": "updated", "f2": 2000}

> SELECT (named).key, (named).key2, f1, f2 from avroavro_objstore_tbl
key     key2  f1       f2
---------------------------
fish    key2  updated  2000

# Test insert: add another key
$ kafka-ingest format=avro topic=avroavro-objstore key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "bird", "key2": "key2" } {"f1": "birdval", "f2": 3000}

> SELECT (named).key, (named).key2, f1, f2 from avroavro_objstore_tbl ORDER BY (named).key
key     key2  f1       f2
---------------------------
bird    key2  birdval  3000
fish    key2  updated  2000

# Test delete: null value deletes the key
$ kafka-ingest format=avro topic=avroavro-objstore key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish", "key2": "key2" }

> SELECT (named).key, (named).key2, f1, f2 from avroavro_objstore_tbl
key     key2  f1       f2
---------------------------
bird    key2  birdval  3000

# Test 2: Single-field key

$ set keyschemasingle={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ kafka-create-topic topic=single-objstore

$ kafka-ingest format=avro topic=single-objstore key-format=avro key-schema=${keyschemasingle} schema=${schema}
{"key": "fish" } {"f1": "fishval", "f2": 1000}

> BEGIN
> CREATE SOURCE single_objstore
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-single-objstore-${testdrive.seed}')

> CREATE TABLE single_objstore_tbl FROM SOURCE single_objstore (REFERENCE "testdrive-single-objstore-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschemasingle}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE KEY AS named
  ENVELOPE UPSERT
> COMMIT

> SELECT named, f1, f2 from single_objstore_tbl
named   f1       f2
---------------------------
fish    fishval  1000

# Test update for single-field key
$ kafka-ingest format=avro topic=single-objstore key-format=avro key-schema=${keyschemasingle} schema=${schema}
{"key": "fish" } {"f1": "updated", "f2": 9999}

> SELECT named, f1, f2 from single_objstore_tbl
named   f1       f2
---------------------------
fish    updated  9999

# Cleanup: reset the dyncfg
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET storage_upsert_use_object_storage
ALTER SYSTEM RESET storage_upsert_object_storage_in_memory
