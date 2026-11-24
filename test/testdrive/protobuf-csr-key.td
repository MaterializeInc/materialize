# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Test how a Protobuf format from the schema registry treats the key subject.

$ kafka-create-topic topic=avro-key partitions=1

$ schema-registry-publish subject=testdrive-avro-key-${testdrive.seed}-key schema-type=avro
{"type": "record", "name": "Key", "fields": [{"name": "id", "type": "long"}]}

$ schema-registry-publish subject=testdrive-avro-key-${testdrive.seed}-value schema-type=protobuf
syntax = "proto3";

message Value {
    int32 x = 1;
}

$ kafka-create-topic topic=broken-key partitions=1

$ schema-registry-publish subject=testdrive-broken-key-${testdrive.seed}-key schema-type=protobuf
syntax = "proto3";

message Key {
    int32 id = 1;
}

message Other {
    int32 y = 1;
}

$ schema-registry-publish subject=testdrive-broken-key-${testdrive.seed}-value schema-type=protobuf
syntax = "proto3";

message Value {
    int32 x = 1;
}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

# A Protobuf value format ignores the key subject, which here holds Avro.
> CREATE SOURCE avro_key
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avro-key-${testdrive.seed}')

> CREATE TABLE avro_key_tbl FROM SOURCE avro_key (REFERENCE "testdrive-avro-key-${testdrive.seed}")
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> SELECT name, type FROM mz_columns JOIN mz_tables ON mz_columns.id = mz_tables.id
  WHERE mz_tables.name = 'avro_key_tbl' ORDER BY position
id bigint
x  integer

# An explicit Protobuf key format must not fall back to the value schema when
# the key subject exists but cannot be used.
> CREATE SOURCE broken_key
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-broken-key-${testdrive.seed}')

! CREATE TABLE broken_key_tbl FROM SOURCE broken_key (REFERENCE "testdrive-broken-key-${testdrive.seed}")
  KEY FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT
contains:Protobuf schemas with multiple messages
