# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Test that Protobuf schemas using well-known types can be compiled from the
# Confluent Schema Registry even when the well-known types themselves are NOT
# registered in the schema registry.
#
# This is a common real-world scenario: users register their own schemas that
# import types like google/protobuf/timestamp.proto, but they don't register
# the well-known types themselves (since protoc implicitly provides them).
#
# Previously this would fail with "invalid protobuf schema" because the
# well-known types couldn't be found. The fix embeds well-known types so they
# are always available during schema compilation.

# A simple schema that uses google.protobuf.Timestamp without any other imports.
$ set timestamp-user-schema
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message Event {
    string id = 1;
    google.protobuf.Timestamp created_at = 2;
}

# Compile the schema locally (protoc has access to well-known types)
$ file-append path=event.proto
\${timestamp-user-schema}

$ protobuf-compile-descriptors inputs=event.proto output=event.pb set-var=event-schema

# Create Kafka topic and connections
$ kafka-create-topic topic=well-known-types-test partitions=1

> CREATE CONNECTION IF NOT EXISTS kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

# Publish ONLY the user schema to the schema registry.
# Note: We are NOT publishing google/protobuf/timestamp.proto to the registry.
# This simulates the common case where users don't register well-known types.
$ schema-registry-publish subject=testdrive-well-known-types-test-${testdrive.seed}-value schema-type=protobuf
\${timestamp-user-schema}

# Ingest a test message
$ kafka-ingest topic=well-known-types-test format=protobuf descriptor-file=event.pb message=Event confluent-wire-format=true
{"id": "evt-123", "created_at": "2024-01-15T10:30:00Z"}

# Create source using CSR - this should succeed even though timestamp.proto
# is not in the registry, because we now embed well-known types.
> BEGIN
> CREATE SOURCE well_known_types_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-well-known-types-test-${testdrive.seed}')

> CREATE TABLE well_known_types_tbl FROM SOURCE well_known_types_source (REFERENCE "testdrive-well-known-types-test-${testdrive.seed}")
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
> COMMIT

# Verify the data was ingested correctly
> SELECT id, created_at::text FROM well_known_types_tbl
id       created_at
--------------------------
evt-123  "(1705314600,0)"

# Test with google.protobuf.Duration as well
$ set duration-user-schema
syntax = "proto3";

import "google/protobuf/duration.proto";

message Task {
    string name = 1;
    google.protobuf.Duration timeout = 2;
}

$ file-append path=task.proto
\${duration-user-schema}

$ protobuf-compile-descriptors inputs=task.proto output=task.pb set-var=task-schema

$ kafka-create-topic topic=well-known-duration-test partitions=1

$ schema-registry-publish subject=testdrive-well-known-duration-test-${testdrive.seed}-value schema-type=protobuf
\${duration-user-schema}

$ kafka-ingest topic=well-known-duration-test format=protobuf descriptor-file=task.pb message=Task confluent-wire-format=true
{"name": "my-task", "timeout": "30s"}

> BEGIN
> CREATE SOURCE well_known_duration_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-well-known-duration-test-${testdrive.seed}')

> CREATE TABLE well_known_duration_tbl FROM SOURCE well_known_duration_source (REFERENCE "testdrive-well-known-duration-test-${testdrive.seed}")
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
> COMMIT

> SELECT name, timeout::text FROM well_known_duration_tbl
name     timeout
-------------------
my-task  "(30,0)"
