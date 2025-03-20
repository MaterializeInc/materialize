# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Test that corrupted Protobuf messages result in sensible error messages.

$ file-append path=simple.proto
syntax = "proto3";

message OneInt {
    int64 f = 1;
}

message OneString {
    string f = 1;
}

$ protobuf-compile-descriptors inputs=simple.proto output=simple.pb set-var=simple-schema

$ kafka-create-topic topic=total-garbage

$ kafka-ingest format=bytes topic=total-garbage
garbage

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE total_garbage
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-total-garbage-${testdrive.seed}')

> CREATE TABLE total_garbage_tbl FROM SOURCE total_garbage (REFERENCE "testdrive-total-garbage-${testdrive.seed}")
  FORMAT PROTOBUF MESSAGE '.OneInt' USING SCHEMA '${simple-schema}'

! SELECT * FROM total_garbage_tbl
contains:Decode error: protobuf deserialization error: failed to decode Protobuf message: invalid wire type value: 7 (original text: garbage, original bytes: "67617262616765")

$ kafka-create-topic topic=wrong-message

$ kafka-ingest topic=wrong-message format=protobuf descriptor-file=simple.pb message=OneInt
{"f": 1}

> CREATE SOURCE wrong_message
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-wrong-message-${testdrive.seed}')

> CREATE TABLE wrong_message_tbl FROM SOURCE wrong_message (REFERENCE "testdrive-wrong-message-${testdrive.seed}")
  FORMAT PROTOBUF MESSAGE '.OneString' USING SCHEMA '${simple-schema}'

! SELECT * FROM wrong_message_tbl
contains:Decode error: protobuf deserialization error: failed to decode Protobuf message: invalid wire type: Varint (expected LengthDelimited)
