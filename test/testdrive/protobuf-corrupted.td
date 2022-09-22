# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that corrupted Protobuf messages result in sensible error messages.

$ file-append path=simple.proto
syntax = "proto3";

message OneInt {
    int64 f = 1;
}

message OneString {
    string f = 1;
}

$ protobuf-compile-descriptors inputs=simple.proto output=simple.pb

$ kafka-create-topic topic=total-garbage

$ kafka-ingest format=bytes topic=total-garbage
garbage

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE total_garbage FROM
  KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-total-garbage-${testdrive.seed}')
  FORMAT PROTOBUF MESSAGE '.OneInt' USING SCHEMA FILE '${testdrive.temp-dir}/simple.pb'

! SELECT * FROM total_garbage
contains:Decode error: Text: protobuf deserialization error: failed to decode Protobuf message: invalid wire type value: 7

$ kafka-create-topic topic=wrong-message

$ kafka-ingest topic=wrong-message format=protobuf descriptor-file=simple.pb message=OneInt
{"f": 1}

> CREATE SOURCE wrong_message FROM
  KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-wrong-message-${testdrive.seed}')
  FORMAT PROTOBUF MESSAGE '.OneString' USING SCHEMA FILE '${testdrive.temp-dir}/simple.pb'

! SELECT * FROM wrong_message
contains:Decode error: Text: protobuf deserialization error: failed to decode Protobuf message: invalid wire type: Varint (expected LengthDelimited)
