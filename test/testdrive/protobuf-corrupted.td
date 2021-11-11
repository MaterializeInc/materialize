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

message Simple {
    int64 f = 1;
}

$ protobuf-compile-descriptors inputs=simple.proto output=simple.pb

$ kafka-create-topic topic=corrupted

$ kafka-ingest format=bytes topic=corrupted
garbage

> CREATE MATERIALIZED SOURCE corrupted FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-corrupted-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Simple' USING SCHEMA FILE '${testdrive.temp-dir}/simple.pb'

! SELECT * FROM corrupted
Decode error: Text: protobuf deserialization error: Deserializing into rust object: protobuf error
