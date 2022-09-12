# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that unsigned Protobuf types are unsupported.
# See: https://github.com/MaterializeInc/materialize/issues/7629

$ file-append path=unsigned.proto
syntax = "proto3";

message UInt32 {
  uint32 f = 1;
}

message UInt64 {
  uint64 f = 1;
}

message Fixed32 {
  fixed32 f = 1;
}

message Fixed64 {
  fixed64 f = 1;
}

$ protobuf-compile-descriptors inputs=unsigned.proto output=unsigned.pb

! CREATE SOURCE unsigned FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-unsigned-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.UInt32' USING SCHEMA FILE '${testdrive.temp-dir}/unsigned.pb'
contains:Protobuf unsigned integer types are not supported

! CREATE SOURCE unsigned FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-unsigned-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.UInt64' USING SCHEMA FILE '${testdrive.temp-dir}/unsigned.pb'
contains:Protobuf unsigned integer types are not supported

! CREATE SOURCE unsigned FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-unsigned-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Fixed32' USING SCHEMA FILE '${testdrive.temp-dir}/unsigned.pb'
contains:Protobuf unsigned integer types are not supported

! CREATE SOURCE unsigned FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-unsigned-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Fixed64' USING SCHEMA FILE '${testdrive.temp-dir}/unsigned.pb'
contains:Protobuf unsigned integer types are not supported
