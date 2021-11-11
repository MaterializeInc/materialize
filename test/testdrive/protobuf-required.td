# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that required Protobuf fields are unsupported.

$ file-append path=required.proto
syntax = "proto2";

message Required {
  required int32 f = 1;
}

$ protobuf-compile-descriptors inputs=required.proto output=required.pb

! CREATE SOURCE unsigned FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'required'
  FORMAT PROTOBUF MESSAGE '.Required' USING SCHEMA FILE '${testdrive.temp-dir}/required.pb'
Required field f not supported
