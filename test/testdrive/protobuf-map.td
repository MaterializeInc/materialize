# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that Protobuf map fields are unsupported.

$ file-append path=maps.proto
syntax = "proto3";

import "google/protobuf/wrappers.proto";

message Maps {
  map<int32, int32> int_map = 1;
  map<string, google.protobuf.Int64Value> message_map = 2;
}

$ protobuf-compile-descriptors inputs=maps.proto output=maps.pb

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

! CREATE SOURCE maps FROM
  KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-maps-${testdrive.seed}')
  FORMAT PROTOBUF MESSAGE '.Maps' USING SCHEMA FILE '${testdrive.temp-dir}/maps.pb'
contains:Protobuf map fields are not supported
