# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Test that Protobuf map fields are unsupported.

$ kafka-create-topic topic=maps partitions=1

$ file-append path=maps.proto
syntax = "proto3";

import "google/protobuf/wrappers.proto";

message Maps {
  map<int32, int32> int_map = 1;
  map<string, google.protobuf.Int64Value> message_map = 2;
}

$ protobuf-compile-descriptors inputs=maps.proto output=maps.pb set-var=maps-schema

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE maps
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-maps-${testdrive.seed}')

! CREATE TABLE maps_tbl FROM SOURCE maps (REFERENCE "testdrive-maps-${testdrive.seed}")
  FORMAT PROTOBUF MESSAGE '.Maps' USING SCHEMA '${maps-schema}'
contains:Protobuf map fields are not supported
