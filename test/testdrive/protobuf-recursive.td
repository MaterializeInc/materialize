# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that recursive Protobuf types are unsupported.

$ file-append path=recursive.proto
syntax = "proto3";

message Self {
    Self self = 1;
}

message Mutual1 {
    Mutual2 m = 1;
}

message Mutual2 {
    Mutual3 m = 1;
}

message Mutual3 {
    Mutual1 m = 1;
}

$ protobuf-compile-descriptors inputs=recursive.proto output=recursive.pb set-var=recursive-schema

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

! CREATE SOURCE recursive FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-recursive-${testdrive.seed}')
  FORMAT PROTOBUF MESSAGE '.Self' USING SCHEMA '${recursive-schema}'
contains:Recursive types are not supported: Self

! CREATE SOURCE recursive FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-recursive-${testdrive.seed}')
  FORMAT PROTOBUF MESSAGE '.Mutual1' USING SCHEMA '${recursive-schema}'
contains:Recursive types are not supported: Mutual1
