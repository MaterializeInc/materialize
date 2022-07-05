# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that proto2 custom defaults are respected.

$ file-append path=defaults.proto
syntax = "proto2";

message Defaults {
    enum Enum {
        ENUM0 = 0;
        ENUM1 = 1;
    }

    optional bool bool = 1 [default = true];
    optional int32 int32 = 2 [default = 42];
    optional int64 int64 = 3 [default = 42];
    optional float float = 4 [default = 42.0];
    optional double double = 5 [default = 42.0];
    optional bytes bytes = 6 [default = "aaa"];
    optional string string = 7 [default = "bbb"];
    optional Enum enum = 8 [default = ENUM1];
}

$ protobuf-compile-descriptors inputs=defaults.proto output=defaults.pb

$ kafka-create-topic topic=defaults partitions=1

$ kafka-ingest topic=defaults format=protobuf descriptor-file=defaults.pb message=Defaults
{}

> CREATE MATERIALIZED SOURCE defaults FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-defaults-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Defaults' USING SCHEMA FILE '${testdrive.temp-dir}/defaults.pb'

> SELECT * FROM defaults
bool  int32  int64  float  double  bytes  string enum
----
true  42     42     42     42      aaa    bbb    ENUM1
