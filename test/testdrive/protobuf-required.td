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

$ kafka-create-topic topic=required partitions=1

$ kafka-ingest topic=required format=protobuf descriptor-file=required.pb message=Required
{"f": 42}

> CREATE MATERIALIZED SOURCE required FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-required-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Required' USING SCHEMA FILE '${testdrive.temp-dir}/required.pb'

> SELECT * FROM required
f
----
42

$ kafka-ingest topic=required format=protobuf descriptor-file=required.pb message=Required
{}

! SELECT * FROM required
contains:protobuf message missing required field f
