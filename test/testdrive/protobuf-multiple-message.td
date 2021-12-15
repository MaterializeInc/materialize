# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that Protobuf imports are handled correctly.

# define the schema
$ set the-schema
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message Message1 {
    bool b = 1;
}

message Message2 {
    int64 value = 1;
}


# setup kafka
$ kafka-create-topic topic=import-csr partitions=1


# Compile the protos for kafka to use
$ file-append path=the.proto
\${the-schema}

$ protobuf-compile-descriptors inputs=the.proto output=the.pb


# Publish the schema
$ schema-registry-publish subject=testdrive-import-csr-${testdrive.seed}-value schema-type=protobuf
\${the-schema}


# Note that only the first message schema works
$ kafka-ingest topic=import-csr format=protobuf descriptor-file=the.pb message=.Message1 confluent-wire-format=true
{"b": false}}

! CREATE MATERIALIZED SOURCE import_csr FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-import-csr-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
proto files with multiple `message`'s are not yet supported
