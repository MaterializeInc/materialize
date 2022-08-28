# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema
syntax = "proto3";

message Message {
  string id = 1;
}

$ file-append path=message.proto
\${schema}

$ protobuf-compile-descriptors inputs=message.proto output=message.pb

$ kafka-create-topic topic=upgrade-proto-source-${arg.upgrade-from-version}

$ schema-registry-publish subject=testdrive-upgrade-proto-source-${arg.upgrade-from-version}-${testdrive.seed}-value schema-type=protobuf
\${schema}

$ kafka-ingest topic=upgrade-proto-source-${arg.upgrade-from-version} format=protobuf descriptor-file=message.pb message=Message confluent-wire-format=true
{"id": "c"}
{"id": "h"}

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE kafka_proto_source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-upgrade-proto-source-${arg.upgrade-from-version}-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
