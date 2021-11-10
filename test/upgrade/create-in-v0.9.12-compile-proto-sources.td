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
message SimpleId {
  string id = 1;
}

$ kafka-create-topic topic=upgrade-proto-source-${env.UPGRADE_FROM_VERSION}

$ kafka-ingest topic=upgrade-proto-source-${env.UPGRADE_FROM_VERSION} format=protobuf schema=${schema} message=simpleid publish=true
{"id": "c"}
{"id": "h"}

> CREATE MATERIALIZED SOURCE kafka_proto_source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-upgrade-proto-source-${env.UPGRADE_FROM_VERSION}-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SELECT COUNT(*) = 2 FROM kafka_proto_source
true
