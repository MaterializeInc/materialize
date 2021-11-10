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

> SELECT * FROM kafka_source;
id
--
1
2

$ kafka-ingest topic=upgrade-proto-source-${env.UPGRADE_FROM_VERSION} format=protobuf schema=${schema} message=simpleid publish=true
{"id": "q"}
{"id": "r"}

> SELECT * FROM kafka_source;
id
--
a
b
q
r

> DROP SOURCE kafka_source;
