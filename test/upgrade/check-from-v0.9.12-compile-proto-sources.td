# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM kafka_proto_source;
id mz_offset
------------
c  1
h  2

$ kafka-ingest topic=upgrade-proto-source-${env.UPGRADE_FROM_VERSION} format=protobuf descriptor-file=message.pb message=.Message confluent-wire-format=true schema-id=1
{"id": "a"}
{"id": "e"}

> SELECT * FROM kafka_proto_source;
id mz_offset
------------
c  1
h  2
a  3
e  4

> DROP SOURCE kafka_proto_source;
