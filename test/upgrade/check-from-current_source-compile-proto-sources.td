# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM kafka_proto_source;
id
---
c
h

$ kafka-ingest topic=upgrade-proto-source-${arg.upgrade-from-version} format=protobuf descriptor-file=message.pb message=Message confluent-wire-format=true
{"id": "a"}
{"id": "e"}

> SELECT * FROM kafka_proto_source;
id
---
c
h
a
e

> DROP SOURCE kafka_proto_source;
