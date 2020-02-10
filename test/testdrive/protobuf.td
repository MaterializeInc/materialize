# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SOURCE protomessages FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA '${testdrive.protobuf-descriptors}'

> CREATE MATERIALIZED VIEW pm AS SELECT * FROM protomessages

> CREATE SOURCE protomessages2 FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA FILE '${testdrive.protobuf-descriptors-file}'

> CREATE VIEW pm2 AS SELECT * FROM protomessages

$ kafka-ingest format=protobuf topic=messages message=.Struct timestamp=1
{"int": 1, "bad_int": 1, "bin": "One", "st": "my-string"}
{"int": 2, "bad_int": 2, "bin": "One", "st": "something-valid"}

# TODO: these should be fully json
> SELECT * FROM pm
1 1 ONE  my-string
2 2 ONE  something-valid
