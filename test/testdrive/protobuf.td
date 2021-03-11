# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

! CREATE SOURCE bad FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Self' USING SCHEMA '${testdrive.protobuf-descriptors}'
Recursive types are not supported: .Self

! CREATE SOURCE bad FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Mutual1' USING SCHEMA '${testdrive.protobuf-descriptors}'
Recursive types are not supported: .Mutual1

> CREATE SOURCE protomessages FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA '${testdrive.protobuf-descriptors}'

> CREATE MATERIALIZED VIEW pm AS SELECT * FROM protomessages

> CREATE SOURCE protomessages2 FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA FILE '${testdrive.protobuf-descriptors-file}'

> CREATE VIEW pm2 AS SELECT * FROM protomessages

$ kafka-create-topic topic=messages

$ kafka-ingest format=protobuf topic=messages message=struct timestamp=1
{"int": 1, "bad_int": 1, "bin": "ONE", "st": "my-string"}
{"int": 2, "bad_int": 2, "bin": "ONE", "st": "something-valid"}

# TODO: these should be fully json
> SELECT * FROM pm
1 1 ONE  my-string 1
2 2 ONE  something-valid 2

# Test failure to deserialize protobuf messages when the value is corrupted
> CREATE SOURCE corrupted_protomessages FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-corrupted-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA '${testdrive.protobuf-descriptors}'

> CREATE MATERIALIZED VIEW corrupted_proto_messages AS SELECT * FROM corrupted_protomessages

$ kafka-create-topic topic=corrupted-messages

$ kafka-ingest format=protobuf topic=corrupted-messages message=struct timestamp=1 corrupt-values=true
{"int": 1, "bad_int": 1, "bin": "ONE", "st": "my-string"}
{"int": 2, "bad_int": 2, "bin": "ONE", "st": "something-valid"}

! SELECT * from corrupted_proto_messages
Decode error: Text: protobuf deserialization error: Deserializing into rust object: protobuf error: incorrect tag

> CREATE MATERIALIZED SOURCE protomessages3 FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  WITH (start_offset=1)
  FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA FILE '${testdrive.protobuf-descriptors-file}'

> SELECT * FROM protomessages3
2 2 ONE  something-valid 2

$ kafka-create-topic topic=messages-partitioned partitions=2

$ kafka-ingest format=protobuf topic=messages-partitioned message=struct timestamp=1 partition=0
{"int": 1, "bad_int": 1, "bin": "ONE", "st": "my-string"}

$ kafka-ingest format=protobuf topic=messages-partitioned message=struct timestamp=1 partition=1
{"int": 2, "bad_int": 2, "bin": "ONE", "st": "something-valid"}

> CREATE MATERIALIZED SOURCE protomessages_partitioned FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-partitioned-${testdrive.seed}'
  WITH (start_offset=[1,0])
  FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA FILE '${testdrive.protobuf-descriptors-file}'
