# Copyright Materialize, Inc. and contributors. All rights reserved.
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

! CREATE SOURCE bad FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.UInt32' USING SCHEMA '${testdrive.protobuf-descriptors}'
Protobuf type "uint32" is not supported

! CREATE SOURCE bad FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.UInt64' USING SCHEMA '${testdrive.protobuf-descriptors}'
Protobuf type "uint64" is not supported

! CREATE SOURCE bad FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Fixed32' USING SCHEMA '${testdrive.protobuf-descriptors}'
Protobuf type "fixed32" is not supported

! CREATE SOURCE bad FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Fixed64' USING SCHEMA '${testdrive.protobuf-descriptors}'
Protobuf type "fixed64" is not supported

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

$ kafka-ingest format=bytes topic=corrupted-messages timestamp=1
garbage

! SELECT * from corrupted_proto_messages
Decode error: Text: protobuf deserialization error: Deserializing into rust object: protobuf error

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

$ kafka-create-topic topic=tester

$ set schema
syntax = "proto3";

message SimpleId {
  string id = 1;
}

$ kafka-ingest topic=tester format=protobuf schema=${schema} message=simpleid publish=true
{"id": "a"}
{"id": "b"}

> CREATE MATERIALIZED SOURCE tester
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-tester-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SHOW COLUMNS FROM tester
id         true   text
mz_offset  false  bigint

> SELECT * FROM tester
id   mz_offset
------------
a    1
b    2

$ kafka-create-topic topic=nested-proto

$ set schema
syntax = "proto3";

enum Binary {
  ZERO = 0;
  ONE = 1;
}

message NestedOuter {
  double double = 1;
  float float = 2;
  int32 int32 = 3;
  int64 int64 = 4;
  sint32 sint32 = 5;
  sint64 sint64 = 6;
  sfixed32 sfixed32 = 7;
  sfixed64 sfixed64 = 8;
  bool bool = 9;
  string string = 10;
  bytes bytes = 11;

  Binary binary = 12;

  repeated NestedInner nested = 13;
}

message NestedInner {
  double double = 1;
  float float = 2;
  int32 int32 = 3;
  int64 int64 = 4;
  sint32 sint32 = 5;
  sint64 sint64 = 6;
  sfixed32 sfixed32 = 7;
  sfixed64 sfixed64 = 8;
  bool bool = 9;
  string string = 10;

  Binary binary = 11;
}


$ kafka-ingest topic=nested-proto format=protobuf schema=${schema} message=nested publish=true
{"double": 1.1, "float": 2.2, "int32": -100, "int64": 200, "sint32": 987234, "sint64": 129387981723, "sfixed32": 123, "sfixed64": 567, "bool": true, "string": "hey", "bytes": [102, 111, 111], "binary": "ZERO", "nested": []}

> CREATE MATERIALIZED SOURCE nested_proto_inline FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-nested-proto-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.NestedOuter' USING SCHEMA '${testdrive.protobuf-descriptors}'

> SELECT * FROM nested_proto_inline
double  float   int32   int64   sint32   sint64         sfixed32   sfixed64   bool   string   bytes   binary   nested   mz_offset
---------------------------------------------------------------------------------------------------------------------------
1.1     2.2     -100    200     987234   129387981723   123        567        true   "hey"    "foo"   "ZERO"   []       1

> CREATE MATERIALIZED SOURCE nested_proto
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-nested-proto-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SHOW COLUMNS FROM nested_proto
double    true "double precision"
float     true real
int32     true integer
int64     true bigint
sint32    true integer
sint64    true bigint
sfixed32  true integer
sfixed64  true bigint
bool      true boolean
string    true text
bytes     true bytea
binary    true text
nested    true jsonb
mz_offset false  bigint

> SELECT * FROM nested_proto
double  float   int32   int64   sint32   sint64         sfixed32   sfixed64   bool   string   bytes   binary   nested   mz_offset
--------------------------------------------------------------------------------------------------------------------------
1.1     2.2     -100    200     987234   129387981723   123        567        true   "hey"    "foo"   "ZERO"  []       1
