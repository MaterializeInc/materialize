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

$ kafka-create-topic topic=simple

$ set schema
syntax = "proto3";

import 'google/protobuf/timestamp.proto';

message SimpleId {
  string id = 1;
}

$ kafka-ingest topic=simple format=protobuf schema=${schema} message=simpleid publish=true
{"id": "a"}
{"id": "b"}

> CREATE MATERIALIZED SOURCE simple
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-simple-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SHOW COLUMNS FROM simple
id         true   text
mz_offset  false  bigint

> SELECT * FROM simple
id   mz_offset
------------
a    1
b    2

$ http-request method=POST url=${testdrive.schema-registry-url}subjects/no_messages.proto/versions content-type=application/json
{
    "schema": "syntax = \"proto3\"; enum Choices { ZERO = 0; ONE = 1; TWO = 2;}",
    "schemaType": "PROTOBUF",
    "references": []
}

$ kafka-create-topic topic=imported

$ set schema
syntax = "proto3";

import 'no_messages.proto';

message Imported {
    Choices choice = 1;
}

$ kafka-ingest topic=imported format=protobuf schema=${schema} message=imported publish=true
{"choice": "ONE"}
{"choice": "TWO"}

> CREATE MATERIALIZED SOURCE imported_csr
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-imported-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# Can't SELECT record types.
> SELECT COUNT(*) FROM imported_csr
2

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
{"double": 1.1, "float": 2.2, "int32": -100, "int64": 200, "sint32": 987234, "sint64": 129387981723, "sfixed32": 123, "sfixed64": 567, "bool": true, "string": "hey", "bytes": [102, 111, 111], "binary": "ZERO", "nested": [{"double": 1.1, "float": 2.2, "int32": -100, "int64": 200, "sint32": 987234, "sint64": 129387981723, "sfixed32": 123, "sfixed64": 567, "bool": true, "string": "hey", "bytes": [102, 111, 111], "binary": "ZERO"}]}

> CREATE MATERIALIZED SOURCE nested_proto_inline FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-nested-proto-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.NestedOuter' USING SCHEMA '${testdrive.protobuf-descriptors}'

> SELECT COUNT(*) FROM nested_proto_inline
1

> SELECT double, float, int32, int64, sint32, sint64, sfixed32, sfixed64, bool, string, bytes, binary, mz_offset FROM nested_proto_inline
double  float   int32   int64   sint32   sint64         sfixed32   sfixed64   bool   string   bytes   binary  mz_offset
---------------------------------------------------------------------------------------------------------------------------
1.1     2.2     -100    200     987234   129387981723   123        567        true   "hey"    "foo"   "ZERO"  1

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
nested    true list
mz_offset false  bigint

> SELECT COUNT(*) FROM nested_proto
1

> SELECT double, float, int32, int64, sint32, sint64, sfixed32, sfixed64, bool, string, bytes, binary, mz_offset FROM nested_proto
double  float   int32   int64   sint32   sint64         sfixed32   sfixed64   bool   string   bytes   binary   mz_offset
--------------------------------------------------------------------------------------------------------------------------
1.1     2.2     -100    200     987234   129387981723   123        567        true   "hey"    "foo"   "ZERO"   1

# Test records
$ kafka-create-topic topic=simple_nested_proto

> CREATE MATERIALIZED SOURCE simple_nested_proto
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-simple_nested_proto-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.SimpleNestedOuter' USING SCHEMA '${testdrive.protobuf-descriptors}'

$ kafka-ingest format=protobuf topic=simple_nested_proto message=simple-nested
{"inner": {"message": "hello"}}
{"inner": {"message": "world"}}

> SHOW COLUMNS FROM simple_nested_proto
name            nullable  type
-------------------------------
inner           true      record
mz_offset       false     bigint

> SELECT COUNT(*) FROM simple_nested_proto
2

$ kafka-create-topic topic=batch_proto

> CREATE MATERIALIZED SOURCE batch_proto
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-batch_proto-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Batch' USING SCHEMA '${testdrive.protobuf-descriptors}'

> SHOW COLUMNS FROM batch_proto
name            nullable  type
-------------------------------
id              true      text
interval_end    true      text
interval_start  true      text
mz_offset       false     bigint
records         true      list

$ kafka-ingest format=protobuf topic=batch_proto message=batch timestamp=1
{"id": "1", "interval_start": "2020-01-01_00:00:00", "interval_end": "2020-01-01_00:00:09", "records": []}
{"id": "2", "interval_start": "2020-01-01_00:00:10", "interval_end": "2020-01-01_00:00:19", "records": []}

> SELECT COUNT(*) FROM batch_proto
2

> SELECT id, interval_start, interval_end, mz_offset FROM batch_proto
 id  interval_start       interval_end         mz_offset
------------------------------------------------------------------
 1   2020-01-01_00:00:00  2020-01-01_00:00:09  1
 2   2020-01-01_00:00:10  2020-01-01_00:00:19  2

$ http-request method=POST url=${testdrive.schema-registry-url}subjects/google%2Fprotobuf%2Ftimestamp.proto/versions content-type=application/json
{
    "schema": "syntax = \"proto3\"; package google.protobuf; message Timestamp{int64 seconds = 1; int32 nanos=2;}",
    "schemaType": "PROTOBUF",
    "references": []
}

$ kafka-create-topic topic=well-known-import

$ set schema
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message TimestampId {
  string id = 1;
  .google.protobuf.Timestamp ts = 2;
}

$ kafka-ingest topic=well-known-import format=protobuf schema=${schema} message=timestampid publish=true
{"id": "a", "ts": {"seconds": 1234, "nanos": 5678}}
{"id": "b", "ts": {"seconds": 4321, "nanos": 8765}}

> CREATE MATERIALIZED SOURCE well_known_import
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-well-known-import-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SHOW COLUMNS FROM well_known_import
id         true   text
mz_offset  false  bigint
ts         true   record

> SELECT id, mz_offset FROM well_known_import WHERE ts IS NOT NULL
id   mz_offset
------------
a    1
b    2
