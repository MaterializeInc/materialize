# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that basic types in a Protobuf message are decoded correctly.

$ file-append path=basic.proto
syntax = "proto3";

message Basic {
    enum Enum {
        ENUM0 = 0;
        ENUM1 = 1;
    }

    message Nested {
      bool bool = 1;
      int32 int32 = 2;
      int64 int64 = 3;
      sint32 sint32 = 4;
      sint64 sint64 = 5;
      sfixed32 sfixed32 = 6;
      sfixed64 sfixed64 = 7;
      uint32 uint32 = 8;
      uint64 uint64 = 9;
      fixed32 fixed32 = 10;
      fixed64 fixed64 = 11;
      float float = 12;
      double double = 13;
      bytes bytes = 14;
      string string = 15;
      Enum enum = 16;
    }

    bool bool = 1;
    int32 int32 = 2;
    int64 int64 = 3;
    sint32 sint32 = 4;
    sint64 sint64 = 5;
    sfixed32 sfixed32 = 6;
    sfixed64 sfixed64 = 7;
    uint32 uint32 = 8;
    uint64 uint64 = 9;
    fixed32 fixed32 = 10;
    fixed64 fixed64 = 11;
    float float = 12;
    double double = 13;
    bytes bytes = 14;
    string string = 15;
    Enum enum = 16;
    Nested message = 17;
}

$ protobuf-compile-descriptors inputs=basic.proto output=basic.pb set-var=basic-schema

$ kafka-create-topic topic=basic partitions=1

$ kafka-ingest topic=basic format=protobuf descriptor-file=basic.pb message=Basic
{"bool": true, "int32": 1, "int64": 2, "sint32": -1, "sint64": -2, "sfixed32": -3, "sfixed64": -4, "uint32": 3, "uint64": 4, "fixed32": 5, "fixed64": 6, "float": 1.2, "double": 3.2, "bytes": "YWFh", "string": "bbb", "enum": "ENUM1", "message": {"bool": true, "int32": 1, "int64": 2, "sint32": -1, "sint64": -2, "sfixed32": -3, "sfixed64": -4, "uint32": 3, "uint64": 4, "fixed32": 5, "fixed64": 6, "float": 1.2, "double": 3.2, "bytes": "YWFh", "string": "bbb", "enum": "ENUM1"}}
{}
# Test the case where the nested message is explicitly set to all default
# values. This previously crashed Materialize (see #8930).
{"message": {}}

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE basic FROM
  KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-basic-${testdrive.seed}')
  FORMAT PROTOBUF MESSAGE '.Basic' USING SCHEMA '${basic-schema}'

> SHOW COLUMNS FROM basic
name       nullable  type
---------------------------------------
bool       false     boolean
int32      false     integer
int64      false     bigint
sint32     false     integer
sint64     false     bigint
sfixed32   false     integer
sfixed64   false     bigint
uint32     false     uint4
uint64     false     uint8
fixed32    false     uint4
fixed64    false     uint8
float      false     real
double     false     "double precision"
bytes      false     bytea
string     false     text
enum       false     text
message    true      record

> SELECT bool, int32, int64, sint32, sint64, sfixed32, sfixed64, uint32, uint64, fixed32, fixed64, float, double, bytes, string, enum, message::text FROM basic
bool  int32  int64  sint32  sint64  sfixed32  sfixed64  uint32  uint64  fixed32  fixed64  float  double  bytes  string enum  message
----
true  1      2      -1      -2      -3        -4        3       4       5        6        1.2    3.2     aaa    bbb    ENUM1 (t,1,2,-1,-2,-3,-4,3,4,5,6,1.2,3.2,\x616161,bbb,ENUM1)
false 0      0      0       0       0         0         0       0       0        0        0      0       ""     ""     ENUM0 <null>
false 0      0      0       0       0         0         0       0       0        0        0      0       ""     ""     ENUM0 "(f,0,0,0,0,0,0,0,0,0,0,0,0,\\x,\"\",ENUM0)"
