# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that repeated fields in a Protobuf message are decoded correctly.

$ file-append path=repeated.proto
syntax = "proto3";

message Repeated {
    enum Enum {
        ENUM0 = 0;
        ENUM1 = 1;
    }

    message Message {
        int32 m1 = 1;
        int32 m2 = 2;
    }

    repeated bool bool = 1;
    repeated int32 int32 = 2;
    repeated int64 int64 = 3;
    repeated float float = 4;
    repeated double double = 5;
    repeated bytes bytes = 6;
    repeated string string = 7;
    repeated Enum enum = 8;
    repeated Message message = 9;
}

$ protobuf-compile-descriptors inputs=repeated.proto output=repeated.pb

$ kafka-create-topic topic=repeated partitions=1

$ kafka-ingest topic=repeated format=protobuf descriptor-file=repeated.pb message=Repeated
{"bool": [true, false], "int32": [2, 1], "int64": [2, 1], "float": [3.2, 1.0], "double": [3.2, 1.0], "bytes": ["YmJi", "YWFh"], "string": ["bbb", "aaa"], "enum": ["ENUM1", "ENUM0"], "message": [{"m1": 4, "m2": 2}, {"m1": 2, "m2": 4}]}
{}

> CREATE SOURCE repeated FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-repeated-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Repeated' USING SCHEMA FILE '${testdrive.temp-dir}/repeated.pb'

> SHOW COLUMNS FROM repeated
name       nullable  type
----------------------------
bool       false     list
int32      false     list
int64      false     list
float      false     list
double     false     list
bytes      false     list
string     false     list
enum       false     list
message    false     list

> SELECT bool::text, int32::text, int64::text, float::text, double::text, string::text, bytes::text, enum::text, message::text FROM repeated
bool    int32  int64  float   double   string     bytes                enum           message
----
{t,f}  {2,1}   {2,1}  {3.2,1} {3.2,1}  {bbb,aaa}  {\x626262,\x616161}  {ENUM1,ENUM0}  "{\"(4,2)\",\"(2,4)\"}"
{}     {}      {}     {}      {}       {}         {}                   {}             {}
