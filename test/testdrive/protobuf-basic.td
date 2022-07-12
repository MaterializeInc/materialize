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
        float float = 4;
        double double = 5;
        bytes bytes = 6;
        string string = 7;
        Enum enum = 8;
    }

    bool bool = 1;
    int32 int32 = 2;
    int64 int64 = 3;
    float float = 4;
    double double = 5;
    bytes bytes = 6;
    string string = 7;
    Enum enum = 8;
    Nested message = 9;
}

$ protobuf-compile-descriptors inputs=basic.proto output=basic.pb

$ kafka-create-topic topic=basic partitions=1

$ kafka-ingest topic=basic format=protobuf descriptor-file=basic.pb message=Basic
{"bool": true, "int32": 1, "int64": 2, "float": 1.2, "double": 3.2, "bytes": "YWFh", "string": "bbb", "enum": "ENUM1", "message": {"bool": true, "int32": 1, "int64": 2, "float": 1.2, "double": 3.2, "bytes": "YWFh", "string": "bbb", "enum": "ENUM1"}}
{}
# Test the case where the nested message is explicitly set to all default
# values. This previously crashed Materialize (see #8930).
{"message": {}}

> CREATE SOURCE basic FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-basic-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Basic' USING SCHEMA FILE '${testdrive.temp-dir}/basic.pb'

> SHOW COLUMNS FROM basic
name       nullable  type
---------------------------------------
bool       false     boolean
int32      false     integer
int64      false     bigint
float      false     real
double     false     "double precision"
bytes      false     bytea
string     false     text
enum       false     text
message    true      record

> SELECT bool, int32, int64, float, double, bytes, string, enum, message::text FROM basic
bool  int32  int64  float  double  bytes  string enum  message
----
true  1      2      1.2    3.2     aaa    bbb    ENUM1 (t,1,2,1.2,3.2,\x616161,bbb,ENUM1)
false 0      0      0      0       ""     ""     ENUM0 <null>
false 0      0      0      0       ""     ""     ENUM0 "(f,0,0,0,0,\\x,\"\",ENUM0)"
