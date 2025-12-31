// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf utilities for SQL purification.
//!
//! This module provides embedded well-known protobuf types that are required
//! when compiling protobuf schemas fetched from Confluent Schema Registry.
//! These types are typically bundled with protoc but are not registered in
//! the schema registry, so we embed them here to make them available during
//! schema compilation.

use std::path::Path;
use std::pin::Pin;

use protobuf_native::compiler::VirtualSourceTree;

/// Well-known protobuf types that are implicitly available to protoc.
///
/// These are the standard types from the `google.protobuf` package that are
/// commonly imported by user schemas (e.g., `google/protobuf/timestamp.proto`).
///
/// Source: https://github.com/protocolbuffers/protobuf/tree/main/src/google/protobuf
pub static WELL_KNOWN_TYPES: &[(&str, &str)] = &[
    ("google/protobuf/any.proto", ANY_PROTO),
    ("google/protobuf/api.proto", API_PROTO),
    ("google/protobuf/duration.proto", DURATION_PROTO),
    ("google/protobuf/empty.proto", EMPTY_PROTO),
    ("google/protobuf/field_mask.proto", FIELD_MASK_PROTO),
    ("google/protobuf/source_context.proto", SOURCE_CONTEXT_PROTO),
    ("google/protobuf/struct.proto", STRUCT_PROTO),
    ("google/protobuf/timestamp.proto", TIMESTAMP_PROTO),
    ("google/protobuf/type.proto", TYPE_PROTO),
    ("google/protobuf/wrappers.proto", WRAPPERS_PROTO),
];

/// Adds all well-known protobuf types to the given source tree.
///
/// This should be called before compiling protobuf schemas that may import
/// well-known types like `google/protobuf/timestamp.proto`.
pub fn add_well_known_types(mut source_tree: Pin<&mut VirtualSourceTree>) {
    for (path, content) in WELL_KNOWN_TYPES {
        source_tree
            .as_mut()
            .add_file(Path::new(path), content.as_bytes().to_vec());
    }
}

// The following are the well-known protobuf type definitions from Google's
// official protobuf repository. These are licensed under the BSD 3-Clause
// license and are embedded here to support schema compilation without
// requiring runtime file system access.

static ANY_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

option go_package = "google.golang.org/protobuf/types/known/anypb";
option java_package = "com.google.protobuf";
option java_outer_classname = "AnyProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";

message Any {
  string type_url = 1;
  bytes value = 2;
}
"#;

static API_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

import "google/protobuf/source_context.proto";
import "google/protobuf/type.proto";

option java_package = "com.google.protobuf";
option java_outer_classname = "ApiProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option go_package = "google.golang.org/protobuf/types/known/apipb";

message Api {
  string name = 1;
  repeated Method methods = 2;
  repeated Option options = 3;
  string version = 4;
  SourceContext source_context = 5;
  repeated Mixin mixins = 6;
  Syntax syntax = 7;
}

message Method {
  string name = 1;
  string request_type_url = 2;
  bool request_streaming = 3;
  string response_type_url = 4;
  bool response_streaming = 5;
  repeated Option options = 6;
  Syntax syntax = 7;
}

message Mixin {
  string name = 1;
  string root = 2;
}
"#;

static DURATION_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

option cc_enable_arenas = true;
option go_package = "google.golang.org/protobuf/types/known/durationpb";
option java_package = "com.google.protobuf";
option java_outer_classname = "DurationProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";

message Duration {
  int64 seconds = 1;
  int32 nanos = 2;
}
"#;

static EMPTY_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

option go_package = "google.golang.org/protobuf/types/known/emptypb";
option java_package = "com.google.protobuf";
option java_outer_classname = "EmptyProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option cc_enable_arenas = true;

message Empty {}
"#;

static FIELD_MASK_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

option java_package = "com.google.protobuf";
option java_outer_classname = "FieldMaskProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option go_package = "google.golang.org/protobuf/types/known/fieldmaskpb";
option cc_enable_arenas = true;

message FieldMask {
  repeated string paths = 1;
}
"#;

static SOURCE_CONTEXT_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

option java_package = "com.google.protobuf";
option java_outer_classname = "SourceContextProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option go_package = "google.golang.org/protobuf/types/known/sourcecontextpb";

message SourceContext {
  string file_name = 1;
}
"#;

static STRUCT_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

option cc_enable_arenas = true;
option go_package = "google.golang.org/protobuf/types/known/structpb";
option java_package = "com.google.protobuf";
option java_outer_classname = "StructProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";

message Struct {
  map<string, Value> fields = 1;
}

message Value {
  oneof kind {
    NullValue null_value = 1;
    double number_value = 2;
    string string_value = 3;
    bool bool_value = 4;
    Struct struct_value = 5;
    ListValue list_value = 6;
  }
}

enum NullValue {
  NULL_VALUE = 0;
}

message ListValue {
  repeated Value values = 1;
}
"#;

static TIMESTAMP_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

option cc_enable_arenas = true;
option go_package = "google.golang.org/protobuf/types/known/timestamppb";
option java_package = "com.google.protobuf";
option java_outer_classname = "TimestampProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";

message Timestamp {
  int64 seconds = 1;
  int32 nanos = 2;
}
"#;

static TYPE_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

import "google/protobuf/any.proto";
import "google/protobuf/source_context.proto";

option cc_enable_arenas = true;
option java_package = "com.google.protobuf";
option java_outer_classname = "TypeProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option go_package = "google.golang.org/protobuf/types/known/typepb";

message Type {
  string name = 1;
  repeated Field fields = 2;
  repeated string oneofs = 3;
  repeated Option options = 4;
  SourceContext source_context = 5;
  Syntax syntax = 6;
  string edition = 7;
}

message Field {
  enum Kind {
    TYPE_UNKNOWN = 0;
    TYPE_DOUBLE = 1;
    TYPE_FLOAT = 2;
    TYPE_INT64 = 3;
    TYPE_UINT64 = 4;
    TYPE_INT32 = 5;
    TYPE_FIXED64 = 6;
    TYPE_FIXED32 = 7;
    TYPE_BOOL = 8;
    TYPE_STRING = 9;
    TYPE_GROUP = 10;
    TYPE_MESSAGE = 11;
    TYPE_BYTES = 12;
    TYPE_UINT32 = 13;
    TYPE_ENUM = 14;
    TYPE_SFIXED32 = 15;
    TYPE_SFIXED64 = 16;
    TYPE_SINT32 = 17;
    TYPE_SINT64 = 18;
  }

  enum Cardinality {
    CARDINALITY_UNKNOWN = 0;
    CARDINALITY_OPTIONAL = 1;
    CARDINALITY_REQUIRED = 2;
    CARDINALITY_REPEATED = 3;
  }

  Kind kind = 1;
  Cardinality cardinality = 2;
  int32 number = 3;
  string name = 4;
  string type_url = 6;
  int32 oneof_index = 7;
  bool packed = 8;
  repeated Option options = 9;
  string json_name = 10;
  string default_value = 11;
}

message Enum {
  string name = 1;
  repeated EnumValue enumvalue = 2;
  repeated Option options = 3;
  SourceContext source_context = 4;
  Syntax syntax = 5;
  string edition = 6;
}

message EnumValue {
  string name = 1;
  int32 number = 2;
  repeated Option options = 3;
}

message Option {
  string name = 1;
  Any value = 2;
}

enum Syntax {
  SYNTAX_PROTO2 = 0;
  SYNTAX_PROTO3 = 1;
  SYNTAX_EDITIONS = 2;
}
"#;

static WRAPPERS_PROTO: &str = r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.

syntax = "proto3";

package google.protobuf;

option cc_enable_arenas = true;
option go_package = "google.golang.org/protobuf/types/known/wrapperspb";
option java_package = "com.google.protobuf";
option java_outer_classname = "WrappersProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";

message DoubleValue {
  double value = 1;
}

message FloatValue {
  float value = 1;
}

message Int64Value {
  int64 value = 1;
}

message UInt64Value {
  uint64 value = 1;
}

message Int32Value {
  int32 value = 1;
}

message UInt32Value {
  uint32 value = 1;
}

message BoolValue {
  bool value = 1;
}

message StringValue {
  string value = 1;
}

message BytesValue {
  bytes value = 1;
}
"#;
