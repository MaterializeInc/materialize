// Copyright 2018 Flavien Raynaud.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is derived from the avro-rs project, available at
// https://github.com/flavray/avro-rs. It was incorporated
// directly into Materialize on March 3, 2020.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
#![warn(clippy::large_futures)]
// END LINT CONFIG

//! Port of tests from:
//!     - https://github.com/apache/avro/blob/master/lang/py/avro/test/test_schema.py
//!     - https://github.com/apache/avro/tree/master/lang/c/tests/schema_tests
use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::NaiveDateTime;
use mz_avro::types::{DecimalValue, Value};
use mz_avro::Schema;
use once_cell::sync::Lazy;

static UNPARSEABLE_SCHEMAS: Lazy<Vec<&'static str>> = Lazy::new(|| {
    vec![
        // Primitive examples
        r#""True""#,
        r#"'True"#,
        r#"invalid"#,
        r#"{"no_type": "test"}"#,
        // Fixed examples
        r#"{"type": "fixed", "size": 1}"#, // Fixed type requires a name
        r#"{"type": "fixed", "name": "", "size": 1}"#, // Name cannot be empty string
        r#"{"type": "fixed", "name": "Missing size"}"#,
        r#"{"type": "fixed", "size": 314}",
        r#"{"type": "fixed", "size": 314, "name": "dr. spaceman"#, // AVRO-621
        // Enum examples
        r#"{"type": "enum", "symbols": ["A", "B"]}"#, // Enum type requires a name
        r#"{"type": "enum", "name": "", "symbols": ["A", "B"]}"#, // Name cannot be empty string
        r#"{"type": "enum", "name": "Status", "symbols": "Normal Caution Critical"}"#,
        r#"{"type": "enum", "name": [0, 1, 1, 2, 3, 5, 8],
            "symbols": ["Golden", "Mean"]}"#,
        r#"{"type": "enum", "symbols": ["I", "will", "fail", "no", "name"]}"#,
        r#"{"type": "enum", "name": "Test", "symbols": ["AA", "AA"]}"#,
        r#"{"type": "enum", "name": "2d2", "symbols": ["c3p0"]}"#,
        // Array examples
        r#"{"type": "array", "invalid": "key"}"#,
        r#"{"type": "array", "multiple": "attributes", "shouldnot": "work"}"#,
        // Map examples
        r#"{"type": "map", "invalid": "key"}"#,
        r#"{"type": "map", "multiple": "atrributes", "shouldnot": "work"}"#,
        // Union examples
        r#"["null", "null"]"#,
        r#"["long", "long"]"#,
        r#"[{"type": "array", "items": "long"},
            {"type": "array", "items": "string"}]"#,
        r#"["null", ["null", "int"], "string"]"#,
        // Record examples
        r#"{"type": "fields": [{"name": "addr", "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                                                         {"name": "IPv4", "type": "fixed", "size": 4}]}]}"#, // Record type requires a name
        r#"{"type": "record", "name": "",
                     "fields": [{"name": "addr", "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                                                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}"#, // Name cannot be empty string
        r#"{"type": "record", "name": "Address",
            "fields": [{"type": "string"}, {"type": "string", "name": "City"}]}"#,
        r#"{"type": "record", "name": "Event",
            "fields": [{"name": "Sponsor"}, {"name": "City", "type": "string"}]}"#,
        r#"{"type": "record", "name": "Rainer",
            "fields": "His vision, from the constantly passing bars"}"#,
        r#"{"name": ["Tom", "Jerry"], "type": "record",
           "fields": [{"name": "name", "type": "string"}]}"#,
        r#"{"type": "record", "fields": "His vision, from constantly passing bars", "name": "Rainer"}"#,
    ]
});

static UNPARSEABLE_LOGICAL_TYPES: Lazy<Vec<&'static str>> = Lazy::new(|| {
    vec![
        // Decimal logical type
        r#"{"type": "fixed", "logicalType": "decimal", "name": "TestDecimal2", "precision": 2, "scale": 2, "size": -2}"#,
    ]
});

static VALID_SCHEMAS: Lazy<Vec<(&'static str, Value)>> = Lazy::new(|| {
    vec![
        // Primitive examples
        (r#""null""#, Value::Null),
        (r#"{"type": "null"}"#, Value::Null),
        (r#""boolean""#, Value::Boolean(true)),
        (r#"{"type": "boolean"}"#, Value::Boolean(false)),
        (
            r#"{"type": "boolean", "default": true}"#,
            Value::Boolean(true),
        ),
        (
            r#""string""#,
            Value::String("adsfasdf09809dsf-=adsf".to_string()),
        ),
        (
            r#"{"type": "string"}"#,
            Value::String("adsfasdf09809dsf-=adsf".to_string()),
        ),
        (
            r#""bytes""#,
            Value::Bytes("12345abcd".to_string().into_bytes()),
        ),
        (
            r#"{"type": "bytes"}"#,
            Value::Bytes("helloworld".to_string().into_bytes()),
        ),
        (r#""int""#, Value::Int(1234)),
        (r#"{"type": "int"}"#, Value::Int(-1234)),
        (r#""long""#, Value::Long(1234)),
        (r#"{"type": "long"}"#, Value::Long(-1234)),
        (r#""float""#, Value::Float(1234.0)),
        (r#"{"type": "float"}"#, Value::Float(-1234.0)),
        (r#""double""#, Value::Double(1234.0)),
        (r#"{"type": "double"}"#, Value::Double(-1.0)),
        // Fixed examples
        (
            r#"{"type": "fixed", "name": "Test", "size": 1}"#,
            Value::Fixed(1, vec![0]),
        ),
        (
            r#"{"type": "fixed", "name": "MyFixed", "size": 1,
             "namespace": "org.apache.hadoop.avro"}"#,
            Value::Fixed(1, vec![0]),
        ),
        // Enum examples
        (
            r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
            Value::Enum(0, "A".to_owned()),
        ),
        (
            r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
            Value::Enum(1, "B".to_owned()),
        ),
        // Array examples
        (
            r#"{"type": "array", "items": "long"}"#,
            Value::Array(vec![]),
        ),
        (
            r#"{"type": "array",
              "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}"#,
            Value::Array(vec![Value::Enum(0, "A".to_owned())]),
        ),
        // Map examples
        (
            r#"{"type": "map", "values": "long"}"#,
            Value::Map(BTreeMap::new()),
        ),
        (
            r#"{"type": "map",
             "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}"#,
            Value::Map(BTreeMap::new()),
        ),
        // Union examples
        (
            r#"["null", "int"]"#,
            Value::Union {
                index: 0,
                inner: Box::new(Value::Null),
                n_variants: 2,
                null_variant: Some(0),
            },
        ),
        (
            r#"["null", "int"]"#,
            Value::Union {
                index: 1,
                inner: Box::new(Value::Int(42)),
                n_variants: 2,
                null_variant: Some(0),
            },
        ),
        (
            r#"["null", "double", "string", "int"]"#,
            Value::Union {
                index: 3,
                inner: Box::new(Value::Int(42)),
                n_variants: 4,
                null_variant: Some(0),
            },
        ),
        (
            r#"["string", "null", "long"]"#,
            Value::Union {
                index: 0,
                inner: Box::new(Value::String("string".into())),
                n_variants: 3,
                null_variant: Some(1),
            },
        ),
        // Record examples
        (
            r#"{"type": "record", "name": "R", "fields": []}"#,
            Value::Record(vec![]),
        ),
        (
            r#"{"type": "record",
                     "name": "Interop",
                     "namespace": "org.apache.avro",
                     "fields": [{"name": "intField", "type": "int"},
                                {"name": "longField", "type": "long"},
                                {"name": "stringField", "type": "string"},
                                {"name": "boolField", "type": "boolean"},
                                {"name": "floatField", "type": "float"},
                                {"name": "doubleField", "type": "double"},
                                {"name": "bytesField", "type": "bytes"},
                                {"name": "nullField", "type": "null"},
                                {"name": "arrayField", "type": {"type": "array", "items": "double"}},
                                {"name": "mapField", "type": {"type": "map",
                                                              "values": {"name": "Foo",
                                                                         "type": "record",
                                                                         "fields": [{"name": "label", "type": "string"}]}}},
                                {"name": "unionField", "type": ["boolean", "double", {"type": "array", "items": "bytes"}]},
                                {"name": "enumField", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}},
                                {"name": "fixedField", "type": {"type": "fixed", "name": "MD5", "size": 4}},
                                {"name": "recordField", "type": {"type": "record", "name": "Node",
                                                                 "fields": [{"name": "label", "type": "string"},
                                                                            {"name": "children",
                                                                             "type": {"type": "array",
                                                                                      "items": "Node"}}]}}]}"#,
            Value::Record(vec![
                ("intField".into(), Value::Int(0)),
                ("longField".into(), Value::Long(0)),
                ("stringField".into(), Value::String("string".into())),
                ("boolField".into(), Value::Boolean(true)),
                ("floatField".into(), Value::Float(0.0)),
                ("doubleField".into(), Value::Double(0.0)),
                ("bytesField".into(), Value::Bytes(vec![0])),
                ("nullField".into(), Value::Null),
                ("arrayField".into(), Value::Array(vec![Value::Double(0.0)])),
                ("mapField".into(), Value::Map(BTreeMap::new())),
                (
                    "unionField".into(),
                    Value::Union {
                        index: 1,
                        inner: Box::new(Value::Double(0.0)),
                        n_variants: 3,
                        null_variant: None,
                    },
                ),
                ("enumField".into(), Value::Enum(1, "B".into())),
                ("fixedField".into(), Value::Fixed(4, vec![0, 0, 0, 0])),
                (
                    "recordField".into(),
                    Value::Record(vec![
                        ("label".into(), Value::String("string".into())),
                        ("children".into(), Value::Array(vec![])),
                    ]),
                ),
            ]),
        ),
        (
            r#"{"type": "record", "name": "ipAddr",
                     "fields": [{"name": "addr", "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                                                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}"#,
            Value::Record(vec![(
                "addr".into(),
                Value::Union {
                    index: 1,
                    inner: Box::new(Value::Fixed(4, vec![0, 0, 0, 0])),
                    n_variants: 2,
                    null_variant: None,
                },
            )]),
        ),
        (
            r#"{"name": "person",
             "type": "record",
             "fields": [ {"name": "hacker", "type": "boolean", "default": false} ]}"#,
            Value::Record(vec![("hacker".into(), Value::Boolean(false))]),
        ),
        // Doc examples
        (
            r#"{"type": "record", "name": "TestDoc", "doc": "Doc string",
                     "fields": [{"name": "name", "type": "string", "doc": "Doc String"}]}"#,
            Value::Record(vec![("name".into(), Value::String("string".into()))]),
        ),
        (
            r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}"#,
            Value::Enum(0, "A".into()),
        ),
        // Custom properties
        (
            r#"{"type": "string", "ignored": "value"}"#,
            Value::String("hello, world".to_string()),
        ),
        (r#"{"type":"int", "customProp":"val"}"#, Value::Int(123)),
        (
            r#"{"type":"enum", "name":"Suit", "namespace":"org.apache.test", "aliases":["org.apache.test.OldSuit"],
         "doc":"Card Suits", "customProp":"val", "symbols":["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}"#,
            Value::Enum(0, "SPADES".to_owned()),
        ),
    ]
});

static VALID_LOGICAL_TYPES: Lazy<Vec<(&'static str, Value)>> = Lazy::new(|| {
    vec![
        // Decimal logical type
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "TestDecimal", "precision": 1, "size": 2, "scale": 1}"#,
            Value::Decimal(DecimalValue {
                unscaled: vec![0, 0],
                precision: 1,
                scale: 1,
            }),
        ),
        (
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}"#,
            Value::Decimal(DecimalValue {
                unscaled: vec![0],
                precision: 4,
                scale: 2,
            }),
        ),
        // Date logical type
        (
            r#"{"type": "int", "logicalType": "date"}"#,
            Value::Date(365 * 50 + 120),
        ),
        // Time millis logical type
        (
            r#"{"type": "int", "logicalType": "time-millis"}"#,
            Value::Int(0),
        ),
        // Time micros logical type
        (
            r#"{"type": "long", "logicalType": "time-micros"}"#,
            Value::Long(0),
        ),
        // Timestamp millis logical type
        (
            r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
            Value::Timestamp(NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
        ),
        // Timestamp micros logical type
        (
            r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
            Value::Timestamp(NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
        ),
    ]
});

// From https://avro.apache.org/docs/current/spec.html#Logical+Types
// "Language implementations must ignore unknown logical types when reading, and should use the
//  underlying Avro type. If a logical type is invalid, for example a decimal with scale greater
//  than its precision, then implementations should ignore the logical type and use the underlying
//  Avro type."
static IGNORED_LOGICAL_TYPES: Lazy<Vec<(&'static str, Value)>> = Lazy::new(|| {
    vec![
        // Unknown logical type
        (
            r#"{"type": "string", "logicalType": "unknown-logical-type"}"#,
            Value::String("string".into()),
        ),
        (r#"{"type": "int", "logicalType": "date1"}"#, Value::Int(1)),
        // Decimal logical type
        (
            r#"{"type": "bytes", "logicalType": "decimal", "scale": 0}"#,
            Value::Bytes(vec![]),
        ),
        (
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 2.4, "scale": 0}"#,
            Value::Bytes(vec![]),
        ),
        (
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": -2}"#,
            Value::Bytes(vec![]),
        ),
        (
            r#"{"type": "bytes", "logicalType": "decimal", "precision": -2, "scale": 2}"#,
            Value::Bytes(vec![]),
        ),
        (
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 3}"#,
            Value::Bytes(vec![]),
        ),
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "test", "size": 1}"#,
            Value::Fixed(1, vec![0]),
        ),
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "test", "size": 1, "precision": 1, "scale": -2}"#,
            Value::Fixed(1, vec![0]),
        ),
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "test", "size": 1, "precision": -2, "scale": 0}"#,
            Value::Fixed(1, vec![0]),
        ),
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "test", "size": 1, "precision": 2, "scale": 3}"#,
            Value::Fixed(1, vec![0]),
        ),
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "TestIgnored", "precision": -10, "scale": 2, "size": 5}"#,
            Value::Fixed(5, vec![0, 0, 0, 0, 0]),
        ),
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "TestIgnored", "scale": 2, "size": 5}"#,
            Value::Fixed(5, vec![0, 0, 0, 0, 0]),
        ),
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "TestIgnored", "precision": 2, "scale": 3, "size": 2}"#,
            Value::Fixed(2, vec![0, 0]),
        ),
        (
            r#"{"type": "fixed", "logicalType": "decimal", "name": "TestIgnored", "precision": 311, "size": 2}"#,
            Value::Fixed(2, vec![0, 0]),
        ),
        (
            r#"{"type": "float", "logicalType": "decimal", "precision": 2, "scale": 0}"#,
            Value::Float(0.0),
        ),
        // Date logical type
        (
            r#"{"type": "long", "logicalType": "date"}"#,
            Value::Long(19),
        ),
        // Time-millis logical type
        (
            r#"{"type": "int", "logicalType": "time-milis"}"#,
            Value::Int(7),
        ),
        (
            r#"{"type": "long", "logicalType": "time-millis"}"#,
            Value::Long(2),
        ),
        // Time-micros logical type
        (
            r#"{"type": "long", "logicalType": "time-micro"}"#,
            Value::Long(13),
        ),
        (
            r#"{"type": "int", "logicalType": "time-micros"}"#,
            Value::Int(42),
        ),
        // Timestamp-millis logical type
        (
            r#"{"type": "long", "logicalType": "timestamp-milis"}"#,
            Value::Long(61),
        ),
        (
            r#"{"type": "int", "logicalType": "timestamp-millis"}"#,
            Value::Int(34),
        ),
        // Timestamp-micros logical type
        (
            r#"{"type": "long", "logicalType": "timestamp-micro"}"#,
            Value::Long(3),
        ),
        (
            r#"{"type": "int", "logicalType": "timestamp-micros"}"#,
            Value::Int(1010),
        ),
        // UUID logical type - #3577
        // (r#"{"type": "string", "logicalType": "uuid"}"#, Value::String("string".into())),
    ]
});

#[mz_ore::test]
fn test_unparseable_schemas() {
    for raw_schema in UNPARSEABLE_SCHEMAS.iter() {
        assert!(
            Schema::from_str(raw_schema).is_err(),
            "expected Avro schema not to parse: {}",
            raw_schema
        );
    }
}

#[mz_ore::test]
fn test_unparseable_logical_types() {
    for raw_schema in UNPARSEABLE_LOGICAL_TYPES.iter() {
        assert!(
            Schema::from_str(raw_schema).is_err(),
            "expected Avro schema not to parse: {}",
            raw_schema
        );
    }
}

#[mz_ore::test]
fn test_valid_schemas() {
    for (raw_schema, value) in VALID_SCHEMAS.iter() {
        let schema = Schema::from_str(raw_schema).unwrap();
        assert!(
            value.validate(schema.top_node()),
            "value {:?} does not validate schema: {}",
            value,
            raw_schema
        );
    }
}

#[mz_ore::test]
fn test_valid_logical_types() {
    for (raw_schema, value) in VALID_LOGICAL_TYPES.iter() {
        let schema = Schema::from_str(raw_schema).unwrap();
        assert!(
            value.validate(schema.top_node()),
            "value {:?} does not validate schema: {}",
            value,
            raw_schema
        );
    }
}

#[mz_ore::test]
fn test_ignored_logical_types() {
    for (raw_schema, value) in IGNORED_LOGICAL_TYPES.iter() {
        let schema = Schema::from_str(raw_schema).unwrap();
        assert!(
            value.validate(schema.top_node()),
            "value {:?} does not validate schema: {}",
            value,
            raw_schema
        );
    }
}

#[mz_ore::test]
fn test_namespace_serialization() {
    let input = r#"
{
  "type": "record",
  "name": ".r1",
  "fields": [
    {
      "name": "f1",
      "type": {
        "type": "record",
        "name": "r2",
        "namespace": "ns1",
        "fields": [
          {
            "name": "f1",
            "type": [
              "null",
              "ns1.r2"
            ]
          },
          {
            "name": "f2",
            "type": [
              "null",
              ".r1"
            ]
          }
        ]
      }
    },
    {
      "name": "f2",
      "type": "ns1.r2"
    }
  ]
}
"#;
    let expected_output = r#"{
  "type": "record",
  "name": "r1",
  "fields": [
    {
      "name": "f1",
      "type": {
        "type": "record",
        "name": "r2",
        "namespace": "ns1",
        "fields": [
          {
            "name": "f1",
            "type": [
              "null",
              "r2"
            ]
          },
          {
            "name": "f2",
            "type": [
              "null",
              ".r1"
            ]
          }
        ]
      }
    },
    {
      "name": "f2",
      "type": "ns1.r2"
    }
  ]
}"#;
    let schema = Schema::from_str(input).unwrap();
    let json = serde_json::to_string_pretty(&schema).unwrap();
    assert_eq!(expected_output, json);
}
