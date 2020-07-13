// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Port of https://github.com/apache/avro/blob/master/lang/py/avro/test/test_schema.py
use std::collections::HashMap;

use avro::{types::DecimalValue, types::Value, Schema};
use chrono::{NaiveDate, NaiveDateTime};
use lazy_static::lazy_static;

lazy_static! {
    static ref UNPARSEABLE_SCHEMAS: Vec<&'static str> = vec![
        // Primitive examples
        r#""True""#,
        r#"'True"#,
        r#"invalid"#,
        r#"{"no_type": "test"}"#,
        // Fixed examples
        r#"{"type": "fixed", "name": "Missing size"}"#,
        r#"{"type": "fixed", "size": 314}",
        r#"{"type": "fixed", "size": 314, "name": "dr. spaceman"#, // AVRO-621
        // Enum examples
        r#"{"type": "enum", "name": "Status", "symbols": "Normal Caution Critical"}"#,
        r#"{"type": "enum", "name": [0, 1, 1, 2, 3, 5, 8],
            "symbols": ["Golden", "Mean"]}"#,
        r#"{"type": "enum", "symbols": ["I", "will", "fail", "no", "name"]}"#,
        r#"{"type": "enum", "name": "Test", "symbols": ["AA", "AA"]}"#,
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
        r#"{"type": "record", "name": "Address",
            "fields": [{"type": "string"}, {"type": "string", "name": "City"}]}"#,
        r#"{"type": "record", "name": "Event",
            "fields": [{"name": "Sponsor"}, {"name": "City", "type": "string"}]}"#,
        r#"{"type": "record", "name": "Rainer",
            "fields": "His vision, from the constantly passing bars"}"#,
        r#"{"name": ["Tom", "Jerry"], "type": "record",
           "fields": [{"name": "name", "type": "string"}]}"#,
    ];

    static ref UNPARSEABLE_LOGICAL_TYPES: Vec<&'static str> = vec![
        // Decimal logical type
        r#"{"type": "fixed", "logicalType": "decimal", "name": "TestDecimal2", "precision": 2, "scale": 2, "size": -2}"#,
    ];

    static ref VALID_SCHEMAS: Vec<(&'static str, Value)> = vec![
        // Primitive examples
        (r#""null""#, Value::Null),
        (r#""boolean""#, Value::Boolean(true)),
        (r#""string""#, Value::String("adsfasdf09809dsf-=adsf".to_string())),
        (r#""bytes""#, Value::Bytes("12345abcd".to_string().into_bytes())),
        (r#""int""#, Value::Int(1234)),
        (r#""long""#, Value::Long(1234)),
        (r#""float""#, Value::Float(1234.0)),
        (r#""double""#, Value::Double(1234.0)),
        // Fixed examples
        (r#"{"type": "fixed", "name": "Test", "size": 1}"#, Value::Fixed(1, vec![0])),
        (r#"{"type": "fixed", "name": "MyFixed", "size": 1,
             "namespace": "org.apache.hadoop.avro"}"#, Value::Fixed(1, vec![0])),
        // Enum examples
        (r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#, Value::Enum(0, "A".to_owned())),
        (r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#, Value::Enum(1, "B".to_owned())),
        // Array examples
        (r#"{"type": "array", "items": "long"}"#, Value::Array(vec![])),
        (r#"{"type": "array",
              "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}"#, Value::Array(vec![Value::Enum(0, "A".to_owned())])),
        // Map examples
        (r#"{"type": "map", "values": "long"}"#, Value::Map(HashMap::new())),
        (r#"{"type": "map",
             "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}"#, Value::Map(HashMap::new())),
        // Union examples
        (r#"["null", "int"]"#, Value::Union(0, Box::new(Value::Null))),
        (r#"["null", "int"]"#, Value::Union(1, Box::new(Value::Int(42)))),
        (r#"["null", "double", "string", "int"]"#, Value::Union(3, Box::new(Value::Int(42)))),
        (r#"["string", "null", "long"]"#, Value::Union(0, Box::new(Value::String("string".to_owned())))),
        // Record examples
        (r#"{"type": "record",
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
         Value::Record(vec![("intField".into(), Value::Int(0)),
                           ("longField".into(), Value::Long(0)),
                           ("stringField".into(), Value::String("string".into())),
                           ("boolField".into(), Value::Boolean(true)),
                           ("floatField".into(), Value::Float(0.0)),
                           ("doubleField".into(), Value::Double(0.0)),
                           ("bytesField".into(), Value::Bytes(vec![0])),
                           ("nullField".into(), Value::Null),
                           ("arrayField".into(), Value::Array(vec![Value::Double(0.0)])),
                           ("mapField".into(), Value::Map(HashMap::new())),
                           ("unionField".into(), Value::Union(1, Box::new(Value::Double(0.0)))),
                           ("enumField".into(), Value::Enum(1, "B".into())),
                           ("fixedField".into(), Value::Fixed(4, vec![0, 0, 0, 0])),
                           ("recordField".into(), Value::Record(vec![("label".into(), Value::String("string".into())),
                                                                     ("children".into(), Value::Array(vec![]))]))])),
        (r#"{"type": "record", "name": "ipAddr",
                     "fields": [{"name": "addr", "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                                                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}"#,
         Value::Record(vec![("addr".into(), Value::Union(1, Box::new(Value::Fixed(4, vec![0, 0, 0, 0]))))])),
        // Doc examples
        (r#"{"type": "record", "name": "TestDoc", "doc": "Doc string",
                     "fields": [{"name": "name", "type": "string", "doc": "Doc String"}]}"#,
         Value::Record(vec![("name".into(), Value::String("string".into()))])),
        (r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}"#,
         Value::Enum(0, "A".into())),
    ];

    static ref VALID_LOGICAL_TYPES: Vec<(&'static str, Value)> = vec![
        // Decimal logical type
        (r#"{"type": "fixed", "logicalType": "decimal", "name": "TestDecimal", "precision": 1, "size": 2, "scale": 1}"#,
         Value::Decimal(DecimalValue { unscaled: vec![0, 0], precision: 1, scale: 1})),
        (r#"{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}"#,
         Value::Decimal(DecimalValue { unscaled: vec![0], precision: 4, scale: 2})),
        // Date logical type
        (r#"{"type": "int", "logicalType": "date"}"#, Value::Date(NaiveDate::from_ymd(2020, 7, 13))),
        // Time millis logical type
        (r#"{"type": "int", "logicalType": "time-millis"}"#, Value::Int(0)),
        // Time micros logical type
        (r#"{"type": "long", "logicalType": "time-micros"}"#, Value::Long(0)),
        // Timestamp millis logical type
        (r#"{"type": "long", "logicalType": "timestamp-millis"}"#, Value::Timestamp(NaiveDateTime::from_timestamp(0, 0))),
        // Timestamp micros logical type
        (r#"{"type": "long", "logicalType": "timestamp-micros"}"#, Value::Timestamp(NaiveDateTime::from_timestamp(0, 0))),
    ];

    // From https://avro.apache.org/docs/current/spec.html#Logical+Types
    // "Language implementations must ignore unknown logical types when reading, and should use the
    //  underlying Avro type. If a logical type is invalid, for example a decimal with scale greater
    //  than its precision, then implementations should ignore the logical type and use the underlying
    //  Avro type."
    static ref IGNORED_LOGICAL_TYPES: Vec<(&'static str, Value)> = vec![
        (r#"{"type": "string", "logicalType": "uuid"}"#, Value::String("string".into())),
        (r#"{"type": "string", "logicalType": "unknown-logical-type"}"#, Value::String("string".into())),
        // Decimal logical type
        (r#"{"type": "bytes", "logicalType": "decimal", "scale": 0}"#, Value::Bytes(vec![])),
        (r#"{"type": "bytes", "logicalType": "decimal", "precision": 2.4, "scale": 0}"#, Value::Bytes(vec![])),
        (r#"{"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": -2}"#, Value::Bytes(vec![])),
        (r#"{"type": "bytes", "logicalType": "decimal", "precision": -2, "scale": 2}"#, Value::Bytes(vec![])),
        (r#"{"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 3}"#, Value::Bytes(vec![])),
        (r#"{"type": "fixed", "logicalType": "decimal", "name": "test", "size": 1}"#, Value::Fixed(1, vec![0])),
        (r#"{"type": "fixed", "logicalType": "decimal", "name": "test", "size": 1, "precision": 1, "scale": -2}"#,
         Value::Fixed(1, vec![0])),
        (r#"{"type": "fixed", "logicalType": "decimal", "name": "test", "size": 1, "precision": -2, "scale": 0}"#,
         Value::Fixed(1, vec![0])),
        (r#"{"type": "fixed", "logicalType": "decimal", "name": "test", "size": 1, "precision": 2, "scale": 3}"#,
         Value::Fixed(1, vec![0])),
    ];
}

#[test]
fn test_unparseable_schemas() {
    for raw_schema in UNPARSEABLE_SCHEMAS.iter() {
        assert!(
            Schema::parse_str(raw_schema).is_err(),
            format!("expected Avro schema not to parse: {}", raw_schema)
        );
    }
}

#[test]
fn test_unparseable_logical_types() {
    for raw_schema in UNPARSEABLE_LOGICAL_TYPES.iter() {
        assert!(
            Schema::parse_str(raw_schema).is_err(),
            format!("expected Avro schema not to parse: {}", raw_schema)
        );
    }
}

#[test]
fn test_valid_schemas() {
    for (raw_schema, value) in VALID_SCHEMAS.iter() {
        let schema = Schema::parse_str(raw_schema).unwrap();
        assert!(
            value.validate(schema.top_node()),
            format!("value {:?} does not validate schema: {}", value, raw_schema)
        );
    }
}

#[test]
fn test_valid_logical_types() {
    for (raw_schema, value) in VALID_LOGICAL_TYPES.iter() {
        let schema = Schema::parse_str(raw_schema).unwrap();
        assert!(
            value.validate(schema.top_node()),
            format!("value {:?} does not validate schema: {}", value, raw_schema)
        );
    }
}

#[test]
fn test_ignored_logical_types() {
    for (raw_schema, value) in IGNORED_LOGICAL_TYPES.iter() {
        let schema = Schema::parse_str(raw_schema).unwrap();
        assert!(
            value.validate(schema.top_node()),
            format!("value {:?} does not validate schema: {}", value, raw_schema)
        );
    }
}
