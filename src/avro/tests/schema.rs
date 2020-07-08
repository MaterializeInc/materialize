// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Port of https://github.com/apache/avro/blob/master/lang/py/avro/test/test_schema.py
use avro::{types::Value, Schema};
use lazy_static::lazy_static;

lazy_static! {
    static ref PRIMITIVE_INVALID_EXAMPLES: Vec<&'static str> = vec![
        r#""True""#,
        r#"'True"#,
        r#"{"no_type": "test"}"#,
    ];

    static ref FIXED_INVALID_EXAMPLES: Vec<&'static str> = vec![
        r#"{"type": "fixed", "name": "Missing size"}"#,
        r#"{"type": "fixed", "size": 314}",
        r#"{"type": "fixed", "size": 314, "name": "dr. spaceman"#, // AVRO-621
    ];

    static ref ENUM_INVALID_EXAMPLES: Vec<&'static str> = vec![
        r#"{"type": "enum", "name": "Status", "symbols": "Normal Caution Critical"}"#,
        r#"{"type": "enum", "name": [0, 1, 1, 2, 3, 5, 8],
            "symbols": ["Golden", "Mean"]}"#,
        r#"{"type": "enum", "symbols": ["I", "will", "fail", "no", "name"]}"#,
        r#"{"type": "enum", "name": "Test", "symbols": ["AA", "AA"]}"#,
    ];

    static ref PRIMITIVE_VALID_EXAMPLES: Vec<(&'static str, Value)> = vec![
        (r#""null""#, Value::Null),
        (r#""boolean""#, Value::Boolean(true)),
        (r#""string""#, Value::String("adsfasdf09809dsf-=adsf".to_string())),
        (r#""bytes""#, Value::Bytes("12345abcd".to_string().into_bytes())),
        (r#""int""#, Value::Int(1234)),
        (r#""long""#, Value::Long(1234)),
        (r#""float""#, Value::Float(1234.0)),
        (r#""double""#, Value::Double(1234.0)),
    ];
}

#[test]
fn test_invalid_primitives() {
    for raw_schema in PRIMITIVE_INVALID_EXAMPLES.iter() {
        assert_cant_parse(raw_schema);
    }
}

#[test]
fn test_invalid_fixed() {
    for raw_schema in FIXED_INVALID_EXAMPLES.iter() {
        assert_cant_parse(raw_schema);
    }
}

#[test]
fn test_invalid_enum() {
    for raw_schema in ENUM_INVALID_EXAMPLES.iter() {
        assert_cant_parse(raw_schema);
    }
}

fn assert_cant_parse(raw_schema: &str) {
    assert!(
        Schema::parse_str(raw_schema).is_err(),
        format!("expected Avro schema not to parse: {}", raw_schema)
    );
}
