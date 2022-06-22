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

//! Port of https://github.com/apache/avro/blob/master/lang/py/test/test_io.py

use std::io::Cursor;
use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime};
use mz_avro::schema::resolve_schemas;
use mz_avro::types::AvroMap;
use mz_avro::{
    error::Error as AvroError,
    from_avro_datum, to_avro_datum,
    types::{DecimalValue, Value},
    Schema, ValidationError,
};
use once_cell::sync::Lazy;

static SCHEMAS_TO_VALIDATE: Lazy<Vec<(&'static str, Value)>> = Lazy::new(|| {
    vec![
        (r#""null""#, Value::Null),
        (r#""boolean""#, Value::Boolean(true)),
        (
            r#""string""#,
            Value::String("adsfasdf09809dsf-=adsf".to_string()),
        ),
        (
            r#""bytes""#,
            Value::Bytes("12345abcd".to_string().into_bytes()),
        ),
        (r#""int""#, Value::Int(1234)),
        (r#""long""#, Value::Long(1234)),
        (r#""float""#, Value::Float(1234.0)),
        (r#""double""#, Value::Double(1234.0)),
        (
            r#"{"type": "fixed", "name": "Test", "size": 1}"#,
            Value::Fixed(1, vec![b'B']),
        ),
        (
            r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
            Value::Enum(1, "B".to_string()),
        ),
        (
            r#"{"type": "array", "items": "long"}"#,
            Value::Array(vec![Value::Long(1), Value::Long(3), Value::Long(2)]),
        ),
        (
            r#"{"type": "map", "values": "long"}"#,
            Value::Map(AvroMap(
                [
                    ("a".to_string(), Value::Long(1i64)),
                    ("b".to_string(), Value::Long(3i64)),
                    ("c".to_string(), Value::Long(2i64)),
                ]
                .iter()
                .cloned()
                .collect(),
            )),
        ),
        (
            r#"["string", "null", "long"]"#,
            Value::Union {
                index: 1,
                inner: Box::new(Value::Null),
                n_variants: 3,
                null_variant: Some(1),
            },
        ),
        (
            r#"{"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}"#,
            Value::Record(vec![("f".to_string(), Value::Long(1))]),
        ),
    ]
});

static BINARY_ENCODINGS: Lazy<Vec<(i64, Vec<u8>)>> = Lazy::new(|| {
    vec![
        (0, vec![0x00]),
        (-1, vec![0x01]),
        (1, vec![0x02]),
        (-2, vec![0x03]),
        (2, vec![0x04]),
        (-64, vec![0x7f]),
        (64, vec![0x80, 0x01]),
        (8192, vec![0x80, 0x80, 0x01]),
        (-8193, vec![0x81, 0x80, 0x01]),
    ]
});

static DEFAULT_VALUE_EXAMPLES: Lazy<Vec<(&'static str, &'static str, Value)>> = Lazy::new(|| {
    vec![
        (r#""null""#, "null", Value::Null),
        (r#""boolean""#, "true", Value::Boolean(true)),
        (r#""string""#, r#""foo""#, Value::String("foo".to_string())),
        //(r#""bytes""#, r#""\u00FF\u00FF""#, Value::Bytes(vec![0xff, 0xff])),
        (r#""int""#, "5", Value::Int(5)),
        (r#""long""#, "5", Value::Long(5)),
        (r#""float""#, "1.1", Value::Float(1.1)),
        (r#""double""#, "1.1", Value::Double(1.1)),
        //(r#"{"type": "fixed", "name": "F", "size": 2}"#, r#""\u00FF\u00FF""#, Value::Bytes(vec![0xff, 0xff])),
        //(r#"{"type": "enum", "name": "F", "symbols": ["FOO", "BAR"]}"#, r#""FOO""#, Value::Enum(0, "FOO".to_string())),
        (
            r#"{"type": "array", "items": "int"}"#,
            "[1, 2, 3]",
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        ),
        (
            r#"{"type": "map", "values": "int"}"#,
            r#"{"a": 1, "b": 2}"#,
            Value::Map(AvroMap(
                [
                    ("a".to_string(), Value::Int(1)),
                    ("b".to_string(), Value::Int(2)),
                ]
                .iter()
                .cloned()
                .collect(),
            )),
        ),
        //(r#"["int", "null"]"#, "5", Value::Union(Box::new(Value::Int(5)))),
        (
            r#"{"type": "record", "name": "F", "fields": [{"name": "A", "type": "int"}]}"#,
            r#"{"A": 5}"#,
            Value::Record(vec![("A".to_string(), Value::Int(5))]),
        ),
    ]
});

static LONG_RECORD_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::from_str(
        r#"
    {
        "type": "record",
        "name": "Test",
        "fields": [
            {"name": "A", "type": "int"},
            {"name": "B", "type": "int"},
            {"name": "C", "type": "int"},
            {"name": "D", "type": "int"},
            {"name": "E", "type": "int"},
            {"name": "F", "type": "int"},
            {"name": "G", "type": "int"}
        ]
    }
    "#,
    )
    .unwrap()
});

static LONG_RECORD_DATUM: Lazy<Value> = Lazy::new(|| {
    Value::Record(vec![
        ("A".to_string(), Value::Int(1)),
        ("B".to_string(), Value::Int(2)),
        ("C".to_string(), Value::Int(3)),
        ("D".to_string(), Value::Int(4)),
        ("E".to_string(), Value::Int(5)),
        ("F".to_string(), Value::Int(6)),
        ("G".to_string(), Value::Int(7)),
    ])
});

#[test]
fn test_validate() {
    for (raw_schema, value) in SCHEMAS_TO_VALIDATE.iter() {
        let schema = Schema::from_str(raw_schema).unwrap();
        assert!(
            value.validate(schema.top_node()),
            "value {:?} does not validate schema: {}",
            value,
            raw_schema,
        );
    }
}

#[test]
fn test_round_trip() {
    for (raw_schema, value) in SCHEMAS_TO_VALIDATE.iter() {
        let schema = Schema::from_str(raw_schema).unwrap();
        let encoded = to_avro_datum(&schema, value.clone()).unwrap();
        let decoded = from_avro_datum(&schema, &mut Cursor::new(encoded)).unwrap();
        assert_eq!(value, &decoded);
    }
}

#[test]
fn test_binary_int_encoding() {
    for (number, hex_encoding) in BINARY_ENCODINGS.iter() {
        let encoded = to_avro_datum(
            &Schema::from_str("\"int\"").unwrap(),
            Value::Int(*number as i32),
        )
        .unwrap();
        assert_eq!(&encoded, hex_encoding);
    }
}

#[test]
fn test_binary_long_encoding() {
    for (number, hex_encoding) in BINARY_ENCODINGS.iter() {
        let encoded = to_avro_datum(
            &Schema::from_str("\"long\"").unwrap(),
            Value::Long(*number as i64),
        )
        .unwrap();
        assert_eq!(&encoded, hex_encoding);
    }
}

#[test]
fn test_schema_promotion() {
    // Each schema is present in order of promotion (int -> long, long -> float, float -> double)
    // Each value represents the expected decoded value when promoting a value previously encoded with a promotable schema
    let promotable_schemas = vec![r#""int""#, r#""long""#, r#""float""#, r#""double""#];
    let promotable_values = vec![
        Value::Int(219),
        Value::Long(219),
        Value::Float(219.0),
        Value::Double(219.0),
    ];
    for (i, writer_raw_schema) in promotable_schemas.iter().enumerate() {
        let writer_schema = Schema::from_str(writer_raw_schema).unwrap();
        let original_value = &promotable_values[i];
        for (j, reader_raw_schema) in promotable_schemas.iter().enumerate().skip(i + 1) {
            let reader_schema = Schema::from_str(reader_raw_schema).unwrap();
            let encoded = to_avro_datum(&writer_schema, original_value.clone()).unwrap();
            let resolved_schema = resolve_schemas(&writer_schema, &reader_schema).unwrap();
            let decoded = from_avro_datum(&resolved_schema, &mut Cursor::new(encoded))
                .unwrap_or_else(|_| {
                    panic!(
                        "failed to decode {:?} with schema: {:?}",
                        original_value, reader_raw_schema
                    )
                });
            assert_eq!(decoded, promotable_values[j]);
        }
    }
}

#[test]
fn test_unknown_symbol() {
    let writer_schema =
        Schema::from_str(r#"{"type": "enum", "name": "Test", "symbols": ["FOO", "BAR"]}"#).unwrap();
    let reader_schema =
        Schema::from_str(r#"{"type": "enum", "name": "Test", "symbols": ["BAR", "BAZ"]}"#).unwrap();
    let original_value = Value::Enum(0, "FOO".to_string());
    let encoded = to_avro_datum(&writer_schema, original_value).unwrap();
    let resolved_schema = resolve_schemas(&writer_schema, &reader_schema).unwrap();
    let decoded = from_avro_datum(&resolved_schema, &mut Cursor::new(encoded));
    assert!(decoded.is_err());
}

#[test]
fn test_default_value() {
    for (field_type, default_json, default_datum) in DEFAULT_VALUE_EXAMPLES.iter() {
        let reader_schema = Schema::from_str(&format!(
            r#"{{
                "type": "record",
                "name": "Test",
                "fields": [
                    {{"name": "H", "type": {}, "default": {}}}
                ]
            }}"#,
            field_type, default_json
        ))
        .unwrap();
        let datum_to_read = Value::Record(vec![("H".to_string(), default_datum.clone())]);
        let encoded = to_avro_datum(&LONG_RECORD_SCHEMA, LONG_RECORD_DATUM.clone()).unwrap();
        let resolved_schema = resolve_schemas(&LONG_RECORD_SCHEMA, &reader_schema).unwrap();
        let datum_read = from_avro_datum(&resolved_schema, &mut Cursor::new(encoded)).unwrap();
        assert_eq!(
            datum_read, datum_to_read,
            "{} -> {}",
            *field_type, *default_json
        );
    }
}

#[test]
fn test_no_default_value() -> Result<(), String> {
    let reader_schema = Schema::from_str(
        r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "H", "type": "int"}
            ]
        }"#,
    )
    .unwrap();
    let resolved_schema = resolve_schemas(&LONG_RECORD_SCHEMA, &reader_schema);
    match resolved_schema {
        Ok(_) => Err(String::from("Expected SchemaResolutionError, got Ok")),
        Err(ref e) => {
            if let AvroError::ResolveSchema(_) = e {
                Ok(())
            } else {
                Err(format!("Expected SchemaResultionError, got {}", e))
            }
        }
    }
}

#[test]
fn test_union_default() {
    let reader_schema = Schema::from_str(
        r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "f1", "type": ["int", "null"], "default": 42},
                {"name": "f2", "type": "long"}
            ]
        }"#,
    )
    .unwrap();
    let writer_schema = Schema::from_str(
        r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "f2", "type": "long"}
            ]
        }"#,
    )
    .unwrap();
    resolve_schemas(&writer_schema, &reader_schema).unwrap();
}

#[test]
fn test_datetime_resolutions() {
    let writer_schema = Schema::from_str(
        r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {
                    "name": "f1",
                    "type": "int"
                },
                {
                    "name": "f2",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-millis"
                    }
                },
                {
                    "name": "f3",
                    "type": "long"
                },
                {
                    "name": "f4",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-micros"
                    }
                },
                {
                    "name": "f5",
                    "type": "int"
                },
                {
                    "name": "f6",
                    "type": {
                        "type": "int",
                        "logicalType": "date"
                    }
                },
                {
                    "name": "f7",
                    "type": {
                        "type": "int",
                        "logicalType": "date"
                     }
                }
            ]
        }
"#,
    )
    .unwrap();
    let reader_schema = Schema::from_str(
        r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-millis"
                    }
                },
                {
                    "name": "f2",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-micros"
                    }
                },
                {
                    "name": "f3",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-millis"
                    }
                },
                {
                    "name": "f4",
                    "type": "long"
                },
                {
                    "name": "f5",
                    "type": {
                        "type": "int",
                        "logicalType": "date"
                    }
                },
                {
                    "name": "f6",
                    "type": "int"
                },
                {
                    "name": "f7",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-micros"
                    }
                }
            ]
        }
"#,
    )
    .unwrap();
    let datum_to_write = Value::Record(vec![
        ("f1".into(), Value::Int(1000)),
        (
            "f2".into(),
            Value::Timestamp(NaiveDateTime::from_timestamp(12345, 0)),
        ),
        ("f3".into(), Value::Long(23456000)),
        (
            "f4".into(),
            Value::Timestamp(NaiveDateTime::from_timestamp(34567, 0)),
        ),
        ("f5".into(), Value::Int(365 * 2)),
        ("f6".into(), Value::Date(NaiveDate::from_ymd(1973, 1, 1))),
        ("f7".into(), Value::Date(NaiveDate::from_ymd(1974, 1, 1))),
    ]);
    let datum_to_read = Value::Record(vec![
        (
            "f1".into(),
            Value::Timestamp(NaiveDateTime::from_timestamp(1, 0)),
        ),
        (
            "f2".into(),
            Value::Timestamp(NaiveDateTime::from_timestamp(12345, 0)),
        ),
        (
            "f3".into(),
            Value::Timestamp(NaiveDateTime::from_timestamp(23456, 0)),
        ),
        ("f4".into(), Value::Long(34567000000)),
        ("f5".into(), Value::Date(NaiveDate::from_ymd(1972, 1, 1))),
        ("f6".into(), Value::Int(365 * 3 + 1)), // +1 because 1972 was a leap year
        (
            "f7".into(),
            Value::Timestamp(NaiveDate::from_ymd(1974, 1, 1).and_hms(0, 0, 0)),
        ),
    ]);
    let encoded = to_avro_datum(&writer_schema, datum_to_write).unwrap();
    let resolved_schema = resolve_schemas(&writer_schema, &reader_schema).unwrap();
    let datum_read = from_avro_datum(&resolved_schema, &mut encoded.as_slice()).unwrap();
    assert_eq!(datum_to_read, datum_read);
}

#[test]
fn test_projection() {
    let reader_schema = Schema::from_str(
        r#"
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "E", "type": "int"},
                {"name": "F", "type": "int"}
            ]
        }
    "#,
    )
    .unwrap();
    let datum_to_read = Value::Record(vec![
        ("E".to_string(), Value::Int(5)),
        ("F".to_string(), Value::Int(6)),
    ]);
    let encoded = to_avro_datum(&LONG_RECORD_SCHEMA, LONG_RECORD_DATUM.clone()).unwrap();
    let resolved_schema = resolve_schemas(&LONG_RECORD_SCHEMA, &reader_schema).unwrap();
    let datum_read = from_avro_datum(&resolved_schema, &mut Cursor::new(encoded)).unwrap();
    assert_eq!(datum_to_read, datum_read);
}

#[test]
fn test_field_order() {
    let reader_schema = Schema::from_str(
        r#"
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "F", "type": "int"},
                {"name": "E", "type": "int"}
            ]
        }
    "#,
    )
    .unwrap();
    let datum_to_read = Value::Record(vec![
        ("F".to_string(), Value::Int(6)),
        ("E".to_string(), Value::Int(5)),
    ]);
    let encoded = to_avro_datum(&LONG_RECORD_SCHEMA, LONG_RECORD_DATUM.clone()).unwrap();
    let resolved_schema = resolve_schemas(&LONG_RECORD_SCHEMA, &reader_schema).unwrap();
    let datum_read = from_avro_datum(&resolved_schema, &mut Cursor::new(encoded)).unwrap();
    assert_eq!(datum_to_read, datum_read);
}

#[test]
fn test_type_exception() -> Result<(), String> {
    let writer_schema = Schema::from_str(
        r#"
        {
             "type": "record",
             "name": "Test",
             "fields": [
                {"name": "F", "type": "int"},
                {"name": "E", "type": "int"}
             ]
        }
    "#,
    )
    .unwrap();
    let datum_to_write = Value::Record(vec![
        ("E".to_string(), Value::Int(5)),
        ("F".to_string(), Value::String(String::from("Bad"))),
    ]);
    let encoded = to_avro_datum(&writer_schema, datum_to_write);
    match encoded {
        Ok(_) => Err(String::from("Expected ValidationError, got Ok")),
        Err(ref e) => match e.downcast_ref::<ValidationError>() {
            Some(_) => Ok(()),
            None => Err(format!("Expected ValidationError, got {}", e)),
        },
    }
}

#[test]
fn test_namespaces() {
    let schema = r#"
    {
        "type": "record",
        "name": "some_record",
        "namespace": "io.materialize",
        "fields": [
            {
                "name": "link",
                "type": [
                    "null",
                    "io.materialize.some_record"
                ]
            }
        ]
    }"#;
    let schema = Schema::from_str(schema).unwrap();
    let datum_to_write = Value::Record(vec![(
        "link".to_owned(),
        Value::Union {
            index: 1,
            inner: Box::new(Value::Record(vec![(
                "link".to_owned(),
                Value::Union {
                    index: 0,
                    inner: Box::new(Value::Null),
                    n_variants: 2,
                    null_variant: Some(0),
                },
            )])),
            n_variants: 2,
            null_variant: Some(0),
        },
    )]);
    let encoded = to_avro_datum(&schema, datum_to_write.clone()).unwrap();
    let datum_read = from_avro_datum(&schema, &mut Cursor::new(encoded)).unwrap();
    assert_eq!(datum_to_write, datum_read);
}

#[test]
fn test_self_referential_schema() {
    let schema = r#"
        {
            "name": "some_record",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "type": "array",
                        "items": {
                            "name": "some_item",
                            "type": "record",
                            "fields": [
                                {
                                    "name": "f3",
                                    "type": "string"
                                },
                                {
                                    "name": "f4",
                                    "type": "string"
                                },
                                {
                                    "name": "f5",
                                    "type": [
                                        "null",
                                        "double"
                                    ],
                                    "default": null
                                }
                            ]
                        }
                    }
                },
                {
                    "name": "f2",
                    "type": {
                        "type": "array",
                        "items": "some_item"
                    }
                }
            ]
        }
        "#;
    let schema = Schema::from_str(schema).unwrap();
    let datum_to_write = Value::Record(vec![
        (
            "f1".to_owned(),
            Value::Array(vec![
                Value::Record(vec![
                    ("f3".to_owned(), Value::String("s1".to_owned())),
                    ("f4".to_owned(), Value::String("s2".to_owned())),
                    (
                        "f5".to_owned(),
                        Value::Union {
                            index: 1,
                            inner: Box::new(Value::Double(1.234)),
                            n_variants: 2,
                            null_variant: Some(0),
                        },
                    ),
                ]),
                Value::Record(vec![
                    ("f3".to_owned(), Value::String("s3".to_owned())),
                    ("f4".to_owned(), Value::String("s4".to_owned())),
                    (
                        "f5".to_owned(),
                        Value::Union {
                            index: 1,
                            inner: Box::new(Value::Double(2.468)),
                            n_variants: 2,
                            null_variant: Some(0),
                        },
                    ),
                ]),
            ]),
        ),
        (
            "f2".to_owned(),
            Value::Array(vec![Value::Record(vec![
                ("f3".to_owned(), Value::String("s5".to_owned())),
                ("f4".to_owned(), Value::String("s6".to_owned())),
                (
                    "f5".to_owned(),
                    Value::Union {
                        index: 0,
                        inner: Box::new(Value::Null),
                        n_variants: 2,
                        null_variant: Some(0),
                    },
                ),
            ])]),
        ),
    ]);
    let encoded = to_avro_datum(&schema, datum_to_write.clone()).unwrap();
    let datum_read = from_avro_datum(&schema, &mut Cursor::new(encoded)).unwrap();
    assert_eq!(datum_to_write, datum_read);
}

#[test]
fn test_complex_resolutions() {
    // Attempt to exercise many of the hard parts of schema resolution:
    // Reordering fields in "some_record", field "f0" missing from writer, field "f3" missing
    // from reader, reordering and different set of symbols in enum in "f2",
    // "union to concrete" resolution in "f4", "concrete to union" in f5, "union to union"
    // with reordered fields in "f1", "decimal to bytes/fixed" and vice versa
    // in "f6"-"f9"
    let writer_schema = r#"
        {
            "name": "some_record",
            "type": "record",
            "fields": [
                {
                    "name": "f5",
                    "type": "long"
                },
                {
                    "name": "f4",
                    "type": [
                        "double",
                        {
                            "name": "variant1",
                            "type": "fixed",
                            "size": 1
                        },
                        "null",
                        {
                            "name": "variant2",
                            "type": "fixed",
                            "size": 2
                        }
                    ]
                },
                {
                    "name": "f3",
                    "type": "double"
                },
                {
                    "name": "f2",
                    "type": {
                        "type": "enum",
                        "symbols": ["Clubs", "Diamonds", "Hearts", "Spades", "Jokers"],
                        "name": "Suit"
                    }
                },
                {
                    "name": "f1",
                    "type": [
                        {
                            "name": "variant3",
                            "type": "fixed",
                            "size": 3
                        },
                        "double",
                        {
                            "name": "variant4",
                            "type": "fixed",
                            "size": 4
                        }
                    ]
                },
                {
                    "name": "f6",
                    "type": {
                        "type": "fixed",
                        "name": "dec",
                        "size": 4,
                        "logicalType": "decimal",
                        "precision": 2,
                        "scale": 2
                    }
                },
                {
                    "name": "f7",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 2,
                        "scale": 2
                    }
                },
                {
                    "name": "f8",
                    "type": {
                        "type": "fixed",
                        "name": "fix",
                        "size": 2
                    }
                },
                {
                    "name": "f9",
                    "type": "bytes"
                }
            ]
        }"#;

    let datum_to_write = Value::Record(vec![
        ("f5".to_owned(), Value::Long(1234)),
        (
            "f4".to_owned(),
            Value::Union {
                index: 1,
                inner: Box::new(Value::Fixed(1, vec![0])),
                n_variants: 4,
                null_variant: Some(2),
            },
        ),
        ("f3".to_owned(), Value::Double(1.234)),
        ("f2".to_owned(), Value::Enum(4, "Jokers".to_owned())),
        (
            "f1".to_owned(),
            Value::Union {
                index: 2,
                inner: Box::new(Value::Fixed(4, vec![0, 1, 2, 3])),
                n_variants: 3,
                null_variant: None,
            },
        ),
        (
            "f6".to_owned(),
            Value::Decimal(DecimalValue {
                precision: 2,
                scale: 2,
                unscaled: vec![1, 2, 3, 4],
            }),
        ),
        (
            "f7".to_owned(),
            Value::Decimal(DecimalValue {
                precision: 2,
                scale: 2,
                unscaled: vec![1, 2],
            }),
        ),
        ("f8".to_owned(), Value::Fixed(2, vec![0, 1])),
        ("f9".to_owned(), Value::Bytes(vec![3, 4, 5])),
    ]);
    let expected_read = Value::Record(vec![
        (
            "f0".to_owned(),
            Value::Record(vec![
                ("f0_0".to_owned(), Value::Double(1.234)),
                (
                    "f0_1".to_owned(),
                    Value::Union {
                        index: 0,
                        inner: Box::new(Value::Enum(1, "bar".to_owned())),
                        n_variants: 2,
                        null_variant: Some(1),
                    },
                ),
                (
                    "f0_2".to_owned(),
                    Value::Map(AvroMap(
                        vec![("a".to_string(), Value::Long(42))]
                            .into_iter()
                            .collect(),
                    )),
                ),
            ]),
        ),
        (
            "f1".to_owned(),
            Value::Union {
                index: 0,
                inner: Box::new(Value::Fixed(4, vec![0, 1, 2, 3])),
                n_variants: 3,
                null_variant: None,
            },
        ),
        ("f2".to_owned(), Value::Enum(1, "Spades".to_owned())),
        ("f4".to_owned(), Value::Fixed(1, vec![0])),
        (
            "f5".to_owned(),
            Value::Union {
                index: 1,
                inner: Box::new(Value::Long(1234)),
                n_variants: 2,
                null_variant: None,
            },
        ),
        ("f6".to_owned(), Value::Fixed(4, vec![1, 2, 3, 4])),
        ("f7".to_owned(), Value::Bytes(vec![1, 2])),
        (
            "f8".to_owned(),
            Value::Decimal(DecimalValue {
                precision: 2,
                scale: 2,
                unscaled: vec![0, 1],
            }),
        ),
        (
            "f9".to_owned(),
            Value::Decimal(DecimalValue {
                precision: 2,
                scale: 2,
                unscaled: vec![3, 4, 5],
            }),
        ),
    ]);
    let reader_schema = r#"
        {
            "name": "some_record",
            "type": "record",
            "fields": [
                {
                    "name": "f0",
                    "type": {
                        "type": "record",
                        "name": "f0_value",
                        "fields": [
                            {
                                "name": "f0_0",
                                "type": "double"
                            },
                            {
                                "name": "f0_1",
                                "type": [
                                    {
                                        "type": "enum",
                                        "symbols": ["foo", "bar", "blah"],
                                        "name": "some_enum"
                                    },
                                    "null"
                                ]
                            },
                            {
                                "name": "f0_2",
                                "type": {
                                    "type": "map",
                                    "values": "long"
                                }
                            }
                        ]
                    },
                    "default": {"f0_1": "bar", "f0_0": 1.234, "f0_2": {"a": 42}}
                },
                {
                    "name": "f1",
                    "type": [
                        {
                            "name": "variant4",
                            "type": "fixed",
                            "size": 4
                        },
                        {
                            "name": "variant3",
                            "type": "fixed",
                            "size": 3
                        },
                        "double"
                    ]
                },
                {
                    "name": "f2",
                    "type": {
                        "type": "enum",
                        "symbols": ["Hearts", "Spades", "Diamonds", "Clubs"],
                        "name": "Suit",
                        "default": "Spades"
                    }
                },
                {
                    "name": "f4",
                    "type": {
                        "name": "variant1",
                        "type": "fixed",
                        "size": 1
                    }
                },
                {
                    "name": "f5",
                    "type": [
                        {
                            "name": "extra_variant",
                            "type": "fixed",
                            "size": 10
                        },
                        "long"
                     ]
                },
                {
                    "name": "f6",
                    "type": {
                        "type": "fixed",
                        "name": "dec",
                        "size": 4
                    }
                },
                {
                    "name": "f7",
                    "type": "bytes"
                },
                {
                    "name": "f8",
                    "type": {
                        "type": "fixed",
                        "name": "fix",
                        "size": 2,
                        "logicalType": "decimal",
                        "precision": 2,
                        "scale": 2
                    }
                },
                {
                    "name": "f9",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 2,
                        "scale": 2
                    }
                }
            ]
        }
    "#;
    let writer_schema = Schema::from_str(writer_schema).unwrap();
    let reader_schema = Schema::from_str(reader_schema).unwrap();
    let resolved_schema = resolve_schemas(&writer_schema, &reader_schema).unwrap();
    let encoded = to_avro_datum(&writer_schema, datum_to_write).unwrap();
    let datum_read = from_avro_datum(&resolved_schema, &mut Cursor::new(encoded)).unwrap();
    assert_eq!(expected_read, datum_read);
}

#[test]
fn test_partially_broken_union() {
    // If one variant of a union fails to match or resolve, we should still be able to decode one of the others.
    // The first variant will fail to match (there is no "r" in the reader schema).
    // The second variant will match, but fail to resolve (there is an "s" both in the reader and writer, but their fields are irreconcilable).
    // The third variant is fine.
    let writer_schema = r#"
        [
            {
                "type": "record",
                "name": "r",
                "fields": [{"name": "a", "type": "long"}]
            },
            {
                "type": "record",
                "name": "s",
                "fields": [{"name": "b", "type": "long"}]
            },
            "long"
       ]"#;
    let reader_schema = r#"
        [
            {
                "type": "record",
                "name": "s",
                "fields": [{"name": "a", "type": "long"}]
            },
            "long"
       ]"#;
    let writer_schema = Schema::from_str(writer_schema).unwrap();
    let reader_schema = Schema::from_str(reader_schema).unwrap();
    let resolved_schema = resolve_schemas(&writer_schema, &reader_schema).unwrap();
    let datum_to_write = Value::Union {
        index: 2,
        inner: Box::new(Value::Long(42)),
        n_variants: 3,
        null_variant: None,
    };
    let datum_to_read = Value::Union {
        index: 1,
        inner: Box::new(Value::Long(42)),
        n_variants: 2,
        null_variant: None,
    };
    let encoded = to_avro_datum(&writer_schema, datum_to_write).unwrap();
    let datum_read = from_avro_datum(&resolved_schema, &mut encoded.as_slice()).unwrap();
    assert_eq!(datum_to_read, datum_read);
    let err_datum_to_write = Value::Union {
        index: 0,
        inner: Box::new(Value::Record(vec![("a".into(), Value::Long(42))])),
        n_variants: 3,
        null_variant: None,
    };
    let err_encoded = to_avro_datum(&writer_schema, err_datum_to_write).unwrap();
    let err_read = from_avro_datum(&resolved_schema, &mut err_encoded.as_slice()).unwrap_err();
    assert!(err_read.to_string().contains("Failed to match"));
    let err_datum_to_write = Value::Union {
        index: 1,
        inner: Box::new(Value::Record(vec![("b".into(), Value::Long(42))])),
        n_variants: 3,
        null_variant: None,
    };
    let err_encoded = to_avro_datum(&writer_schema, err_datum_to_write).unwrap();
    let err_read = from_avro_datum(&resolved_schema, &mut err_encoded.as_slice()).unwrap_err();
    assert!(err_read
        .to_string()
        .contains("Reader field `s.a` not found in writer"));
}
