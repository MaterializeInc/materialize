use std::io::Cursor;

use avro_rs::{from_avro_datum, to_avro_datum, types::Value, Schema};
use lazy_static::lazy_static;

lazy_static! {
    static ref RAW_RECORD_SCHEMA: &'static str = r#"
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
    "#;
    static ref RECORD_VALUE: Value = Value::Record(vec![
        ("A".to_string(), Value::Int(1)),
        ("B".to_string(), Value::Int(2)),
        ("C".to_string(), Value::Int(3)),
        ("D".to_string(), Value::Int(4)),
        ("E".to_string(), Value::Int(5)),
        ("F".to_string(), Value::Int(6)),
        ("G".to_string(), Value::Int(7)),
    ]);
    static ref DEFAULT_VALUES: Vec<(&'static str, &'static str, Value)> = vec![
        (r#""null""#, "null", Value::Null),
        (r#""boolean""#, "true", Value::Boolean(true)),
        (r#""string""#, r#""foo""#, Value::String("foo".to_string())),
        // (r#""bytes""#, r#""\u00FF\u00FF""#, Value::Bytes(vec![0xff, 0xff])),
    ];
}

// See https://github.com/apache/avro/blob/5af5e399/lang/py/test/test_io.py#L229
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
        let writer_schema = Schema::parse_str(writer_raw_schema)
            .expect(&format!("failed to parse schema: {}", writer_raw_schema));

        let original_value = &promotable_values[i];

        for (j, reader_raw_schema) in promotable_schemas.iter().enumerate().skip(i + 1) {
            let reader_schema = Schema::parse_str(reader_raw_schema)
                .expect(&format!("failed to parse schema: {}", reader_raw_schema));

            let encoded = to_avro_datum(&writer_schema, original_value.clone())
                .expect("failed to encode value");

            let mut cursor = Cursor::new(encoded);
            let decoded = from_avro_datum(&writer_schema, &mut cursor, Some(&reader_schema))
                .expect(&format!(
                    "failed to decode {:?} with schema: {:?}",
                    original_value, reader_raw_schema
                ));

            assert_eq!(decoded, promotable_values[j]);
        }
    }
}

#[test]
fn test_unknown_symbol() {
    let writer_schema =
        Schema::parse_str(r#"{"type": "enum", "name": "Test", "symbols": ["FOO", "BAR"]}"#)
            .unwrap();

    let reader_schema =
        Schema::parse_str(r#"{"type": "enum", "name": "Test", "symbols": ["BAR", "BAZ"]}"#)
            .unwrap();

    let original_value = Value::Enum(0, "FOO".to_string());

    let encoded = to_avro_datum(&writer_schema, original_value).expect("failed to encode value");

    let mut cursor = Cursor::new(encoded);
    let decoded = from_avro_datum(&writer_schema, &mut cursor, Some(&reader_schema));

    assert!(decoded.is_err());
}

#[test]
fn test_default_value() {
    let writer_schema = Schema::parse_str(&RAW_RECORD_SCHEMA).unwrap();

    // Value will be written as is, but will be read as if its schema
    // contained only one field (not present in the writer schema),
    // with a default value.
    // This test only ensures that default value decoding works properly
    for (raw_type, raw_default_value, expected_default) in DEFAULT_VALUES.iter() {
        let value = RECORD_VALUE.clone();
        let encoded = to_avro_datum(&writer_schema, value).expect("failed to encode value");

        let raw_reader_schema = format!(
            r#"{{
                "type": "record",
                "name": "Test",
                "fields": [
                    {{"name": "H", "type": {}, "default": {}}}
                ]
            }}"#,
            raw_type, raw_default_value
        );

        let reader_schema = Schema::parse_str(&raw_reader_schema)
            .expect(&format!("failed to parse schema: {}", raw_reader_schema));

        let mut cursor = Cursor::new(encoded);
        let decoded = from_avro_datum(&writer_schema, &mut cursor, Some(&reader_schema))
            .expect("failed to decode value");

        let expected_value = Value::Record(vec![("H".to_string(), expected_default.clone())]);

        assert_eq!(decoded, expected_value);
    }
}
