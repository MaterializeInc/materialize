extern crate avro_rs;
#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::io::Cursor;

use avro_rs::types::{ToAvro, Value};
use avro_rs::{from_avro_datum, to_avro_datum, Schema};

// See https://github.com/apache/avro/blob/5af5e399/lang/py/test/test_io.py#L28
lazy_static! {
    static ref SCHEMAS_TO_VALIDATE: Vec<(&'static str, Value)> = vec![
        (r#""null""#, Value::Null),
        (r#""boolean""#, Value::Boolean(true)),
        (
            r#""string""#,
            Value::String("adsfasdf09809dsf-=adsf".to_string())
        ),
        (
            r#""bytes""#,
            Value::Bytes("12345abcd".to_string().into_bytes())
        ),
        (r#""int""#, Value::Int(1234)),
        (r#""long""#, Value::Long(1234)),
        (r#""float""#, Value::Float(1234.0)),
        (r#""double""#, Value::Double(1234.0)),
        (
            r#"{"type": "fixed", "name": "Test", "size": 1}"#,
            Value::Fixed(1, vec!['B' as u8])
        ),
        (
            r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
            Value::Enum(1, "B".to_string())
        ),
        (
            r#"{"type": "array", "items": "long"}"#,
            Value::Array(vec![Value::Long(1), Value::Long(3), Value::Long(2)])
        ),
        (r#"{"type": "map", "values": "long"}"#, {
            let mut map = HashMap::new();
            map.insert("a".to_string(), 1i64);
            map.insert("b".to_string(), 3i64);
            map.insert("c".to_string(), 2i64);
            map.avro()
        }),
        (
            r#"["string", "null", "long"]"#,
            Value::Union(Box::new(Value::Null))
        ),
        (
            r#"{"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}"#,
            Value::Record(vec![("f".to_string(), Value::Long(1))])
        )
    ];
    static ref BINARY_ENCODINGS: Vec<(i64, Vec<u8>)> = vec![
        (0, vec![0x00]),
        (-1, vec![0x01]),
        (1, vec![0x02]),
        (-2, vec![0x03]),
        (2, vec![0x04]),
        (-64, vec![0x7f]),
        (64, vec![0x80, 0x01]),
        (8192, vec![0x80, 0x80, 0x01]),
        (-8193, vec![0x81, 0x80, 0x01]),
    ];
}

#[test]
fn test_validate() {
    for (raw_schema, value) in SCHEMAS_TO_VALIDATE.iter() {
        let schema = Schema::parse_str(raw_schema)
            .expect(&format!("failed to parse schema: {}", raw_schema));

        assert!(
            value.validate(&schema),
            format!("value {:?} does not validate schema: {}", value, raw_schema)
        );
    }
}

#[test]
fn test_round_trip() {
    for (raw_schema, value) in SCHEMAS_TO_VALIDATE.iter() {
        let schema = Schema::parse_str(raw_schema)
            .expect(&format!("failed to parse schema: {}", raw_schema));

        let encoded =
            to_avro_datum(&schema, value.clone()).expect(&format!("failed to encode: {:?}", value));

        let mut cursor = Cursor::new(encoded);
        let decoded = from_avro_datum(&schema, &mut cursor, None)
            .expect(&format!("failed to decode encoded data for: {:?}", value));

        assert_eq!(value, &decoded);
    }
}

#[test]
fn test_int_encoding() {
    for (number, expected) in BINARY_ENCODINGS.iter() {
        let encoded = to_avro_datum(&Schema::Int, Value::Int(*number as i32)).unwrap();
        assert_eq!(&encoded, expected);
    }
}

#[test]
fn test_long_encoding() {
    for (number, expected) in BINARY_ENCODINGS.iter() {
        let encoded = to_avro_datum(&Schema::Long, Value::Long(*number)).unwrap();
        assert_eq!(&encoded, expected);
    }
}
