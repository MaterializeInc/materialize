//! ```
//! extern crate avro;
//!
//! #[macro_use]
//! extern crate serde_derive;
//!
//! use avro::Codec;
//! use avro::from_value;
//! use avro::Reader;
//! use avro::schema::Schema;
//! use avro::types::Record;
//! use avro::Writer;
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! struct Test {
//!     a: i64,
//!     b: String,
//! }
//!
//! fn main() {
//!     let raw_schema = r#"
//!         {
//!             "type": "record",
//!             "name": "test",
//!             "fields": [
//!                 {"name": "a", "type": "long", "default": 42},
//!                 {"name": "b", "type": "string"}
//!             ]
//!         }
//!     "#;
//!
//!     let schema = Schema::parse_str(raw_schema).unwrap();
//!
//!     println!("{:?}", schema);
//!
//!     let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);
//!
//!     let mut record = Record::new(writer.schema()).unwrap();
//!     record.put("a", 27i64);
//!     record.put("b", "foo");
//!
//!     writer.append(record).unwrap();
//!
//!     let test = Test {
//!         a: 27,
//!         b: "foo".to_owned(),
//!     };
//!
//!     writer.append_ser(test).unwrap();
//!
//!     writer.flush().unwrap();
//!
//!     let input = writer.into_inner();
//!     let reader = Reader::with_schema(&schema, &input[..]).unwrap();
//!
//!     for record in reader {
//!         println!("{:?}", from_value::<Test>(&record.unwrap()));
//!     }
//! }
//! ```

extern crate failure;
extern crate libflate;
extern crate rand;
#[macro_use]
extern crate serde;

extern crate serde_json;
#[cfg(feature = "snappy")]
extern crate snap;

// test dependency
#[cfg(test)]
#[macro_use]
extern crate serde_derive;

mod codec;
mod de;
mod decode;
mod encode;
mod reader;
mod ser;
mod util;
mod writer;

pub mod schema;
pub mod types;

pub use codec::Codec;
pub use de::from_value;
pub use reader::Reader;
pub use writer::{to_avro_datum, Writer};

#[cfg(test)]
mod tests {
    use super::*;
    use reader::Reader;
    use schema::Schema;
    use types::{Record, Value};

    //TODO: move where it fits better
    #[test]
    fn test_enum_default() {
        let writer_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;
        let reader_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let reader_schema = Schema::parse_str(reader_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::with_schema(&reader_schema, &input[..]).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(1, "spades".to_string())),
            ])
        );
        assert!(reader.next().is_none());
    }

    //TODO: move where it fits better
    #[test]
    fn test_enum_string_value() {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let schema = Schema::parse_str(raw_schema).unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::with_schema(&schema, &input[..]).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(2, "clubs".to_string())),
            ])
        );
        assert!(reader.next().is_none());
    }

    //TODO: move where it fits better
    #[test]
    fn test_enum_resolution() {
        let writer_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let reader_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "ninja", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let reader_schema = Schema::parse_str(reader_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::with_schema(&reader_schema, &input[..]).unwrap();
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
    }

    //TODO: move where it fits better
    #[test]
    fn test_enum_no_reader_schema() {
        let writer_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::new(&input[..]).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(2, "clubs".to_string())),
            ])
        );
    }
}
