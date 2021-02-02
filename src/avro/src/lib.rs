// Copyright Materialize, Inc., Flavien Raynaud and other contributors.
//
// Use of this software is governed by the Apache License, Version 2.0

//! # avro
//! **[Apache Avro](https://avro.apache.org/)** is a data serialization system which provides rich
//! data structures and a compact, fast, binary data format.
//!
//! All data in Avro is schematized, as in the following example:
//!
//! ```text
//! {
//!     "type": "record",
//!     "name": "test",
//!     "fields": [
//!         {"name": "a", "type": "long", "default": 42},
//!         {"name": "b", "type": "string"}
//!     ]
//! }
//! ```
//!
//! There are basically two ways of handling Avro data in Rust:
//!
//! * **as Avro-specialized data types** based on an Avro schema;
//! * **as generic Rust types** with custom serialization logic implementing `AvroDecode`
//!   (currently only supports deserialization, not serialization).
//!
//! **avro** provides a way to read and write both these data representations easily and
//! efficiently.
//!
//! # Installing the library
//!
//!
//! Add to your `Cargo.toml`:
//!
//! ```text
//! [dependencies]
//! avro = "x.y"
//! ```
//!
//! Or in case you want to leverage the **Snappy** codec:
//!
//! ```text
//! [dependencies.avro]
//! version = "x.y"
//! features = ["snappy"]
//! ```
//!
//! # Defining a schema
//!
//! Avro data cannot exist without an Avro schema. Schemas **must** be used both while writing and
//! reading and they carry the information regarding the type of data we are
//! handling. Avro schemas are used for both schema validation and resolution of Avro data.
//!
//! Avro schemas are defined in **JSON** format and can just be parsed out of a raw string:
//!
//! ```
//! use mz_avro::Schema;
//!
//! let raw_schema = r#"
//!     {
//!         "type": "record",
//!         "name": "test",
//!         "fields": [
//!             {"name": "a", "type": "long", "default": 42},
//!             {"name": "b", "type": "string"}
//!         ]
//!     }
//! "#;
//!
//! // if the schema is not valid, this function will return an error
//! let schema: Schema = raw_schema.parse().unwrap();
//!
//! // schemas can be printed for debugging
//! println!("{:?}", schema);
//! ```
//!
//! For more information about schemas and what kind of information you can encapsulate in them,
//! please refer to the appropriate section of the
//! [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas).
//!
//! # Writing data
//!
//! Once we have defined a schema, we are ready to serialize data in Avro, validating them against
//! the provided schema in the process.
//!
//! **NOTE:** The library also provides a low-level interface for encoding a single datum in Avro
//! bytecode without generating markers and headers (for advanced use), but we highly recommend the
//! `Writer` interface to be totally Avro-compatible. Please read the API reference in case you are
//! interested.
//!
//! Given that the schema we defined above is that of an Avro *Record*, we are going to use the
//! associated type provided by the library to specify the data we want to serialize:
//!
//! ```
//! # use mz_avro::Schema;
//! use mz_avro::types::Record;
//! use mz_avro::Writer;
//! #
//! # let raw_schema = r#"
//! #     {
//! #         "type": "record",
//! #         "name": "test",
//! #         "fields": [
//! #             {"name": "a", "type": "long", "default": 42},
//! #             {"name": "b", "type": "string"}
//! #         ]
//! #     }
//! # "#;
//! # let schema: Schema = raw_schema.parse().unwrap();
//! // a writer needs a schema and something to write to
//! let mut writer = Writer::new(schema.clone(), Vec::new());
//!
//! // the Record type models our Record schema
//! let mut record = Record::new(schema.top_node()).unwrap();
//! record.put("a", 27i64);
//! record.put("b", "foo");
//!
//! // schema validation happens here
//! writer.append(record).unwrap();
//!
//! // flushing makes sure that all data gets encoded
//! writer.flush().unwrap();
//!
//! // this is how to get back the resulting avro bytecode
//! let encoded = writer.into_inner();
//! ```
//!
//! The vast majority of the time, schemas tend to define a record as a top-level container
//! encapsulating all the values to convert as fields and providing documentation for them, but in
//! case we want to directly define an Avro value, the library offers that capability via the
//! `Value` interface.
//!
//! ```
//! use mz_avro::types::Value;
//!
//! let mut value = Value::String("foo".to_string());
//! ```
//!
//! ## Using codecs to compress data
//!
//! Avro supports three different compression codecs when encoding data:
//!
//! * **Null**: leaves data uncompressed;
//! * **Deflate**: writes the data block using the deflate algorithm as specified in RFC 1951, and
//! typically implemented using the zlib library. Note that this format (unlike the "zlib format" in
//! RFC 1950) does not have a checksum.
//! * **Snappy**: uses Google's [Snappy](http://google.github.io/snappy/) compression library. Each
//! compressed block is followed by the 4-byte, big-endianCRC32 checksum of the uncompressed data in
//! the block. You must enable the `snappy` feature to use this codec.
//!
//! To specify a codec to use to compress data, just specify it while creating a `Writer`:
//! ```
//! # use mz_avro::Schema;
//! use mz_avro::Writer;
//! use mz_avro::Codec;
//! #
//! # let raw_schema = r#"
//! #     {
//! #         "type": "record",
//! #         "name": "test",
//! #         "fields": [
//! #             {"name": "a", "type": "long", "default": 42},
//! #             {"name": "b", "type": "string"}
//! #         ]
//! #     }
//! # "#;
//! # let schema: Schema = raw_schema.parse().unwrap();
//! let mut writer = Writer::with_codec(schema, Vec::new(), Codec::Deflate);
//! ```
//!
//! # Reading data
//!
//! As far as reading Avro encoded data goes, we can just use the schema encoded with the data to
//! read them. The library will do it automatically for us, as it already does for the compression
//! codec:
//!
//! ```
//!
//! use mz_avro::Reader;
//! # use mz_avro::Schema;
//! # use mz_avro::types::Record;
//! # use mz_avro::Writer;
//! #
//! # let raw_schema = r#"
//! #     {
//! #         "type": "record",
//! #         "name": "test",
//! #         "fields": [
//! #             {"name": "a", "type": "long", "default": 42},
//! #             {"name": "b", "type": "string"}
//! #         ]
//! #     }
//! # "#;
//! # let schema: Schema = raw_schema.parse().unwrap();
//! # let mut writer = Writer::new(schema.clone(), Vec::new());
//! # let mut record = Record::new(schema.top_node()).unwrap();
//! # record.put("a", 27i64);
//! # record.put("b", "foo");
//! # writer.append(record).unwrap();
//! # writer.flush().unwrap();
//! # let input = writer.into_inner();
//! // reader creation can fail in case the input to read from is not Avro-compatible or malformed
//! let reader = Reader::new(&input[..]).unwrap();
//! ```
//!
//! In case, instead, we want to specify a different (but compatible) reader schema from the schema
//! the data has been written with, we can just do as the following:
//! ```
//! use mz_avro::Schema;
//! use mz_avro::Reader;
//! # use mz_avro::types::Record;
//! # use mz_avro::Writer;
//! #
//! # let writer_raw_schema = r#"
//! #     {
//! #         "type": "record",
//! #         "name": "test",
//! #         "fields": [
//! #             {"name": "a", "type": "long", "default": 42},
//! #             {"name": "b", "type": "string"}
//! #         ]
//! #     }
//! # "#;
//! # let writer_schema: Schema = writer_raw_schema.parse().unwrap();
//! # let mut writer = Writer::new(writer_schema.clone(), Vec::new());
//! # let mut record = Record::new(writer_schema.top_node()).unwrap();
//! # record.put("a", 27i64);
//! # record.put("b", "foo");
//! # writer.append(record).unwrap();
//! # writer.flush().unwrap();
//! # let input = writer.into_inner();
//!
//! let reader_raw_schema = r#"
//!     {
//!         "type": "record",
//!         "name": "test",
//!         "fields": [
//!             {"name": "a", "type": "long", "default": 42},
//!             {"name": "b", "type": "string"},
//!             {"name": "c", "type": "long", "default": 43}
//!         ]
//!     }
//! "#;
//!
//! let reader_schema: Schema = reader_raw_schema.parse().unwrap();
//!
//! // reader creation can fail in case the input to read from is not Avro-compatible or malformed
//! let reader = Reader::with_schema(&reader_schema, &input[..]).unwrap();
//! ```
//!
//! The library will also automatically perform schema resolution while reading the data.
//!
//! For more information about schema compatibility and resolution, please refer to the
//! [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas).
//!
//! There are two ways to handle deserializing Avro data in Rust, as you can see below.
//!
//! **NOTE:** The library also provides a low-level interface for decoding a single datum in Avro
//! bytecode without markers and header (for advanced use), but we highly recommend the `Reader`
//! interface to leverage all Avro features. Please read the API reference in case you are
//! interested.
//!
//!
//! ## The avro way
//!
//! We can just read directly instances of `Value` out of the `Reader` iterator:
//!
//! ```
//! # use mz_avro::Schema;
//! # use mz_avro::types::Record;
//! # use mz_avro::Writer;
//! use mz_avro::Reader;
//! #
//! # let raw_schema = r#"
//! #     {
//! #         "type": "record",
//! #         "name": "test",
//! #         "fields": [
//! #             {"name": "a", "type": "long", "default": 42},
//! #             {"name": "b", "type": "string"}
//! #         ]
//! #     }
//! # "#;
//! # let schema: Schema = raw_schema.parse().unwrap();
//! # let mut writer = Writer::new(schema.clone(), Vec::new());
//! # let mut record = Record::new(schema.top_node()).unwrap();
//! # record.put("a", 27i64);
//! # record.put("b", "foo");
//! # writer.append(record).unwrap();
//! # writer.flush().unwrap();
//! # let input = writer.into_inner();
//! let mut reader = Reader::new(&input[..]).unwrap();
//!
//! // value is a Result of an Avro Value in case the read operation fails
//! for value in reader {
//!     println!("{:?}", value.unwrap());
//! }
//!
//! ```
//!
//! ## Custom deserialization (advanced)
//!
//! It is possible to avoid the intermediate stage of decoding to `Value`,
//! by implementing `AvroDecode` for one or more structs that will determine how to decode various schema pieces.
//!
//! This API is in flux, and more complete documentation is coming soon. For now,
//! [Materialize](https://github.com/MaterializeInc/materialize/blob/main/src/interchange/src/avro.rs)
//! furnishes the most complete example.

mod codec;
mod decode;
pub mod encode;
mod reader;
mod util;
mod writer;

pub mod error;
pub mod schema;
pub mod types;

pub use crate::codec::Codec;
pub use crate::decode::public_decoders::*;
pub use crate::decode::{
    give_value, AvroArrayAccess, AvroDecodable, AvroDecode, AvroDeserializer, AvroFieldAccess,
    AvroMapAccess, AvroRead, AvroRecordAccess, GeneralDeserializer, Skip, StatefulAvroDecodable,
    ValueOrReader,
};
pub use crate::encode::encode as encode_unchecked;
pub use crate::reader::{from_avro_datum, Reader};
pub use crate::schema::{ParseSchemaError, Schema};
pub use crate::types::SchemaResolutionError;
pub use crate::util::max_allocation_bytes;
pub use crate::writer::{to_avro_datum, write_avro_datum, ValidationError, Writer};

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::reader::Reader;
    use crate::schema::Schema;
    use crate::types::{Record, Value};

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
        let writer_schema = Schema::from_str(writer_raw_schema).unwrap();
        let reader_schema = Schema::from_str(reader_raw_schema).unwrap();
        let mut writer = Writer::with_codec(writer_schema.clone(), Vec::new(), Codec::Null);
        let mut record = Record::new(writer_schema.top_node()).unwrap();
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
        let schema = Schema::from_str(raw_schema).unwrap();
        let mut writer = Writer::with_codec(schema.clone(), Vec::new(), Codec::Null);
        let mut record = Record::new(schema.top_node()).unwrap();
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
        let writer_schema = Schema::from_str(writer_raw_schema).unwrap();
        let reader_schema = Schema::from_str(reader_raw_schema).unwrap();
        let mut writer = Writer::with_codec(writer_schema.clone(), Vec::new(), Codec::Null);
        let mut record = Record::new(writer_schema.top_node()).unwrap();
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
        let writer_schema = Schema::from_str(writer_raw_schema).unwrap();
        let mut writer = Writer::with_codec(writer_schema.clone(), Vec::new(), Codec::Null);
        let mut record = Record::new(writer_schema.top_node()).unwrap();
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
    #[test]
    fn test_datetime_value() {
        let writer_raw_schema = r#"{
        "type": "record",
        "name": "dttest",
        "fields": [
            {
                "name": "a",
                "type": {
                    "type": "long",
                    "logicalType": "timestamp-micros"
                }
            }
        ]}"#;
        let writer_schema = Schema::from_str(writer_raw_schema).unwrap();
        let mut writer = Writer::with_codec(writer_schema.clone(), Vec::new(), Codec::Null);
        let mut record = Record::new(writer_schema.top_node()).unwrap();
        let dt = chrono::NaiveDateTime::from_timestamp(1_000, 995_000_000);
        record.put("a", types::Value::Timestamp(dt));
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::new(&input[..]).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![("a".to_string(), Value::Timestamp(dt)),])
        );
    }

    #[test]
    fn test_illformed_length() {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;

        let schema = Schema::from_str(raw_schema).unwrap();

        // Would allocated 18446744073709551605 bytes
        let illformed: &[u8] = &[0x3e, 0x15, 0xff, 0x1f, 0x15, 0xff];

        let value = from_avro_datum(&schema, &mut &illformed[..]);
        assert!(value.is_err());
    }
}
