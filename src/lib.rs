//! ```
//! extern crate avro;
//!
//! #[macro_use]
//! extern crate serde_derive;
//!
//! use avro::Codec;
//! use avro::from_value;
//! use avro::reader::Reader;
//! use avro::schema::Schema;
//! use avro::types::Record;
//! use avro::writer::Writer;
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
//!     let input = writer.into_inner();
//!     let reader = Reader::with_schema(&schema, &input[..]);
//!
//!     for record in reader {
//!         println!("{:?}", from_value::<Test>(&record));
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

pub mod codec;
pub mod de;
pub mod decode;
pub mod encode;
pub mod reader;
pub mod schema;
pub mod ser;
pub mod types;
mod util;
pub mod writer;

pub use codec::Codec;

pub use de::from_value;

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
