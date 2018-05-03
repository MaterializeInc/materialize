//! Logic handling reading from Avro format at user level.
use std::collections::VecDeque;
use std::io::{ErrorKind, Read};
use std::rc::Rc;
use std::str::{from_utf8, FromStr};

use failure::{err_msg, Error};
use serde_json::from_slice;

use decode::decode;
use schema::Schema;
use types::Value;
use Codec;

/// Main interface for reading Avro formatted values.
///
/// To be used as an iterator:
///
/// ```no_run
/// # use avro::Reader;
/// # use std::io::Cursor;
/// # let input = Cursor::new(Vec::<u8>::new());
/// for value in Reader::new(input).unwrap() {
///     match value {
///         Ok(v) => println!("{:?}", v),
///         Err(e) => println!("Error: {}", e),
///     };
/// }
/// ```
pub struct Reader<'a, R> {
    reader: R,
    reader_schema: Option<&'a Schema>,
    writer_schema: Schema,
    codec: Codec,
    marker: [u8; 16],
    items: VecDeque<Value>,
    errored: bool,
}

impl<'a, R: Read> Reader<'a, R> {
    /// Creates a `Reader` given something implementing the `io::Read` trait to read from.
    /// No reader `Schema` will be set.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn new(reader: R) -> Result<Reader<'a, R>, Error> {
        let mut reader = Reader {
            reader,
            reader_schema: None,
            writer_schema: Schema::Null,
            codec: Codec::Null,
            marker: [0u8; 16],
            items: VecDeque::new(),
            errored: false,
        };
        reader.read_header()?;
        Ok(reader)
    }

    /// Creates a `Reader` given a reader `Schema` and something implementing the `io::Read` trait
    /// to read from.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn with_schema(schema: &'a Schema, reader: R) -> Result<Reader<'a, R>, Error> {
        let mut reader = Reader {
            reader,
            reader_schema: Some(schema),
            writer_schema: Schema::Null,
            codec: Codec::Null,
            marker: [0u8; 16],
            items: VecDeque::new(),
            errored: false,
        };
        reader.read_header()?;
        Ok(reader)
    }

    /// Get a reference to the writer `Schema`.
    pub fn writer_schema(&self) -> &Schema {
        &self.writer_schema
    }

    /// Get a reference to the optional reader `Schema`.
    pub fn reader_schema(&self) -> Option<&Schema> {
        self.reader_schema
    }

    /// Try to read the header and to set the writer `Schema`, the `Codec` and the marker based on
    /// its content.
    fn read_header(&mut self) -> Result<(), Error> {
        let meta_schema = Schema::Map(Rc::new(Schema::Bytes));

        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;

        if buf != ['O' as u8, 'b' as u8, 'j' as u8, 1u8] {
            return Err(err_msg("wrong magic in header"))
        }

        if let Value::Map(meta) = decode(&meta_schema, &mut self.reader)? {
            let schema = meta.get("avro.schema")
                .and_then(|bytes| {
                    if let &Value::Bytes(ref bytes) = bytes {
                        from_slice(bytes.as_ref()).ok()
                    } else {
                        None
                    }
                })
                .and_then(|json| Schema::parse(&json).ok());
            if let Some(schema) = schema {
                self.writer_schema = schema;
            } else {
                return Err(err_msg("unable to parse schema"))
            }

            if let Some(codec) = meta.get("avro.codec")
                .and_then(|codec| {
                    if let &Value::Bytes(ref bytes) = codec {
                        from_utf8(bytes.as_ref()).ok()
                    } else {
                        None
                    }
                })
                .and_then(|codec| Codec::from_str(codec).ok())
            {
                self.codec = codec;
            }
        } else {
            return Err(err_msg("no metadata in header"))
        }

        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf)?;
        self.marker = buf;

        Ok(())
    }

    /// Try to read a data block, also performing schema resolution for the objects contained in
    /// the block. The objects are stored in an internal buffer to the `Reader`.
    fn read_block(&mut self) -> Result<(), Error> {
        match decode(&Schema::Long, &mut self.reader) {
            Ok(block) => {
                if let Value::Long(block_len) = block {
                    if let Value::Long(block_bytes) = decode(&Schema::Long, &mut self.reader)? {
                        let mut bytes = vec![0u8; block_bytes as usize];
                        self.reader.read_exact(&mut bytes)?;

                        let mut marker = [0u8; 16];
                        self.reader.read_exact(&mut marker)?;

                        if marker != self.marker {
                            return Err(err_msg("block marker does not match header marker"))
                        }

                        self.codec.decompress(&mut bytes)?;

                        self.items.clear();
                        self.items.reserve_exact(block_len as usize);

                        for _ in 0..block_len {
                            let item = from_avro_datum(
                                &self.writer_schema,
                                &mut &bytes[..],
                                self.reader_schema,
                            )?;
                            self.items.push_back(item)
                        }

                        return Ok(())
                    }
                }
            },
            Err(e) => match e.downcast::<::std::io::Error>()?.kind() {
                // to not return any error in case we only finished to read cleanly from the stream
                ErrorKind::UnexpectedEof => return Ok(()),
                _ => (),
            },
        };
        Err(err_msg("unable to read block"))
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = Result<Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // to prevent keep on reading after the first error occurs
        if self.errored {
            return None
        };
        if self.items.len() == 0 {
            if let Err(e) = self.read_block() {
                self.errored = true;
                return Some(Err(err_msg(e)))
            }
        }

        self.items.pop_front().map(Ok)
    }
}

/// Decode a `Value` encoded in Avro format given its `Schema` and anything implementing `io::Read`
/// to read from.
///
/// In case a reader `Schema` is provided, schema resolution will also be performed.
///
/// **NOTE** This function has a quite small niche of usage and does NOT take care of reading the
/// header and consecutive data blocks; use [`Reader`](struct.Reader.html) if you don't know what
/// you are doing, instead.
pub fn from_avro_datum<R: Read>(
    writer_schema: &Schema,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> Result<Value, Error> {
    let value = decode(writer_schema, reader)?;
    match reader_schema {
        Some(ref schema) => value.resolve(schema),
        None => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use types::{Record, ToAvro};
    use Reader;

    static SCHEMA: &'static str = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;
    static ENCODED: &'static [u8] = &[
        79u8, 98u8, 106u8, 1u8, 4u8, 22u8, 97u8, 118u8, 114u8, 111u8, 46u8, 115u8, 99u8, 104u8,
        101u8, 109u8, 97u8, 222u8, 1u8, 123u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8,
        114u8, 101u8, 99u8, 111u8, 114u8, 100u8, 34u8, 44u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8,
        58u8, 34u8, 116u8, 101u8, 115u8, 116u8, 34u8, 44u8, 34u8, 102u8, 105u8, 101u8, 108u8,
        100u8, 115u8, 34u8, 58u8, 91u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8,
        97u8, 34u8, 44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 108u8, 111u8, 110u8,
        103u8, 34u8, 44u8, 34u8, 100u8, 101u8, 102u8, 97u8, 117u8, 108u8, 116u8, 34u8, 58u8, 52u8,
        50u8, 125u8, 44u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8, 98u8, 34u8,
        44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 115u8, 116u8, 114u8, 105u8,
        110u8, 103u8, 34u8, 125u8, 93u8, 125u8, 20u8, 97u8, 118u8, 114u8, 111u8, 46u8, 99u8, 111u8,
        100u8, 101u8, 99u8, 8u8, 110u8, 117u8, 108u8, 108u8, 0u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8, 4u8, 20u8, 54u8,
        6u8, 102u8, 111u8, 111u8, 54u8, 6u8, 102u8, 111u8, 111u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239,
    ];

    #[test]
    fn test_from_avro_datum() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let expected = record.avro();

        assert_eq!(
            from_avro_datum(&schema, &mut encoded, None).unwrap(),
            expected
        );
    }

    #[test]
    fn test_reader_iterator() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let reader = Reader::with_schema(&schema, ENCODED).unwrap();

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let expected = record.avro();

        for value in reader {
            assert_eq!(value.unwrap(), expected);
        }
    }

    #[test]
    fn test_reader_invalid_header() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let invalid = ENCODED.to_owned().into_iter().skip(1).collect::<Vec<u8>>();
        assert!(Reader::with_schema(&schema, &invalid[..]).is_err());
    }

    #[test]
    fn test_reader_invalid_block() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let invalid = ENCODED
            .to_owned()
            .into_iter()
            .rev()
            .skip(19)
            .collect::<Vec<u8>>()
            .into_iter()
            .rev()
            .collect::<Vec<u8>>();
        let reader = Reader::with_schema(&schema, &invalid[..]).unwrap();
        for value in reader {
            assert!(value.is_err());
        }
    }

    #[test]
    fn test_reader_empty_buffer() {
        let empty = Cursor::new(Vec::new());
        Reader::new(empty).is_err();
    }

    #[test]
    fn test_reader_only_header() {
        let invalid = ENCODED
            .to_owned()
            .into_iter()
            .take(165)
            .collect::<Vec<u8>>();
        let reader = Reader::new(&invalid[..]).unwrap();
        for value in reader {
            assert!(value.is_err());
        }
    }
}
