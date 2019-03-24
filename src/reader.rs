//! Logic handling reading from Avro format at user level.
use std::io::{ErrorKind, Read};
use std::str::{from_utf8, FromStr};

use failure::Error;
use serde_json::from_slice;

use crate::decode::decode;
use crate::schema::ParseSchemaError;
use crate::schema::Schema;
use crate::types::Value;
use crate::util::{self, DecodeError};
use crate::Codec;

// Internal Block reader.
#[derive(Debug, Clone)]
struct Block<R> {
    reader: R,
    // Internal buffering to reduce allocation.
    buf: Vec<u8>,
    buf_idx: usize,
    // Number of elements expected to exist within this block.
    message_count: usize,
    marker: [u8; 16],
    codec: Codec,
    writer_schema: Schema,
}

impl<R: Read> Block<R> {
    fn new(reader: R) -> Result<Block<R>, Error> {
        let mut block = Block {
            reader,
            codec: Codec::Null,
            writer_schema: Schema::Null,
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [0; 16],
        };

        block.read_header()?;
        Ok(block)
    }

    /// Try to read the header and to set the writer `Schema`, the `Codec` and the marker based on
    /// its content.
    fn read_header(&mut self) -> Result<(), Error> {
        let meta_schema = Schema::Map(Box::new(Schema::Bytes));

        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;

        if buf != [b'O', b'b', b'j', 1u8] {
            return Err(DecodeError::new("wrong magic in header").into());
        }

        if let Value::Map(meta) = decode(&meta_schema, &mut self.reader)? {
            // TODO: surface original parse schema errors instead of coalescing them here
            let schema = meta
                .get("avro.schema")
                .and_then(|bytes| {
                    if let Value::Bytes(ref bytes) = *bytes {
                        from_slice(bytes.as_ref()).ok()
                    } else {
                        None
                    }
                })
                .and_then(|json| Schema::parse(&json).ok());
            if let Some(schema) = schema {
                self.writer_schema = schema;
            } else {
                return Err(ParseSchemaError::new("unable to parse schema").into());
            }

            if let Some(codec) = meta
                .get("avro.codec")
                .and_then(|codec| {
                    if let Value::Bytes(ref bytes) = *codec {
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
            return Err(DecodeError::new("no metadata in header").into());
        }

        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf)?;
        self.marker = buf;

        Ok(())
    }

    fn fill_buf(&mut self, n: usize) -> Result<(), Error> {
        // We don't have enough space in the buffer, need to grow it.
        if n >= self.buf.capacity() {
            self.buf.reserve(n);
        }

        unsafe {
            self.buf.set_len(n);
        }
        self.reader.read_exact(&mut self.buf[..n])?;
        self.buf_idx = 0;
        Ok(())
    }

    /// Try to read a data block, also performing schema resolution for the objects contained in
    /// the block. The objects are stored in an internal buffer to the `Reader`.
    fn read_block_next(&mut self) -> Result<(), Error> {
        assert!(self.is_empty(), "Expected self to be empty!");
        match util::read_long(&mut self.reader) {
            Ok(block_len) => {
                self.message_count = block_len as usize;
                let block_bytes = util::read_long(&mut self.reader)?;
                self.fill_buf(block_bytes as usize)?;
                let mut marker = [0u8; 16];
                self.reader.read_exact(&mut marker)?;

                if marker != self.marker {
                    return Err(
                        DecodeError::new("block marker does not match header marker").into(),
                    );
                }

                // NOTE (JAB): This doesn't fit this Reader pattern very well.
                // `self.buf` is a growable buffer that is reused as the reader is iterated.
                // For non `Codec::Null` variants, `decompress` will allocate a new `Vec`
                // and replace `buf` with the new one, instead of reusing the same buffer.
                // We can address this by using some "limited read" type to decode directly
                // into the buffer. But this is fine, for now.
                self.codec.decompress(&mut self.buf)?;

                return Ok(());
            }
            Err(e) => {
                if let ErrorKind::UnexpectedEof = e.downcast::<::std::io::Error>()?.kind() {
                    // to not return any error in case we only finished to read cleanly from the stream
                    return Ok(());
                }
            }
        };
        Err(DecodeError::new("unable to read block").into())
    }

    fn len(&self) -> usize {
        self.message_count
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read_next(&mut self, read_schema: Option<&Schema>) -> Result<Option<Value>, Error> {
        if self.is_empty() {
            self.read_block_next()?;
            if self.is_empty() {
                return Ok(None);
            }
        }

        let mut block_bytes = &self.buf[self.buf_idx..];
        let b_original = block_bytes.len();
        let item = from_avro_datum(&self.writer_schema, &mut block_bytes, read_schema)?;
        self.buf_idx += b_original - block_bytes.len();
        self.message_count -= 1;
        Ok(Some(item))
    }
}

/// Main interface for reading Avro formatted values.
///
/// To be used as an iterator:
///
/// ```no_run
/// # use avro_rs::Reader;
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
    block: Block<R>,
    reader_schema: Option<&'a Schema>,
    errored: bool,
    should_resolve_schema: bool,
}

impl<'a, R: Read> Reader<'a, R> {
    /// Creates a `Reader` given something implementing the `io::Read` trait to read from.
    /// No reader `Schema` will be set.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn new(reader: R) -> Result<Reader<'a, R>, Error> {
        let block = Block::new(reader)?;
        let reader = Reader {
            block,
            reader_schema: None,
            errored: false,
            should_resolve_schema: false,
        };
        Ok(reader)
    }

    /// Creates a `Reader` given a reader `Schema` and something implementing the `io::Read` trait
    /// to read from.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn with_schema(schema: &'a Schema, reader: R) -> Result<Reader<'a, R>, Error> {
        let block = Block::new(reader)?;
        let mut reader = Reader {
            block,
            reader_schema: Some(schema),
            errored: false,
            should_resolve_schema: false,
        };
        // Check if the reader and writer schemas disagree.
        reader.should_resolve_schema = reader.writer_schema() != schema;
        Ok(reader)
    }

    /// Get a reference to the writer `Schema`.
    pub fn writer_schema(&self) -> &Schema {
        &self.block.writer_schema
    }

    /// Get a reference to the optional reader `Schema`.
    pub fn reader_schema(&self) -> Option<&Schema> {
        self.reader_schema
    }

    #[inline]
    fn read_next(&mut self) -> Result<Option<Value>, Error> {
        let read_schema = if self.should_resolve_schema {
            self.reader_schema
        } else {
            None
        };

        self.block.read_next(read_schema)
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = Result<Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // to prevent keep on reading after the first error occurs
        if self.errored {
            return None;
        };
        match self.read_next() {
            Ok(opt) => opt.map(Ok),
            Err(e) => {
                self.errored = true;
                Some(Err(e))
            }
        }
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
    use crate::types::{Record, ToAvro};
    use crate::Reader;
    use std::io::Cursor;

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
    static UNION_SCHEMA: &'static str = r#"
            ["null", "long"]
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
        6u8, 102u8, 111u8, 111u8, 84u8, 6u8, 98u8, 97u8, 114u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8,
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
    fn test_null_union() {
        let schema = Schema::parse_str(UNION_SCHEMA).unwrap();
        let mut encoded: &'static [u8] = &[2, 0];

        assert_eq!(
            from_avro_datum(&schema, &mut encoded, None).unwrap(),
            Value::Union(Box::new(Value::Long(0)))
        );
    }

    #[test]
    fn test_reader_iterator() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let reader = Reader::with_schema(&schema, ENCODED).unwrap();

        let mut record1 = Record::new(&schema).unwrap();
        record1.put("a", 27i64);
        record1.put("b", "foo");

        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", 42i64);
        record2.put("b", "bar");

        let expected = vec![record1.avro(), record2.avro()];

        for (i, value) in reader.enumerate() {
            assert_eq!(value.unwrap(), expected[i]);
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
