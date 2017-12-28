use std::collections::VecDeque;
use std::io::Read;
use std::rc::Rc;
use std::str::{from_utf8, FromStr};

use failure::{Error, err_msg};
use serde_json::from_slice;

use Codec;
use de::decode::decode;
use schema::Schema;
use types::Value;

pub struct Reader<'a, R> {
    reader: R,
    schema: &'a Schema,
    header_schema: Schema,
    codec: Codec,
    marker: [u8; 16],
    items: VecDeque<Value>,
}

impl<'a, R: Read> Reader<'a, R> {
    pub fn new(schema: &'a Schema, reader: R) -> Reader<'a, R> {
        let mut reader = Reader {
            reader: reader,
            schema: schema,
            header_schema: Schema::Null,
            codec: Codec::Null,
            marker: [0u8; 16],
            items: VecDeque::new(),
        };

        reader.read_header().unwrap();  // TODO

        reader
    }

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
                self.header_schema = schema
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
                .and_then(|codec| Codec::from_str(codec).ok()) {
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

    fn read_block(&mut self) -> Result<(), Error> {
        if let Value::Long(block_len) = decode(&Schema::Long, &mut self.reader)? {
            if let Value::Long(block_bytes) = decode(&Schema::Long, &mut self.reader)? {
                let mut raw_bytes = vec![0u8; block_bytes as usize];
                self.reader.read_exact(&mut raw_bytes)?;

                let mut marker = [0u8; 16];
                self.reader.read_exact(&mut marker)?;

                if marker != self.marker {
                    return Err(err_msg("block marker does not match header marker"));
                }

                let decompressed = self.codec.decompress(raw_bytes)?;

                self.items.clear();
                for _ in 0..block_len {
                    let item = decode(&self.header_schema, &mut &decompressed[..])?;

                    if let Some(item) = item.with_schema(self.schema) {
                        self.items.push_back(item);
                        // self.items.push_back(decode(&self.header_schema, &mut &decompressed[..])?);
                    }
                }

                return Ok(())
            }
        }

        Err(err_msg("unable to read block"))
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.items.len() == 0 {
            if let Ok(_) = self.read_block() {
                return self.next();
            }
        }

        self.items.pop_front()
    }
}
