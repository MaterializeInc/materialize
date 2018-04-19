use std::collections::HashMap;
use std::io::Write;

use failure::{err_msg, Error};
use rand::random;
use serde::Serialize;
use serde_json;

use encode::{encode, encode_raw};
use schema::Schema;
use ser::Serializer;
use types::{ToAvro, Value};
use Codec;

pub const SYNC_SIZE: usize = 16;
pub const SYNC_INTERVAL: usize = 1000 * SYNC_SIZE; // TODO: parametrize in Writer

// When using SingleWriter, generating a random sync marker has no real added value
pub const SINGLE_WRITER_MARKER: &'static [u8] = &[0u8; 16];

const AVRO_OBJECT_HEADER: &'static [u8] = &[b'O', b'b', b'j', 1u8];

pub struct Writer<'a, W> {
    schema: &'a Schema,
    serializer: Serializer,
    writer: W,
    buffer: Vec<u8>,
    num_values: usize,
    codec: Codec,
    marker: Vec<u8>,
    has_header: bool,
}

impl<'a, W: Write> Writer<'a, W> {
    pub fn new(schema: &'a Schema, writer: W) -> Writer<'a, W> {
        Self::with_codec(schema, writer, Codec::Null)
    }

    pub fn with_codec(schema: &'a Schema, writer: W, codec: Codec) -> Writer<'a, W> {
        let mut marker = Vec::with_capacity(16);
        for _ in 0..16 {
            marker.push(random::<u8>());
        }

        Writer {
            schema,
            serializer: Serializer::new(),
            writer,
            buffer: Vec::with_capacity(SYNC_INTERVAL),
            num_values: 0,
            codec,
            marker,
            has_header: false,
        }
    }

    pub fn schema(&self) -> &'a Schema {
        self.schema
    }

    pub fn append<T: ToAvro>(&mut self, value: T) -> Result<usize, Error> {
        if !self.has_header {
            let header = header(self.schema, self.codec, self.marker.as_ref());
            self.append_bytes(header.as_ref())?;
            self.has_header = true;
        }

        let avro = value.avro();
        if !avro.validate(self.schema) {
            return Err(err_msg("value does not match schema"))
        }

        encode(avro.resolve(self.schema)?, &mut self.buffer);
        self.num_values += 1;

        if self.buffer.len() >= SYNC_INTERVAL {
            return self.flush()
        }

        Ok(0) // Technically, no bytes have been written
    }

    pub fn append_ser<S: Serialize>(&mut self, value: S) -> Result<usize, Error> {
        let avro_value = value.serialize(&mut self.serializer)?;
        self.append(avro_value)
    }

    fn append_marker(&mut self) -> Result<usize, Error> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        Ok(self.writer.write(&self.marker)?)
    }

    fn append_raw(&mut self, value: Value) -> Result<usize, Error> {
        self.append_bytes(encode_raw(value).as_ref())
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        Ok(self.writer.write(bytes)?)
    }

    pub fn extend<I, T: ToAvro>(&mut self, values: I) -> Result<usize, Error>
    where
        I: Iterator<Item = T>,
    {
        /*
        https://github.com/rust-lang/rfcs/issues/811 :(
        let mut stream = values
            .filter_map(|value| value.serialize(&mut self.serializer).ok())
            .map(|value| value.encode(self.schema))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("value does not match given schema"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, stream| {
                num_values += 1;
                acc.extend(stream); acc
            });
        */

        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append(value)?;
        }
        self.flush()?;

        Ok(num_bytes)
    }

    pub fn flush(&mut self) -> Result<usize, Error> {
        if self.num_values == 0 {
            return Ok(0)
        }

        self.codec.compress(&mut self.buffer)?;

        let num_values = self.num_values;
        let stream_len = self.buffer.len();

        let num_bytes = self.append_raw(num_values.avro())? + self.append_raw(stream_len.avro())?
            + self.writer.write(self.buffer.as_ref())?
            + self.append_marker()?;

        self.buffer.clear();
        self.num_values = 0;

        Ok(num_bytes)
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

pub struct SingleWriter<'a> {
    schema: &'a Schema,
    header: Vec<u8>,
    buffer: Vec<u8>,
    codec: Codec,
}

impl<'a> SingleWriter<'a> {
    pub fn new(schema: &'a Schema) -> SingleWriter<'a> {
        Self::with_codec(schema, Codec::Null)
    }

    pub fn with_codec(schema: &'a Schema, codec: Codec) -> SingleWriter<'a> {
        Self {
            schema,
            header: header(schema, codec, SINGLE_WRITER_MARKER),
            buffer: Vec::with_capacity(SYNC_INTERVAL),
            codec,
        }
    }

    pub fn schema(&self) -> &'a Schema {
        self.schema
    }

    pub fn write<W: Write, T: ToAvro>(&mut self, writer: &mut W, value: T) -> Result<(), Error> {
        let avro = value.avro();
        if !avro.validate(self.schema) {
            return Err(err_msg("value does not match schema"))
        }

        encode(avro, &mut self.buffer);
        self.codec.compress(&mut self.buffer)?;

        let num_bytes = self.buffer.len();

        writer.write_all(self.header.as_slice())?;
        writer.write_all(encode_raw(1i64.avro()).as_ref())?;
        writer.write_all(encode_raw(num_bytes.avro()).as_ref())?;
        writer.write_all(self.buffer.as_ref())?;
        writer.write_all(SINGLE_WRITER_MARKER)?;

        self.buffer.clear();

        Ok(())
    }
}

fn header(schema: &Schema, codec: Codec, marker: &[u8]) -> Vec<u8> {
    let schema_bytes = serde_json::to_string(schema).unwrap().into_bytes();

    let mut metadata = HashMap::with_capacity(2);
    metadata.insert("avro.schema", Value::Bytes(schema_bytes));
    metadata.insert("avro.codec", codec.avro());

    let mut header = Vec::new();
    header.extend_from_slice(AVRO_OBJECT_HEADER);
    encode(metadata.avro(), &mut header);
    header.extend_from_slice(marker);

    header
}
