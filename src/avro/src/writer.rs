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

//! Logic handling writing in Avro format at user level.

use std::collections::HashMap;
use std::fmt;
use std::io::{Seek, SeekFrom, Write};

use anyhow::Error;
use rand::random;

use crate::encode::{encode, encode_ref, encode_to_vec};
use crate::reader::Header;
use crate::schema::{Schema, SchemaPiece};
use crate::types::{ToAvro, Value};
use crate::{decode::AvroRead, Codec};

const SYNC_SIZE: usize = 16;
const SYNC_INTERVAL: usize = 1000 * SYNC_SIZE; // TODO: parametrize in Writer

const AVRO_OBJECT_HEADER: &[u8] = &[b'O', b'b', b'j', 1u8];

/// Describes errors happened while validating Avro data.
#[derive(Debug)]
pub struct ValidationError(String);

impl ValidationError {
    pub fn new<S>(msg: S) -> ValidationError
    where
        S: Into<String>,
    {
        ValidationError(msg.into())
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Validation error: {}", self.0)
    }
}

impl std::error::Error for ValidationError {}

/// Main interface for writing Avro Object Container Files.
pub struct Writer<W> {
    schema: Schema,
    writer: W,
    buffer: Vec<u8>,
    num_values: usize,
    codec: Option<Codec>,
    marker: [u8; 16],
    has_header: bool,
}

impl<W: Write> Writer<W> {
    /// Creates a `Writer` for the `Schema` and something implementing the [`std::io::Write`]
    /// trait to write to.
    ///
    /// This uses the no-compression [`Codec::Null`] when appending records.
    pub fn new(schema: Schema, writer: W) -> Writer<W> {
        Self::with_codec(schema, writer, Codec::Null)
    }

    /// Creates a `Writer` given a [`Schema`] and a specific compression [`Codec`]
    pub fn with_codec(schema: Schema, writer: W, codec: Codec) -> Writer<W> {
        Writer::with_codec_opt(schema, writer, Some(codec))
    }

    /// Create a `Writer` with the given parameters.
    ///
    /// All parameters have the same meaning as `with_codec`, but if `codec` is `None`
    /// then no compression will be used and the `avro.codec` field in the header will be
    /// omitted.
    pub fn with_codec_opt(schema: Schema, writer: W, codec: Option<Codec>) -> Writer<W> {
        let mut marker = [0; 16];
        for i in 0..16 {
            marker[i] = random::<u8>();
        }

        Writer {
            schema,
            writer,
            buffer: Vec::with_capacity(SYNC_INTERVAL),
            num_values: 0,
            codec,
            marker,
            has_header: false,
        }
    }

    /// Creates a `Writer` that appends to an existing OCF file.
    pub fn append_to(mut file: W) -> Result<Writer<W>, Error>
    where
        W: AvroRead + Seek + Unpin + Send,
    {
        let header = Header::from_reader(&mut file)?;
        let (schema, marker, codec) = header.into_parts();
        file.seek(SeekFrom::End(0))?;
        Ok(Writer {
            schema,
            writer: file,
            buffer: Vec::with_capacity(SYNC_INTERVAL),
            num_values: 0,
            codec: Some(codec),
            marker,
            has_header: true,
        })
    }

    /// Get a reference to the `Schema` associated to a `Writer`.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Append a compatible value (implementing the `ToAvro` trait) to a `Writer`, also performing
    /// schema validation.
    ///
    /// Return the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append<T: ToAvro>(&mut self, value: T) -> Result<usize, Error> {
        let n = if !self.has_header {
            let header = self.header()?;
            let n = self.append_bytes(header.as_ref())?;
            self.has_header = true;
            n
        } else {
            0
        };
        let avro = value.avro();
        write_value_ref(&self.schema, &avro, &mut self.buffer)?;

        self.num_values += 1;

        if self.buffer.len() >= SYNC_INTERVAL {
            return self.flush().map(|b| b + n);
        }

        Ok(n)
    }

    /// Append a compatible value to a `Writer`, also performing schema validation.
    ///
    /// Return the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append_value_ref(&mut self, value: &Value) -> Result<usize, Error> {
        let n = if !self.has_header {
            let header = self.header()?;
            let n = self.append_bytes(header.as_ref())?;
            self.has_header = true;
            n
        } else {
            0
        };

        write_value_ref(&self.schema, value, &mut self.buffer)?;

        self.num_values += 1;

        if self.buffer.len() >= SYNC_INTERVAL {
            return self.flush().map(|b| b + n);
        }

        Ok(n)
    }

    /// Extend a `Writer` with an `Iterator` of compatible values (implementing the `ToAvro`
    /// trait), also performing schema validation.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function forces the written data to be flushed (an implicit
    /// call to [`flush`](struct.Writer.html#method.flush) is performed).
    pub fn extend<I, T: ToAvro>(&mut self, values: I) -> Result<usize, Error>
    where
        I: IntoIterator<Item = T>,
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
        num_bytes += self.flush()?;

        Ok(num_bytes)
    }

    /// Extend a `Writer` by appending each `Value` from a slice, while also performing schema
    /// validation on each value appended.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function forces the written data to be flushed (an implicit
    /// call to [`flush`](struct.Writer.html#method.flush) is performed).
    pub fn extend_from_slice(&mut self, values: &[Value]) -> Result<usize, Error> {
        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append_value_ref(value)?;
        }
        num_bytes += self.flush()?;

        Ok(num_bytes)
    }

    /// Flush the content appended to a `Writer`. Call this function to make sure all the content
    /// has been written before releasing the `Writer`.
    ///
    /// Return the number of bytes written.
    pub fn flush(&mut self) -> Result<usize, Error> {
        if self.num_values == 0 {
            return Ok(0);
        }

        let compressor = self.codec.unwrap_or(Codec::Null);
        compressor.compress(&mut self.buffer)?;

        let num_values = self.num_values;
        let stream_len = self.buffer.len();

        let ls = Schema {
            named: vec![],
            indices: Default::default(),
            top: SchemaPiece::Long.into(),
        };

        let num_bytes = self.append_raw(&num_values.avro(), &ls)?
            + self.append_raw(&stream_len.avro(), &ls)?
            + self.writer.write(self.buffer.as_ref())?
            + self.append_marker()?;

        self.buffer.clear();
        self.num_values = 0;

        Ok(num_bytes)
    }

    /// Return what the `Writer` is writing to, consuming the `Writer` itself.
    ///
    /// **NOTE** This function doesn't guarantee that everything gets written before consuming the
    /// buffer. Please call [`flush`](struct.Writer.html#method.flush) before.
    pub fn into_inner(self) -> W {
        self.writer
    }

    /// Generate and append synchronization marker to the payload.
    fn append_marker(&mut self) -> Result<usize, Error> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        Ok(self.writer.write(&self.marker)?)
    }

    /// Append a raw Avro Value to the payload avoiding to encode it again.
    fn append_raw(&mut self, value: &Value, schema: &Schema) -> Result<usize, Error> {
        self.append_bytes(encode_to_vec(value, schema).as_ref())
    }

    /// Append pure bytes to the payload.
    fn append_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        Ok(self.writer.write(bytes)?)
    }

    /// Create an Avro header based on schema, codec and sync marker.
    fn header(&self) -> Result<Vec<u8>, Error> {
        let schema_bytes = serde_json::to_string(&self.schema)?.into_bytes();

        let mut metadata = HashMap::with_capacity(2);
        metadata.insert("avro.schema", Value::Bytes(schema_bytes));
        if let Some(codec) = self.codec {
            metadata.insert("avro.codec", codec.avro());
        };

        let mut header = Vec::new();
        header.extend_from_slice(AVRO_OBJECT_HEADER);
        encode(
            &metadata.avro(),
            &Schema {
                named: vec![],
                indices: Default::default(),
                top: SchemaPiece::Map(Box::new(SchemaPiece::Bytes.into())).into(),
            },
            &mut header,
        );
        header.extend_from_slice(&self.marker);

        Ok(header)
    }
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also performing
/// schema validation.
///
/// This is a function which gets the bytes buffer where to write as parameter instead of
/// creating a new one like `to_avro_datum`.
pub fn write_avro_datum<T: ToAvro>(
    schema: &Schema,
    value: T,
    buffer: &mut Vec<u8>,
) -> Result<(), Error> {
    let avro = value.avro();
    if !avro.validate(schema.top_node()) {
        return Err(ValidationError::new("value does not match schema").into());
    }
    encode(&avro, schema, buffer);
    Ok(())
}

fn write_value_ref(schema: &Schema, value: &Value, buffer: &mut Vec<u8>) -> Result<(), Error> {
    if !value.validate(schema.top_node()) {
        return Err(ValidationError::new("value does not match schema").into());
    }
    encode_ref(value, schema.top_node(), buffer);
    Ok(())
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also
/// performing schema validation.
///
/// **NOTE** This function has a quite small niche of usage and does NOT generate headers and sync
/// markers; use [`Writer`](struct.Writer.html) to be fully Avro-compatible if you don't know what
/// you are doing, instead.
pub fn to_avro_datum<T: ToAvro>(schema: &Schema, value: T) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    write_avro_datum(schema, value, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::str::FromStr;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::types::Record;
    use crate::util::zig_i64;
    use crate::Reader;

    static SCHEMA: &str = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;
    static UNION_SCHEMA: &str = r#"
            ["null", "long"]
        "#;

    #[test]
    fn test_to_avro_datum() {
        let schema = Schema::from_str(SCHEMA).unwrap();
        let mut record = Record::new(schema.top_node()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let mut expected = Vec::new();
        zig_i64(27, &mut expected);
        zig_i64(3, &mut expected);
        expected.extend(vec![b'f', b'o', b'o'].into_iter());

        assert_eq!(to_avro_datum(&schema, record).unwrap(), expected);
    }

    #[test]
    fn test_union() {
        let schema = Schema::from_str(UNION_SCHEMA).unwrap();
        let union = Value::Union {
            index: 1,
            inner: Box::new(Value::Long(3)),
            n_variants: 2,
            null_variant: Some(0),
        };

        let mut expected = Vec::new();
        zig_i64(1, &mut expected);
        zig_i64(3, &mut expected);

        assert_eq!(to_avro_datum(&schema, union).unwrap(), expected);
    }

    #[test]
    fn test_writer_append() {
        let schema = Schema::from_str(SCHEMA).unwrap();
        let mut writer = Writer::new(schema.clone(), Vec::new());

        let mut record = Record::new(schema.top_node()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let n1 = writer.append(record.clone()).unwrap();
        let n2 = writer.append(record.clone()).unwrap();
        let n3 = writer.flush().unwrap();
        let result = writer.into_inner();

        assert_eq!(n1 + n2 + n3, result.len());

        let mut header = Vec::new();
        header.extend(vec![b'O', b'b', b'j', b'\x01']);

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(vec![b'f', b'o', b'o'].into_iter());
        let data_copy = data.clone();
        data.extend(data_copy);

        // starts with magic
        assert_eq!(
            result
                .iter()
                .cloned()
                .take(header.len())
                .collect::<Vec<u8>>(),
            header
        );
        // ends with data and sync marker
        assert_eq!(
            result
                .iter()
                .cloned()
                .rev()
                .skip(16)
                .take(data.len())
                .collect::<Vec<u8>>()
                .into_iter()
                .rev()
                .collect::<Vec<u8>>(),
            data
        );
    }

    #[test]
    fn test_writer_extend() {
        let schema = Schema::from_str(SCHEMA).unwrap();
        let mut writer = Writer::new(schema.clone(), Vec::new());

        let mut record = Record::new(schema.top_node()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let record_copy = record.clone();
        let records = vec![record, record_copy];

        let n1 = writer.extend(records.into_iter()).unwrap();
        let n2 = writer.flush().unwrap();
        let result = writer.into_inner();

        assert_eq!(n1 + n2, result.len());

        let mut header = Vec::new();
        header.extend(vec![b'O', b'b', b'j', b'\x01']);

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(vec![b'f', b'o', b'o'].into_iter());
        let data_copy = data.clone();
        data.extend(data_copy);

        // starts with magic
        assert_eq!(
            result
                .iter()
                .cloned()
                .take(header.len())
                .collect::<Vec<u8>>(),
            header
        );
        // ends with data and sync marker
        assert_eq!(
            result
                .iter()
                .cloned()
                .rev()
                .skip(16)
                .take(data.len())
                .collect::<Vec<u8>>()
                .into_iter()
                .rev()
                .collect::<Vec<u8>>(),
            data
        );
    }

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct TestSerdeSerialize {
        a: i64,
        b: String,
    }

    #[test]
    fn test_writer_with_codec() {
        let schema = Schema::from_str(SCHEMA).unwrap();
        let mut writer = Writer::with_codec(schema.clone(), Vec::new(), Codec::Deflate);

        let mut record = Record::new(schema.top_node()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let n1 = writer.append(record.clone()).unwrap();
        let n2 = writer.append(record.clone()).unwrap();
        let n3 = writer.flush().unwrap();
        let result = writer.into_inner();

        assert_eq!(n1 + n2 + n3, result.len());

        let mut header = Vec::new();
        header.extend(vec![b'O', b'b', b'j', b'\x01']);

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(vec![b'f', b'o', b'o'].into_iter());
        let data_copy = data.clone();
        data.extend(data_copy);
        Codec::Deflate.compress(&mut data).unwrap();

        // starts with magic
        assert_eq!(
            result
                .iter()
                .cloned()
                .take(header.len())
                .collect::<Vec<u8>>(),
            header
        );
        // ends with data and sync marker
        assert_eq!(
            result
                .iter()
                .cloned()
                .rev()
                .skip(16)
                .take(data.len())
                .collect::<Vec<u8>>()
                .into_iter()
                .rev()
                .collect::<Vec<u8>>(),
            data
        );
    }

    #[test]
    fn test_writer_roundtrip() {
        let schema = Schema::from_str(SCHEMA).unwrap();
        let make_record = |a: i64, b| {
            let mut record = Record::new(schema.top_node()).unwrap();
            record.put("a", a);
            record.put("b", b);
            record.avro()
        };

        let mut buf = Vec::new();

        // Write out a file with two blocks.
        {
            let mut writer = Writer::new(schema.clone(), &mut buf);
            writer.append(make_record(27, "foo")).unwrap();
            writer.flush().unwrap();
            writer.append(make_record(54, "bar")).unwrap();
            writer.flush().unwrap();
        }

        // Add another block from a new writer, part i.
        {
            let mut writer = Writer::append_to(Cursor::new(&mut buf)).unwrap();
            writer.append(make_record(42, "baz")).unwrap();
            writer.flush().unwrap();
        }

        // Add another block from a new writer, part ii.
        {
            let mut writer = Writer::append_to(Cursor::new(&mut buf)).unwrap();
            writer.append(make_record(84, "zar")).unwrap();
            writer.flush().unwrap();
        }

        // Ensure all four blocks appear in the file.
        let reader = Reader::new(&buf[..]).unwrap();
        let actual: Result<Vec<_>, _> = reader.collect();
        let actual = actual.unwrap();
        assert_eq!(
            vec![
                make_record(27, "foo"),
                make_record(54, "bar"),
                make_record(42, "baz"),
                make_record(84, "zar")
            ],
            actual
        );
    }
}
