//! Logic handling writing in Avro format at user level.
use std::collections::HashMap;
use std::io::Write;

use failure::{Error, Fail};
use rand::random;
use serde::Serialize;
use serde_json;

use crate::encode::{encode, encode_ref, encode_to_vec};
use crate::schema::Schema;
use crate::ser::Serializer;
use crate::types::{ToAvro, Value};
use crate::Codec;

const SYNC_SIZE: usize = 16;
const SYNC_INTERVAL: usize = 1000 * SYNC_SIZE; // TODO: parametrize in Writer

const AVRO_OBJECT_HEADER: &[u8] = &[b'O', b'b', b'j', 1u8];

/// Describes errors happened while validating Avro data.
#[derive(Fail, Debug)]
#[fail(display = "Validation error: {}", _0)]
pub struct ValidationError(String);

impl ValidationError {
    pub fn new<S>(msg: S) -> ValidationError
    where
        S: Into<String>,
    {
        ValidationError(msg.into())
    }
}

/// Main interface for writing Avro formatted values.
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
    /// Creates a `Writer` given a `Schema` and something implementing the `io::Write` trait to write
    /// to.
    /// No compression `Codec` will be used.
    pub fn new(schema: &'a Schema, writer: W) -> Writer<'a, W> {
        Self::with_codec(schema, writer, Codec::Null)
    }

    /// Creates a `Writer` with a specific `Codec` given a `Schema` and something implementing the
    /// `io::Write` trait to write to.
    pub fn with_codec(schema: &'a Schema, writer: W, codec: Codec) -> Writer<'a, W> {
        let mut marker = Vec::with_capacity(16);
        for _ in 0..16 {
            marker.push(random::<u8>());
        }

        Writer {
            schema,
            serializer: Serializer::default(),
            writer,
            buffer: Vec::with_capacity(SYNC_INTERVAL),
            num_values: 0,
            codec,
            marker,
            has_header: false,
        }
    }

    /// Get a reference to the `Schema` associated to a `Writer`.
    pub fn schema(&self) -> &'a Schema {
        self.schema
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
        write_value_ref(self.schema, &avro, &mut self.buffer)?;

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

        write_value_ref(self.schema, value, &mut self.buffer)?;

        self.num_values += 1;

        if self.buffer.len() >= SYNC_INTERVAL {
            return self.flush().map(|b| b + n);
        }

        Ok(n)
    }

    /// Append anything implementing the `Serialize` trait to a `Writer` for
    /// [`serde`](https://docs.serde.rs/serde/index.html) compatibility, also performing schema
    /// validation.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append_ser<S: Serialize>(&mut self, value: S) -> Result<usize, Error> {
        let avro_value = value.serialize(&mut self.serializer)?;
        self.append(avro_value)
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

    /// Extend a `Writer` with an `Iterator` of anything implementing the `Serialize` trait for
    /// [`serde`](https://docs.serde.rs/serde/index.html) compatibility, also performing schema
    /// validation.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function forces the written data to be flushed (an implicit
    /// call to [`flush`](struct.Writer.html#method.flush) is performed).
    pub fn extend_ser<I, T: Serialize>(&mut self, values: I) -> Result<usize, Error>
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
            num_bytes += self.append_ser(value)?;
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

        self.codec.compress(&mut self.buffer)?;

        let num_values = self.num_values;
        let stream_len = self.buffer.len();

        let num_bytes = self.append_raw(&num_values.avro(), &Schema::Long)?
            + self.append_raw(&stream_len.avro(), &Schema::Long)?
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
        self.append_bytes(encode_to_vec(&value, schema).as_ref())
    }

    /// Append pure bytes to the payload.
    fn append_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        Ok(self.writer.write(bytes)?)
    }

    /// Create an Avro header based on schema, codec and sync marker.
    fn header(&self) -> Result<Vec<u8>, Error> {
        let schema_bytes = serde_json::to_string(self.schema)?.into_bytes();

        let mut metadata = HashMap::with_capacity(2);
        metadata.insert("avro.schema", Value::Bytes(schema_bytes));
        metadata.insert("avro.codec", self.codec.avro());

        let mut header = Vec::new();
        header.extend_from_slice(AVRO_OBJECT_HEADER);
        encode(
            &metadata.avro(),
            &Schema::Map(Box::new(Schema::Bytes)),
            &mut header,
        );
        header.extend_from_slice(&self.marker);

        Ok(header)
    }
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also performing
/// schema validation.
///
/// This is an internal function which gets the bytes buffer where to write as parameter instead of
/// creating a new one like `to_avro_datum`.
fn write_avro_datum<T: ToAvro>(
    schema: &Schema,
    value: T,
    buffer: &mut Vec<u8>,
) -> Result<(), Error> {
    let avro = value.avro();
    if !avro.validate(schema) {
        return Err(ValidationError::new("value does not match schema").into());
    }
    encode(&avro, schema, buffer);
    Ok(())
}

fn write_value_ref(schema: &Schema, value: &Value, buffer: &mut Vec<u8>) -> Result<(), Error> {
    if !value.validate(schema) {
        return Err(ValidationError::new("value does not match schema").into());
    }
    encode_ref(value, schema, buffer);
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
    use super::*;
    use crate::types::Record;
    use crate::util::zig_i64;
    use serde::{Deserialize, Serialize};

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

    #[test]
    fn test_to_avro_datum() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut record = Record::new(&schema).unwrap();
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
        let schema = Schema::parse_str(UNION_SCHEMA).unwrap();
        let union = Value::Union(Box::new(Value::Long(3)));

        let mut expected = Vec::new();
        zig_i64(1, &mut expected);
        zig_i64(3, &mut expected);

        assert_eq!(to_avro_datum(&schema, union).unwrap(), expected);
    }

    #[test]
    fn test_writer_append() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let mut record = Record::new(&schema).unwrap();
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
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let mut record = Record::new(&schema).unwrap();
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
    fn test_writer_append_ser() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let record = TestSerdeSerialize {
            a: 27,
            b: "foo".to_owned(),
        };

        let n1 = writer.append_ser(record).unwrap();
        let n2 = writer.flush().unwrap();
        let result = writer.into_inner();

        assert_eq!(n1 + n2, result.len());

        let mut header = Vec::new();
        header.extend(vec![b'O', b'b', b'j', b'\x01']);

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(vec![b'f', b'o', b'o'].into_iter());

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
    fn test_writer_extend_ser() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let record = TestSerdeSerialize {
            a: 27,
            b: "foo".to_owned(),
        };
        let record_copy = record.clone();
        let records = vec![record, record_copy];

        let n1 = writer.extend_ser(records.into_iter()).unwrap();
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

    #[test]
    fn test_writer_with_codec() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

        let mut record = Record::new(&schema).unwrap();
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
}
