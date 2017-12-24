use std::collections::HashMap;
use std::io::Write;
use std::iter::once;

use failure::{Error, err_msg};
use rand::random;
use serde_json;

use encode::EncodeAvro;
use schema::Schema;
use types::{ToAvro, Value};

pub struct Writer<'a, 'b> {
    schema: &'a Schema,
    writer: &'b mut Write,
    marker: Vec<u8>,
    has_header: bool,
}

impl<'a, 'b> Writer<'a, 'b> {
    pub fn new<W: Write>(schema: &'a Schema, writer: &'b mut W) -> Writer<'a, 'b> where W: Write {
        let mut marker = Vec::with_capacity(16);
        for _ in 0..16 {
            marker.push(random::<u8>());
        }

        Writer {
            schema: schema,
            writer: writer,
            marker: marker,
            has_header: false,
        }
    }

    pub fn header(&mut self) -> Result<usize, Error> {
        let mut n = self.append_raw(Value::Fixed(4, vec!['O' as u8, 'b' as u8, 'j' as u8, 1u8]))?;

        let mut metadata = HashMap::new();
        metadata.insert("avro.schema", Value::Bytes(serde_json::to_string(self.schema)?.into_bytes()));
        metadata.insert("avro.codec", Value::Bytes("null".to_owned().into_bytes()));

        n += self.append_raw(metadata.avro())?;
        n += self.append_marker()?;

        Ok(n)
    }

    pub fn append<V>(&mut self, value: V) -> Result<usize, Error> where V: ToAvro {
        self.extend(once(value))
    }

    fn append_marker(&mut self) -> Result<usize, Error> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        Ok(self.writer.write(&self.marker)?)
    }

    fn append_raw<V>(&mut self, value: V) -> Result<usize, Error> where V: EncodeAvro {
        Ok(self.writer.write(value.encode().as_ref())?)  // TODO: really?
    }

    pub fn extend<I, V>(&mut self, values: I) -> Result<usize, Error>
        where V: ToAvro, I: Iterator<Item=V>
    {
        let mut num_values = 0;
        let stream = values
            .map(|value| value.avro().with_schema(self.schema))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("value does not match given schema"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, value| {
                num_values += 1;
                acc.extend(value.encode()); acc
            });

        if !self.has_header {
            self.header()?;
            self.has_header = true;
        }

        Ok(self.append_raw(num_values)? +
            self.append_raw(stream.len())? +
            self.writer.write(stream.as_ref())? +
            self.append_marker()?)
    }
}
