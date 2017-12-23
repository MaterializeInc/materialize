use std::collections::HashMap;
use std::io::Write;

use failure::{Error, err_msg};
use rand::random;

use encode::EncodeAvro;
use schema::Schema;
use types::{HasSchema, ToAvro, Value};

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
        let mut n = self.append_raw(Value::Bytes(vec!['O' as u8, 'b' as u8, 'j' as u8, 1u8]))?;

        let mut metadata = HashMap::new();
        metadata.insert("avro.schema", Value::Bytes(Vec::new())); // TODO
        metadata.insert("avro.codec", Value::Bytes("null".to_owned().into_bytes()));

        n += self.append_raw(metadata.avro())?;
        n += self.append_marker()?;

        Ok(n)
    }

    pub fn append<V>(&mut self, value: V) -> Result<usize, Error> where V: EncodeAvro + HasSchema {
        self.extend(vec![value])
    }

    fn append_marker(&mut self) -> Result<usize, Error> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        Ok(self.writer.write(&self.marker)?)
    }

    fn append_raw<V>(&mut self, value: V) -> Result<usize, Error> where V: EncodeAvro {
        Ok(self.writer.write(value.encode().as_ref())?)  // TODO: really?
    }

    // TODO: iterator
    pub fn extend<V>(&mut self, values: Vec<V>) -> Result<usize, Error> where V: EncodeAvro + HasSchema {
        // TODO: schema check?
        let values_len = values.len();
        let stream = values.into_iter()
            .map(|value| deconflict(self.schema, value))  // TODO not filter
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("Unable to deconflict schemas"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, stream| { acc.extend(stream); acc });

        if !self.has_header {
            self.has_header = true;
            self.header()?;
        }

        Ok(self.append_raw(values_len)? +
            self.append_raw(stream.len())? +
            self.writer.write(stream.as_ref())? +
            self.append_marker()?)
    }
}


pub fn deconflict<V>(schema: &Schema, value: V) -> Option<Vec<u8>> where V: EncodeAvro + HasSchema {
    deconflict_schemas(schema, &value.schema(), value)
}

pub fn deconflict_schemas<V>(schema: &Schema, other: &Schema, value: V) -> Option<Vec<u8>> where V: EncodeAvro + HasSchema {
    if *schema == *other {
        Some(value.encode())
    } else {
        match (schema, other) {
            /*
            (&Schema::Null, &Schema::Null) => Some(stream),
            (&Schema::Boolean, &Schema::Boolean) => Some(stream),
            (&Schema::Int, &Schema::Int) => Some(stream),
            (&Schema::Long, &Schema::Long) => Some(stream),
            (&Schema::Float, &Schema::Float) => Some(stream),
            (&Schema::Double, &Schema::Double) => Some(stream),
            (&Schema::String, &Schema::String) => Some(stream),
            (&Schema::Bytes, &Schema::Bytes) => Some(stream),
            */
            (&Schema::Bytes, &Schema::Fixed { .. }) => Some(value.encode()),  // TODO maybe not great?
            (&Schema::Fixed { size, .. }, &Schema::Bytes) => {
                let stream = value.encode();
                if stream.len() == (size as usize) {
                    Some(stream)
                } else {
                    None
                }
            },
            // (&Schema::Fixed { size, .. }, &Schema::Fixed { size: s, .. }) if size == s => Some(value.encode()),
            (&Schema::Array(ref s), &Schema::Array(ref c)) => deconflict_schemas(s, c, value),
            (&Schema::Map(ref s), &Schema::Map(ref c)) => deconflict_schemas(s, c, value),
            (&Schema::Union(ref s), &Schema::Union(ref c)) => deconflict_schemas(s, c, value),
            (
                &Schema::Record { ref fields_lookup, .. },
                &Schema::Record { fields_lookup: ref fls, .. }
            ) if fields_lookup == fls => Some(value.encode()),
            _ => None,
        }
    }
}

/*
// maybe later :)
fn deconflict_simple(schema: &Schema, other: &Schema, value: Value) -> Option<Vec<u8>> {
    match (schema, other, value) {
        (&Schema::Long, &Schema::Int, Value::Int(i)) => Some(Value::Long(i as i64).encode()),
        (&Schema::Float, &Schema::Int, Value::Int(i)) => Some(Value::Float(i as f32).encode()),
        (&Schema::Double, &Schema::Int, Value::Int(i)) => Some(Value::Double(i as f64).encode()),
        (&Schema::Float, &Schema::Long, Value::Long(i)) => Some(Value::Float(i as f32).encode()),
        (&Schema::Double, &Schema::Long, Value::Long(i)) => Some(Value::Double(i as f64).encode()),
        (&Schema::Double, &Schema::Float, Value::Float(x)) => Some(Value::Double(x as f64).encode()),
        (&Schema::String, &Schema::Bytes, Value::Bytes(bytes)) => String::from_utf8(bytes).ok().map(|s| Value::String(s).encode()),
        (&Schema::Bytes, &Schema::String, Value::String(s)) => Some(Value::Bytes(s.into_bytes()).encode()),
        _ => None,

    }
}
*/
