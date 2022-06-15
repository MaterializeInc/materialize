// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Read;
use std::rc::Rc;

use ordered_float::OrderedFloat;
use tracing::trace;
use uuid::Uuid;

use mz_avro::error::{DecodeError, Error as AvroError};
use mz_avro::{
    define_unexpected, give_value, AvroArrayAccess, AvroDecode, AvroDeserializer, AvroMapAccess,
    AvroRead, AvroRecordAccess, GeneralDeserializer, StatefulAvroDecodable, ValueDecoder,
    ValueOrReader,
};
use mz_ore::result::ResultExt;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::adt::numeric;
use mz_repr::{Datum, Row, RowPacker};

use crate::avro::ConfluentAvroResolver;

/// Manages decoding of Avro-encoded bytes.
#[derive(Debug)]
pub struct Decoder {
    csr_avro: ConfluentAvroResolver,
    debug_name: String,
    buf1: Vec<u8>,
    row_buf: Row,
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use crate::avro::Decoder;
    use mz_repr::{Datum, Row};

    #[test]
    fn test_error_followed_by_success() {
        let schema = r#"{
"type": "record",
"name": "test",
"fields": [{"name": "f1", "type": "int"}, {"name": "f2", "type": "int"}]
}"#;
        let mut decoder = Decoder::new(&schema, None, "Test".to_string(), false).unwrap();
        // This is not a valid Avro blob for the given schema
        let mut bad_bytes: &[u8] = &[0];
        assert!(block_on(decoder.decode(&mut bad_bytes)).is_err());
        // This is the blob that will make both ints in the value zero.
        let mut good_bytes: &[u8] = &[0, 0];
        // The decode should succeed with the correct value.
        assert_eq!(
            block_on(decoder.decode(&mut good_bytes)).unwrap(),
            Row::pack([Datum::Int32(0), Datum::Int32(0)])
        );
    }
}

impl Decoder {
    /// Creates a new `Decoder`
    ///
    /// The provided schema is called the "reader schema", which is the schema
    /// that we are expecting to use to decode records. The records may indicate
    /// that they are encoded with a different schema; as long as those.
    pub fn new(
        reader_schema: &str,
        ccsr_client: Option<mz_ccsr::Client>,
        debug_name: String,
        confluent_wire_format: bool,
    ) -> anyhow::Result<Decoder> {
        let csr_avro =
            ConfluentAvroResolver::new(reader_schema, ccsr_client, confluent_wire_format)?;

        Ok(Decoder {
            csr_avro,
            debug_name,
            buf1: vec![],
            row_buf: Row::default(),
        })
    }

    /// Decodes Avro-encoded `bytes` into a `Row`.
    pub async fn decode(&mut self, bytes: &mut &[u8]) -> anyhow::Result<Row> {
        // Clear out any bytes that might be left over from
        // an earlier run. This can happen if the
        // `dsr.deserialize` call returns an error,
        // causing us to return early.
        let mut packer = self.row_buf.packer();
        let (bytes2, resolved_schema, csr_schema_id) = self.csr_avro.resolve(bytes).await?;
        *bytes = bytes2;
        let dec = AvroFlatDecoder {
            packer: &mut packer,
            buf: &mut self.buf1,
            is_top: true,
        };
        let dsr = GeneralDeserializer {
            schema: resolved_schema.top_node(),
        };
        dsr.deserialize(bytes, dec).with_context(|| {
            format!(
                "unable to decode row {}",
                match csr_schema_id {
                    Some(id) => format!("(Avro schema id = {:?})", id),
                    None => "".to_string(),
                }
            )
        })?;
        trace!(
            "[customer-data] Decoded row {:?} in {}",
            self.row_buf,
            self.debug_name
        );
        Ok(self.row_buf.clone())
    }
}

pub struct AvroStringDecoder<'a> {
    pub buf: &'a mut Vec<u8>,
}

impl<'a> AvroDecode for AvroStringDecoder<'a> {
    type Out = ();
    fn string<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b str, R>,
    ) -> Result<Self::Out, AvroError> {
        match r {
            ValueOrReader::Value(val) => {
                self.buf.resize_with(val.len(), Default::default);
                val.as_bytes().read_exact(self.buf)?;
            }
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
            }
        }
        Ok(())
    }
    define_unexpected! {
        record, union_branch, array, map, enum_variant, scalar, decimal, bytes, json, uuid, fixed
    }
}

pub(super) struct OptionalRecordDecoder<'a, 'row> {
    pub packer: &'a mut RowPacker<'row>,
    pub buf: &'a mut Vec<u8>,
}

impl<'a, 'row> AvroDecode for OptionalRecordDecoder<'a, 'row> {
    type Out = bool;
    fn union_branch<'b, R: AvroRead, D: AvroDeserializer>(
        self,
        idx: usize,
        _n_variants: usize,
        null_variant: Option<usize>,
        deserializer: D,
        reader: &'b mut R,
    ) -> Result<Self::Out, AvroError> {
        if Some(idx) == null_variant {
            // we are done, the row is null!
            Ok(false)
        } else {
            let d = AvroFlatDecoder {
                packer: self.packer,
                buf: self.buf,
                is_top: false,
            };
            deserializer.deserialize(reader, d)?;
            Ok(true)
        }
    }
    define_unexpected! {
        record, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

pub(super) struct RowDecoder {
    state: (Rc<RefCell<Row>>, Rc<RefCell<Vec<u8>>>),
}

impl AvroDecode for RowDecoder {
    type Out = RowWrapper;
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        a: &mut A,
    ) -> Result<Self::Out, AvroError> {
        let mut row_borrow = self.state.0.borrow_mut();
        let mut buf_borrow = self.state.1.borrow_mut();
        let mut packer = row_borrow.packer();
        let inner = AvroFlatDecoder {
            packer: &mut packer,
            buf: &mut buf_borrow,
            is_top: true,
        };
        inner.record(a)?;
        Ok(RowWrapper(row_borrow.clone()))
    }
    define_unexpected! {
        union_branch, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

// Get around orphan rule
#[derive(Debug)]
pub(super) struct RowWrapper(pub Row);

impl StatefulAvroDecodable for RowWrapper {
    type Decoder = RowDecoder;
    // TODO - can we make this some sort of &'a mut (Row, Vec<u8>) without
    // running into lifetime crap?
    type State = (Rc<RefCell<Row>>, Rc<RefCell<Vec<u8>>>);

    fn new_decoder(state: Self::State) -> Self::Decoder {
        Self::Decoder { state }
    }
}

#[derive(Debug)]
pub struct AvroFlatDecoder<'a, 'row> {
    pub packer: &'a mut RowPacker<'row>,
    pub buf: &'a mut Vec<u8>,
    pub is_top: bool,
}

impl<'a, 'row> AvroDecode for AvroFlatDecoder<'a, 'row> {
    type Out = ();
    #[inline]
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        a: &mut A,
    ) -> Result<Self::Out, AvroError> {
        let mut str_buf = std::mem::take(self.buf);
        let mut pack_record = |rp: &mut RowPacker| -> Result<(), AvroError> {
            let mut expected = 0;
            let mut stash = vec![];
            // The idea here is that if the deserializer gives us fields in the order we're expecting,
            // we can decode them directly into the row.
            // If not, we need to decode them into a Value (the old, slow decoding path) and stash them,
            // so that we can put everything in the right order at the end.
            //
            // TODO(btv) - this is pretty bad, as a misordering at the top of the schema graph will
            // cause the _entire_ chunk under it to be decoded in the slow way!
            // Maybe instead, we should decode to separate sub-Rows and then add an API
            // to Row that just copies in the bytes from another one.
            while let Some((_name, idx, f)) = a.next_field()? {
                if idx == expected {
                    expected += 1;
                    f.decode_field(AvroFlatDecoder {
                        packer: rp,
                        buf: &mut str_buf,
                        is_top: false,
                    })?;
                } else {
                    let val = f.decode_field(ValueDecoder)?;
                    stash.push((idx, val));
                }
            }
            stash.sort_by_key(|(idx, _val)| *idx);
            for (idx, val) in stash {
                assert!(idx == expected);
                expected += 1;
                let dec = AvroFlatDecoder {
                    packer: rp,
                    buf: &mut str_buf,
                    is_top: false,
                };
                give_value(dec, &val)?;
            }
            Ok(())
        };
        if self.is_top {
            pack_record(self.packer)?;
        } else {
            self.packer.push_list_with(pack_record)?;
        }
        *self.buf = str_buf;
        Ok(())
    }
    #[inline]
    fn union_branch<'b, R: AvroRead, D: AvroDeserializer>(
        self,
        idx: usize,
        n_variants: usize,
        null_variant: Option<usize>,
        deserializer: D,
        reader: &'b mut R,
    ) -> Result<Self::Out, AvroError> {
        if null_variant == Some(idx) {
            for _ in 0..n_variants - 1 {
                self.packer.push(Datum::Null)
            }
        } else {
            let mut deserializer = Some(deserializer);
            for i in 0..n_variants {
                let dec = AvroFlatDecoder {
                    packer: self.packer,
                    buf: self.buf,
                    is_top: false,
                };
                if null_variant != Some(i) {
                    if i == idx {
                        deserializer.take().unwrap().deserialize(reader, dec)?;
                    } else {
                        self.packer.push(Datum::Null)
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn enum_variant(self, symbol: &str, _idx: usize) -> Result<Self::Out, AvroError> {
        self.packer.push(Datum::String(symbol));
        Ok(())
    }
    #[inline]
    fn scalar(self, scalar: mz_avro::types::Scalar) -> Result<Self::Out, AvroError> {
        match scalar {
            mz_avro::types::Scalar::Null => self.packer.push(Datum::Null),
            mz_avro::types::Scalar::Boolean(val) => {
                if val {
                    self.packer.push(Datum::True)
                } else {
                    self.packer.push(Datum::False)
                }
            }
            mz_avro::types::Scalar::Int(val) => self.packer.push(Datum::Int32(val)),
            mz_avro::types::Scalar::Long(val) => self.packer.push(Datum::Int64(val)),
            mz_avro::types::Scalar::Float(val) => {
                self.packer.push(Datum::Float32(OrderedFloat(val)))
            }
            mz_avro::types::Scalar::Double(val) => {
                self.packer.push(Datum::Float64(OrderedFloat(val)))
            }
            mz_avro::types::Scalar::Date(val) => self.packer.push(Datum::Date(val)),
            mz_avro::types::Scalar::Timestamp(val) => self.packer.push(Datum::Timestamp(val)),
        }
        Ok(())
    }

    #[inline]
    fn decimal<'b, R: AvroRead>(
        self,
        _precision: usize,
        scale: usize,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<Self::Out, AvroError> {
        let mut buf = match r {
            ValueOrReader::Value(val) => val.to_vec(),
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                let v = self.buf.clone();
                v
            }
        };

        let scale = u8::try_from(scale).map_err(|_| {
            DecodeError::Custom(format!(
                "Error decoding decimal: scale must fit within u8, but got scale {}",
                scale,
            ))
        })?;

        let n = numeric::twos_complement_be_to_numeric(&mut buf, scale)
            .map_err_to_string()
            .map_err(DecodeError::Custom)?;

        if n.is_special()
            || numeric::get_precision(&n) > u32::from(numeric::NUMERIC_DATUM_MAX_PRECISION)
        {
            return Err(AvroError::Decode(DecodeError::Custom(format!(
                "Error decoding numeric: exceeds maximum precision {}",
                numeric::NUMERIC_DATUM_MAX_PRECISION
            ))));
        }

        self.packer.push(Datum::from(n));

        Ok(())
    }

    #[inline]
    fn bytes<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<Self::Out, AvroError> {
        let buf = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                &self.buf
            }
        };
        self.packer.push(Datum::Bytes(buf));
        Ok(())
    }
    #[inline]
    fn string<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b str, R>,
    ) -> Result<Self::Out, AvroError> {
        let s = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                // TODO - this copy is unnecessary,
                // we should special case to just look at the bytes
                // directly when r is &[u8].
                // It probably doesn't make a huge difference though.
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                std::str::from_utf8(&self.buf).map_err(|_| DecodeError::StringUtf8Error)?
            }
        };
        self.packer.push(Datum::String(s));
        Ok(())
    }
    #[inline]
    fn json<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b serde_json::Value, R>,
    ) -> Result<Self::Out, AvroError> {
        match r {
            ValueOrReader::Value(val) => {
                JsonbPacker::new(self.packer)
                    .pack_serde_json(val.clone())
                    .map_err_to_string()
                    .map_err(DecodeError::Custom)?;
            }
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                JsonbPacker::new(self.packer)
                    .pack_slice(&self.buf)
                    .map_err_to_string()
                    .map_err(DecodeError::Custom)?;
            }
        }
        Ok(())
    }
    #[inline]
    fn uuid<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<Self::Out, AvroError> {
        let buf = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                &self.buf
            }
        };
        let s = std::str::from_utf8(&buf).map_err(|_e| DecodeError::UuidUtf8Error)?;
        self.packer.push(Datum::Uuid(
            Uuid::parse_str(s).map_err(DecodeError::BadUuid)?,
        ));
        Ok(())
    }
    #[inline]
    fn fixed<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<Self::Out, AvroError> {
        self.bytes(r)
    }
    #[inline]
    fn array<A: AvroArrayAccess>(mut self, a: &mut A) -> Result<Self::Out, AvroError> {
        self.is_top = false;
        let mut str_buf = std::mem::take(self.buf);
        self.packer.push_list_with(|rp| -> Result<(), AvroError> {
            loop {
                let next = AvroFlatDecoder {
                    packer: rp,
                    buf: &mut str_buf,
                    is_top: false,
                };
                if a.decode_next(next)?.is_none() {
                    break;
                }
            }
            Ok(())
        })?;
        *self.buf = str_buf;
        Ok(())
    }
    #[inline]
    fn map<A: AvroMapAccess>(self, a: &mut A) -> Result<Self::Out, AvroError> {
        // Map (key, value) pairs need to be unique and ordered.
        let mut map = BTreeMap::new();
        while let Some((name, f)) = a.next_entry()? {
            map.insert(name, f.decode_field(ValueDecoder)?);
        }
        self.packer
            .push_dict_with(|packer| -> Result<(), AvroError> {
                for (key, val) in map {
                    packer.push(Datum::String(key.as_str()));
                    give_value(
                        AvroFlatDecoder {
                            packer,
                            buf: &mut vec![],
                            is_top: false,
                        },
                        &val,
                    )?;
                }
                Ok(())
            })?;

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DiffPair<T> {
    pub before: Option<T>,
    pub after: Option<T>,
}
