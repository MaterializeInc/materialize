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

use std::cmp;
use std::collections::BTreeSet;
use std::fmt::{self, Display};
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};

use chrono::{DateTime, NaiveDate};
use flate2::read::MultiGzDecoder;

use crate::error::{DecodeError, Error as AvroError};
use crate::schema::{
    RecordField, ResolvedDefaultValueField, ResolvedRecordField, Schema, SchemaNode, SchemaPiece,
    SchemaPieceOrNamed, SchemaPieceRefOrNamed,
};
use crate::types::{Scalar, Value};
use crate::util::{TsUnit, safe_len, zag_i32, zag_i64};
use crate::{TrivialDecoder, ValueDecoder};

pub trait StatefulAvroDecodable: Sized {
    type Decoder: AvroDecode<Out = Self>;
    type State;
    fn new_decoder(state: Self::State) -> Self::Decoder;
}
pub trait AvroDecodable: Sized {
    type Decoder: AvroDecode<Out = Self>;

    fn new_decoder() -> Self::Decoder;
}
impl<T> AvroDecodable for T
where
    T: StatefulAvroDecodable,
    T::State: Default,
{
    type Decoder = <Self as StatefulAvroDecodable>::Decoder;

    fn new_decoder() -> Self::Decoder {
        <Self as StatefulAvroDecodable>::new_decoder(Default::default())
    }
}
#[inline]
fn decode_long_nonneg<R: Read>(reader: &mut R) -> Result<u64, AvroError> {
    let u = match zag_i64(reader)? {
        i if i >= 0 => i as u64,
        i => return Err(AvroError::Decode(DecodeError::ExpectedNonnegInteger(i))),
    };
    Ok(u)
}

fn decode_int_nonneg<R: Read>(reader: &mut R) -> Result<u32, AvroError> {
    let u = match zag_i32(reader)? {
        i if i >= 0 => i as u32,
        i => {
            return Err(AvroError::Decode(DecodeError::ExpectedNonnegInteger(
                i as i64,
            )));
        }
    };
    Ok(u)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> Result<usize, AvroError> {
    zag_i64(reader).and_then(|i| safe_len(i as usize))
}

#[inline]
fn decode_float<R: Read>(reader: &mut R) -> Result<f32, AvroError> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf[..])?;
    Ok(f32::from_le_bytes(buf))
}

#[inline]
fn decode_double<R: Read>(reader: &mut R) -> Result<f64, AvroError> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf[..])?;
    Ok(f64::from_le_bytes(buf))
}

impl Display for TsUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TsUnit::Millis => write!(f, "ms"),
            TsUnit::Micros => write!(f, "us"),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use crate::types::Value;
    use crate::util::TsUnit;

    use super::build_ts_value;

    #[mz_ore::test]
    fn test_negative_timestamps() {
        assert_eq!(
            build_ts_value(-1, TsUnit::Millis).unwrap(),
            Value::Timestamp(
                DateTime::from_timestamp(-1, 999_000_000)
                    .unwrap()
                    .naive_utc()
            )
        );
        assert_eq!(
            build_ts_value(-1000, TsUnit::Millis).unwrap(),
            Value::Timestamp(DateTime::from_timestamp(-1, 0).unwrap().naive_utc())
        );
        assert_eq!(
            build_ts_value(-1000, TsUnit::Micros).unwrap(),
            Value::Timestamp(
                DateTime::from_timestamp(-1, 999_000_000)
                    .unwrap()
                    .naive_utc()
            )
        );
        assert_eq!(
            build_ts_value(-1, TsUnit::Micros).unwrap(),
            Value::Timestamp(
                DateTime::from_timestamp(-1, 999_999_000)
                    .unwrap()
                    .naive_utc()
            )
        );
        assert_eq!(
            build_ts_value(-123_456_789_123, TsUnit::Micros).unwrap(),
            Value::Timestamp(
                DateTime::from_timestamp(-123_457, (1_000_000 - 789_123) * 1_000)
                    .unwrap()
                    .naive_utc()
            )
        );
    }

    #[mz_ore::test]
    fn array_block_len_bounded_by_remaining_input() {
        // A tiny body claiming a huge array block must error, not allocate. A
        // small (or hostile) message used to drive an unbounded `Vec<Value>` by
        // claiming a multi-million-element block whose items decode from ~no
        // input (e.g. an empty record). Regression for an OOM found by fuzzing.
        use std::str::FromStr;

        use super::{AvroDeserializer, GeneralDeserializer};
        use crate::util::zig_i64;
        use crate::{Schema, ValueDecoder};

        let schema = Schema::from_str(r#"{"type": "array", "items": "long"}"#).unwrap();
        let mut body = Vec::new();
        zig_i64(8_000_000, &mut body); // block count dwarfs the (here, empty) element data
        let dsr = GeneralDeserializer {
            schema: schema.top_node(),
        };
        let mut reader: &[u8] = &body;
        let res = dsr.deserialize(&mut reader, ValueDecoder);
        assert!(
            res.is_err(),
            "an array block longer than the remaining input must be rejected, not allocated"
        );
    }

    #[mz_ore::test]
    fn zero_width_array_elements_decode() {
        // The remaining-input bound must not reject valid arrays whose elements
        // encode to zero bytes. `null` and empty records have no per-element byte
        // floor, so a ten-element block legitimately follows its count with no
        // element bytes at all (Materialize's own writer emits `array<null>` of
        // ten as `[20, 0]`: block count 10, then the terminating zero block).
        use std::str::FromStr;

        use super::{AvroDeserializer, GeneralDeserializer};
        use crate::types::Value;
        use crate::util::zig_i64;
        use crate::{Schema, ValueDecoder};

        for (items, want) in [
            (r#""null""#, Value::Null),
            (
                r#"{"type": "record", "name": "Empty", "fields": []}"#,
                Value::Record(vec![]),
            ),
        ] {
            let schema =
                Schema::from_str(&format!(r#"{{"type": "array", "items": {items}}}"#)).unwrap();
            let mut body = Vec::new();
            zig_i64(10, &mut body); // ten elements...
            body.push(0); // ...then the terminating zero block. No element bytes.
            let dsr = GeneralDeserializer {
                schema: schema.top_node(),
            };
            let mut reader: &[u8] = &body;
            let decoded = dsr
                .deserialize(&mut reader, ValueDecoder)
                .expect("a zero-width array element type must decode, not be rejected");
            assert_eq!(decoded, Value::Array(vec![want; 10]));
        }
    }

    #[mz_ore::test]
    fn valid_null_array_falsely_rejected() {
        // Materialize's encoder emits no element bytes for `null` array
        // elements, so decoding must not assume each element consumes at least
        // one byte of remaining input.
        use std::str::FromStr;

        use super::{AvroDeserializer, GeneralDeserializer};
        use crate::encode::encode_to_vec;
        use crate::types::Value;
        use crate::{Schema, ValueDecoder};

        let schema = Schema::from_str(r#"{"type": "array", "items": "null"}"#).unwrap();
        let value = Value::Array(vec![Value::Null, Value::Null]);
        let body = encode_to_vec(&value, &schema);

        let dsr = GeneralDeserializer {
            schema: schema.top_node(),
        };
        let mut reader: &[u8] = &body;
        let res = dsr.deserialize(&mut reader, ValueDecoder);
        assert!(
            res.is_ok(),
            "an encoder-produced array of nulls should round-trip, but got: {res:?}"
        );
    }

    #[mz_ore::test]
    fn zero_width_array_elements_decode_across_blocks() {
        // Zero-width arrays can be encoded as multiple blocks. The cumulative
        // cap must not reject ordinary valid data below the limit.
        use std::str::FromStr;

        use super::{AvroDeserializer, GeneralDeserializer};
        use crate::types::Value;
        use crate::util::zig_i64;
        use crate::{Schema, ValueDecoder};

        let schema = Schema::from_str(r#"{"type": "array", "items": "null"}"#).unwrap();
        let mut body = Vec::new();
        zig_i64(4, &mut body);
        zig_i64(6, &mut body);
        body.push(0);

        let dsr = GeneralDeserializer {
            schema: schema.top_node(),
        };
        let mut reader: &[u8] = &body;
        let decoded = dsr
            .deserialize(&mut reader, ValueDecoder)
            .expect("zero-width arrays may span multiple blocks below the cap");
        assert_eq!(decoded, Value::Array(vec![Value::Null; 10]));
    }

    #[mz_ore::test]
    fn zero_width_array_total_len_bounded_across_blocks() {
        // A zero-width element type has no input-proportional per-block bound.
        // Keep a cumulative cap so repeated legal-size blocks cannot drive an
        // unbounded decode. Seed the shared node budget at the cap to test the
        // edge without walking millions of null elements first.
        use std::str::FromStr;

        use super::{AvroArrayAccess, DECODE_NODES, MAX_VALUE_NODES, SimpleArrayAccess};
        use crate::util::zig_i64;
        use crate::{Schema, TrivialDecoder};

        let schema = Schema::from_str(r#""null""#).unwrap();
        let mut body = Vec::new();
        zig_i64(1, &mut body);

        let mut reader: &[u8] = &body;
        let mut access = SimpleArrayAccess::new(&mut reader, schema.top_node());
        // Drive `SimpleArrayAccess` directly (no top-level decode entry to reset
        // the budget), so pre-charge the shared counter to the cap by hand.
        DECODE_NODES.with(|n| n.set(MAX_VALUE_NODES));

        let err = access
            .decode_next(TrivialDecoder)
            .expect_err("a new block past the cumulative node budget must be rejected");
        DECODE_NODES.with(|n| n.set(0));
        assert!(
            err.to_string().contains("exceeds cumulative limit"),
            "unexpected error: {err}"
        );
    }

    #[mz_ore::test]
    fn zero_width_record_array_bounded() {
        // Regression for an OOM found by the reader_decode fuzz target: an
        // `array<record{null}>` element is zero-width on the wire (the byte-floor
        // check below can't bound it) yet each element still allocates a
        // `Value::Record`, so a multi-million-element block claimed from a
        // handful of bytes amplified into gigabytes. The cumulative node cap must
        // reject it rather than allocate.
        use std::str::FromStr;

        use super::{AvroDeserializer, GeneralDeserializer};
        use crate::util::zig_i64;
        use crate::{Schema, ValueDecoder};

        let schema = Schema::from_str(
            r#"{"type": "array", "items":
                {"type": "record", "name": "R", "fields": [{"name": "g0", "type": "null"}]}}"#,
        )
        .unwrap();
        // A single block claiming far more zero-width records than the node cap,
        // followed by no element bytes at all.
        let mut body = Vec::new();
        zig_i64(100_000_000, &mut body);
        let dsr = GeneralDeserializer {
            schema: schema.top_node(),
        };
        let mut reader: &[u8] = &body;
        let res = dsr.deserialize(&mut reader, ValueDecoder);
        assert!(
            res.is_err(),
            "an array of zero-width records longer than the node cap must be rejected, not allocated"
        );
    }

    #[mz_ore::test]
    fn small_zero_width_record_array_decodes() {
        // The node cap must not reject an ordinary, below-cap array of zero-width
        // records: ten `record{null}`s encode (like `array<null>`) as just the
        // block count followed by the terminating zero block.
        use std::str::FromStr;

        use super::{AvroDeserializer, GeneralDeserializer};
        use crate::types::Value;
        use crate::util::zig_i64;
        use crate::{Schema, ValueDecoder};

        let schema = Schema::from_str(
            r#"{"type": "array", "items":
                {"type": "record", "name": "R", "fields": [{"name": "g0", "type": "null"}]}}"#,
        )
        .unwrap();
        let mut body = Vec::new();
        zig_i64(10, &mut body);
        body.push(0);
        let dsr = GeneralDeserializer {
            schema: schema.top_node(),
        };
        let mut reader: &[u8] = &body;
        let decoded = dsr
            .deserialize(&mut reader, ValueDecoder)
            .expect("a below-cap array of zero-width records must decode, not be rejected");
        let want = Value::Record(vec![("g0".to_string(), Value::Null)]);
        assert_eq!(decoded, Value::Array(vec![want; 10]));
    }

    #[mz_ore::test]
    fn nested_zero_width_collection_shares_node_budget() {
        // Regression: the node budget must be shared across every collection in
        // one datum, not reset per collection. With a per-collection budget each
        // inner array of `array<record{array<record{null}>}>` would get a fresh
        // `MAX_VALUE_NODES` ceiling, so a few wire bytes amplify into
        // ~`MAX_VALUE_NODES` *per outer element* (the same blow-up the cap exists
        // to stop, one nesting level deeper). Drive the decode so an inner
        // array's block-header charge — reached only after the enclosing record
        // starts decoding — trips the *shared* cumulative cap, proving the inner
        // collection sees the outer element's spend rather than a fresh budget.
        use std::str::FromStr;

        use super::{AvroDeserializer, GeneralDeserializer, MAX_VALUE_NODES};
        use crate::util::zig_i64;
        use crate::{Schema, ValueDecoder};

        let schema = Schema::from_str(
            r#"{"type": "array", "items":
                {"type": "record", "name": "Outer", "fields": [
                    {"name": "inner", "type":
                        {"type": "array", "items":
                            {"type": "record", "name": "Inner",
                             "fields": [{"name": "g0", "type": "null"}]}}}]}}"#,
        )
        .unwrap();
        // One outer element (charges 2 nodes), whose inner array then claims
        // `MAX_VALUE_NODES / 2` zero-width records — exactly `MAX_VALUE_NODES`
        // weighted nodes, which clears the inner block's own per-block check but
        // pushes the *shared* total (2 + MAX_VALUE_NODES) over the cap. No inner
        // element bytes follow: a correct decode rejects at the header before
        // allocating anything; the per-collection bug would instead materialize
        // ~2M `Value::Record`s and only later hit EOF.
        let mut body = Vec::new();
        zig_i64(1, &mut body);
        zig_i64((MAX_VALUE_NODES / 2) as i64, &mut body);
        let dsr = GeneralDeserializer {
            schema: schema.top_node(),
        };
        let mut reader: &[u8] = &body;
        let err = dsr.deserialize(&mut reader, ValueDecoder).expect_err(
            "a nested array claiming MAX_VALUE_NODES on top of the outer spend must be rejected",
        );
        assert!(
            err.to_string().contains("exceeds cumulative limit"),
            "unexpected error: {err}"
        );
    }

    #[mz_ore::test]
    fn top_level_decode_resets_stale_node_budget() {
        // A decode that errored partway can leave the thread-local node counter
        // non-zero; the next top-level decode must reset it (depth 0 -> 1) or an
        // unrelated datum on the same thread is wrongly rejected. Pin the counter
        // at the cap to stand in for that leftover, then require a small array to
        // still decode.
        use std::str::FromStr;

        use super::{AvroDeserializer, DECODE_NODES, GeneralDeserializer, MAX_VALUE_NODES};
        use crate::types::Value;
        use crate::util::zig_i64;
        use crate::{Schema, ValueDecoder};

        let schema = Schema::from_str(r#"{"type": "array", "items": "null"}"#).unwrap();
        let mut body = Vec::new();
        zig_i64(3, &mut body);
        body.push(0);

        DECODE_NODES.with(|n| n.set(MAX_VALUE_NODES));
        let dsr = GeneralDeserializer {
            schema: schema.top_node(),
        };
        let mut reader: &[u8] = &body;
        let decoded = dsr
            .deserialize(&mut reader, ValueDecoder)
            .expect("the top-level entry must reset a stale node budget");
        assert_eq!(decoded, Value::Array(vec![Value::Null; 3]));
    }
}

/// A convenience function to build timestamp values from underlying longs.
pub fn build_ts_value(value: i64, unit: TsUnit) -> Result<Value, AvroError> {
    let result = match unit {
        TsUnit::Millis => DateTime::from_timestamp_millis(value),
        TsUnit::Micros => DateTime::from_timestamp_micros(value),
    };
    let ndt = result.ok_or(AvroError::Decode(DecodeError::BadTimestamp { unit, value }))?;
    Ok(Value::Timestamp(ndt.naive_utc()))
}

/// A convenience trait for types that are both readable and skippable.
///
/// A blanket implementation is provided for all types that implement both
/// [`Read`] and [`Skip`].
pub trait AvroRead: Read + Skip {}

impl<T> AvroRead for T where T: Read + Skip {}

/// A trait that allows for efficient skipping forward while reading data.
pub trait Skip: Read {
    /// Advance the cursor by `len` bytes.
    ///
    /// If possible, the implementation should be more efficient than calling
    /// [`Read::read`] and discarding the resulting bytes.
    ///
    /// Calling `skip` with a `len` that advances the cursor past the end of the
    /// underlying data source is permissible. The only requirement is that the
    /// next call to [`Read::read`] indicates EOF.
    ///
    /// # Errors
    ///
    /// Can return an error in all the same cases that [`Read::read`] can.
    ///
    /// TODO: Remove this clippy suppression when the issue is fixed.
    /// See <https://github.com/rust-lang/rust-clippy/issues/12519>
    #[allow(clippy::unused_io_amount)]
    fn skip(&mut self, mut len: usize) -> Result<(), io::Error> {
        const BUF_SIZE: usize = 512;
        let mut buf = [0; BUF_SIZE];

        while len > 0 {
            let n = if len < BUF_SIZE {
                self.read(&mut buf[..len])?
            } else {
                self.read(&mut buf)?
            };
            if n == 0 {
                break;
            }
            len -= n;
        }
        Ok(())
    }

    /// An upper bound, if cheaply known, on the number of bytes still readable
    /// from this source. Used to reject an array/map block that claims more
    /// elements than the input could possibly contain: each element consumes at
    /// least zero bytes, so a block longer than the remaining input only happens
    /// when a small (or hostile) message claims a huge count, which would
    /// otherwise drive an unbounded `Vec` allocation (length amplification).
    /// Streaming sources that can't answer cheaply return `None`.
    fn remaining_input(&self) -> Option<usize> {
        None
    }
}

impl Skip for File {
    fn skip(&mut self, len: usize) -> Result<(), io::Error> {
        self.seek(SeekFrom::Current(len as i64))?;
        Ok(())
    }
}

impl Skip for &[u8] {
    fn skip(&mut self, len: usize) -> Result<(), io::Error> {
        let len = cmp::min(len, self.len());
        *self = &self[len..];
        Ok(())
    }

    fn remaining_input(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<S: Skip + ?Sized> Skip for Box<S> {
    fn skip(&mut self, len: usize) -> Result<(), io::Error> {
        self.as_mut().skip(len)
    }

    fn remaining_input(&self) -> Option<usize> {
        self.as_ref().remaining_input()
    }
}

impl<T: AsRef<[u8]>> Skip for Cursor<T> {
    fn skip(&mut self, len: usize) -> Result<(), io::Error> {
        self.seek(SeekFrom::Current(len as i64))?;
        Ok(())
    }

    fn remaining_input(&self) -> Option<usize> {
        let total = self.get_ref().as_ref().len();
        Some(total.saturating_sub(usize::try_from(self.position()).unwrap_or(usize::MAX)))
    }
}

impl<R: Read> Skip for MultiGzDecoder<R> {}

pub enum ValueOrReader<'a, V, R: AvroRead> {
    Value(V),
    Reader { len: usize, r: &'a mut R },
}

enum SchemaOrDefault<'b, R: AvroRead> {
    Schema(&'b mut R, SchemaNode<'b>),
    Default(&'b Value),
}
pub struct AvroFieldAccess<'b, R: AvroRead> {
    schema: SchemaOrDefault<'b, R>,
}

impl<'b, R: AvroRead> AvroFieldAccess<'b, R> {
    pub fn decode_field<D: AvroDecode>(self, d: D) -> Result<D::Out, AvroError> {
        match self.schema {
            SchemaOrDefault::Schema(r, schema) => {
                let des = GeneralDeserializer { schema };
                des.deserialize(r, d)
            }
            SchemaOrDefault::Default(value) => give_value(d, value),
        }
    }
}

pub trait AvroRecordAccess<R: AvroRead> {
    fn next_field<'b>(
        &'b mut self,
    ) -> Result<Option<(&'b str, usize, AvroFieldAccess<'b, R>)>, AvroError>;
}

struct SimpleRecordAccess<'a, R: AvroRead> {
    schema: SchemaNode<'a>,
    r: &'a mut R,
    fields: &'a [RecordField],
    i: usize,
}

impl<'a, R: AvroRead> SimpleRecordAccess<'a, R> {
    fn new(schema: SchemaNode<'a>, r: &'a mut R, fields: &'a [RecordField]) -> Self {
        Self {
            schema,
            r,
            fields,
            i: 0,
        }
    }
}

impl<'a, R: AvroRead> AvroRecordAccess<R> for SimpleRecordAccess<'a, R> {
    fn next_field<'b>(
        &'b mut self,
    ) -> Result<Option<(&'b str, usize, AvroFieldAccess<'b, R>)>, AvroError> {
        assert!(self.i <= self.fields.len());
        if self.i == self.fields.len() {
            Ok(None)
        } else {
            let f = &self.fields[self.i];
            self.i += 1;
            Ok(Some((
                f.name.as_str(),
                f.position,
                AvroFieldAccess {
                    schema: SchemaOrDefault::Schema(self.r, self.schema.step(&f.schema)),
                },
            )))
        }
    }
}

struct ValueRecordAccess<'a> {
    values: &'a [(String, Value)],
    i: usize,
}

impl<'a> ValueRecordAccess<'a> {
    fn new(values: &'a [(String, Value)]) -> Self {
        Self { values, i: 0 }
    }
}

impl<'a> AvroRecordAccess<&'a [u8]> for ValueRecordAccess<'a> {
    fn next_field<'b>(
        &'b mut self,
    ) -> Result<Option<(&'b str, usize, AvroFieldAccess<'b, &'a [u8]>)>, AvroError> {
        assert!(self.i <= self.values.len());
        if self.i == self.values.len() {
            Ok(None)
        } else {
            let (name, val) = &self.values[self.i];
            self.i += 1;
            Ok(Some((
                name.as_str(),
                self.i - 1,
                AvroFieldAccess {
                    schema: SchemaOrDefault::Default(val),
                },
            )))
        }
    }
}

struct ValueMapAccess<'a> {
    values: &'a [(String, Value)],
    i: usize,
}

impl<'a> ValueMapAccess<'a> {
    fn new(values: &'a [(String, Value)]) -> Self {
        Self { values, i: 0 }
    }
}

impl<'a> AvroMapAccess for ValueMapAccess<'a> {
    type R = &'a [u8];
    fn next_entry<'b>(
        &'b mut self,
    ) -> Result<Option<(String, AvroFieldAccess<'b, Self::R>)>, AvroError> {
        assert!(self.i <= self.values.len());
        if self.i == self.values.len() {
            Ok(None)
        } else {
            let (name, val) = &self.values[self.i];
            self.i += 1;
            Ok(Some((
                name.clone(),
                AvroFieldAccess {
                    schema: SchemaOrDefault::Default(val),
                },
            )))
        }
    }
}

struct ResolvedRecordAccess<'a, R: AvroRead> {
    defaults: &'a [ResolvedDefaultValueField],
    i_defaults: usize,
    fields: &'a [ResolvedRecordField],
    i_fields: usize,
    r: &'a mut R,
    schema: SchemaNode<'a>,
}

impl<'a, R: AvroRead> ResolvedRecordAccess<'a, R> {
    fn new(
        defaults: &'a [ResolvedDefaultValueField],
        fields: &'a [ResolvedRecordField],
        r: &'a mut R,
        schema: SchemaNode<'a>,
    ) -> Self {
        Self {
            defaults,
            i_defaults: 0,
            fields,
            i_fields: 0,
            r,
            schema,
        }
    }
}

impl<'a, R: AvroRead> AvroRecordAccess<R> for ResolvedRecordAccess<'a, R> {
    fn next_field<'b>(
        &'b mut self,
    ) -> Result<Option<(&'b str, usize, AvroFieldAccess<'b, R>)>, AvroError> {
        assert!(self.i_defaults <= self.defaults.len() && self.i_fields <= self.fields.len());
        if self.i_defaults < self.defaults.len() {
            let default = &self.defaults[self.i_defaults];
            self.i_defaults += 1;
            Ok(Some((
                default.name.as_str(),
                default.position,
                AvroFieldAccess {
                    schema: SchemaOrDefault::Default(&default.default),
                },
            )))
        } else {
            while self.i_fields < self.fields.len() {
                let field = &self.fields[self.i_fields];
                self.i_fields += 1;
                match field {
                    ResolvedRecordField::Absent(absent_schema) => {
                        // we don't care what's in the value, but we still need to read it in order to skip ahead the proper amount in the input.
                        let d = GeneralDeserializer {
                            schema: absent_schema.top_node(),
                        };
                        d.deserialize(self.r, TrivialDecoder)?;
                        continue;
                    }
                    ResolvedRecordField::Present(field) => {
                        return Ok(Some((
                            field.name.as_str(),
                            field.position,
                            AvroFieldAccess {
                                schema: SchemaOrDefault::Schema(
                                    self.r,
                                    self.schema.step(&field.schema),
                                ),
                            },
                        )));
                    }
                }
            }
            Ok(None)
        }
    }
}

pub trait AvroArrayAccess {
    fn decode_next<D: AvroDecode>(&mut self, d: D) -> Result<Option<D::Out>, AvroError>;
}

pub trait AvroMapAccess {
    type R: AvroRead;
    fn next_entry<'b>(
        &'b mut self,
    ) -> Result<Option<(String, AvroFieldAccess<'b, Self::R>)>, AvroError>;
}

pub struct SimpleMapAccess<'a, R: AvroRead> {
    entry_schema: SchemaNode<'a>,
    r: &'a mut R,
    done: bool,
    remaining: usize,
    /// Lower bound on the `Value` nodes a single entry materializes: the key
    /// `String` plus the value's [`min_value_nodes`]. Charged against the shared
    /// [`DECODE_NODES`] budget per block; see [`charge_value_nodes`].
    entry_nodes: usize,
}

impl<'a, R: AvroRead> SimpleMapAccess<'a, R> {
    fn new(entry_schema: SchemaNode<'a>, r: &'a mut R) -> Self {
        Self {
            entry_schema,
            r,
            done: false,
            remaining: 0,
            // One node for the key `String`, plus the value's own nodes.
            entry_nodes: 1usize.saturating_add(min_value_nodes(entry_schema)),
        }
    }
}

impl<'a, R: AvroRead> AvroMapAccess for SimpleMapAccess<'a, R> {
    type R = R;
    fn next_entry<'b>(&'b mut self) -> Result<Option<(String, AvroFieldAccess<'b, R>)>, AvroError> {
        if self.done {
            return Ok(None);
        }
        if self.remaining == 0 {
            // TODO -- we can use len_in_bytes to quickly skip non-demanded arrays
            let (len, _len_in_bytes) = match zag_i64(self.r)? {
                len if len > 0 => (len as usize, None),
                neglen if neglen < 0 => (neglen.unsigned_abs() as usize, Some(decode_len(self.r)?)),
                0 => {
                    self.done = true;
                    return Ok(None);
                }
                _ => unreachable!(),
            };
            // See `SimpleArrayAccess::decode_next` — same `MAX_VALUE_NODES`
            // memory bound applies, weighting the entry count by the per-entry
            // node lower bound so a block whose values are wide-but-zero-width
            // records can't amplify a few wire bytes into millions of `Value`s.
            let block_nodes = len.saturating_mul(self.entry_nodes);
            if block_nodes > MAX_VALUE_NODES {
                return Err(AvroError::Decode(DecodeError::Custom(format!(
                    "Avro map block length {len} exceeds limit {MAX_VALUE_NODES} decoded values"
                ))));
            }
            // Charge against the budget shared by every array/map in the datum,
            // so nested collections can't each get a fresh cap (see
            // `charge_value_nodes` / `MAX_VALUE_NODES`).
            charge_value_nodes("map", block_nodes)?;
            // A block can't hold more entries than there are bytes left to
            // decode them from; reject a count that claims otherwise rather than
            // letting it drive an unbounded allocation (see `Skip::remaining_input`).
            // Unlike an array item, every map entry encodes at least a one-byte
            // key-length varint, so each entry has a guaranteed one-byte floor and
            // a count above the remaining input is always bogus.
            if let Some(remaining) = self.r.remaining_input() {
                if len > remaining {
                    return Err(AvroError::Decode(DecodeError::Custom(format!(
                        "Avro map block length {len} exceeds remaining input ({remaining} bytes)"
                    ))));
                }
            }
            self.remaining = len;
        }
        assert!(self.remaining > 0);
        self.remaining -= 1;

        // TODO - We can try to avoid this allocation, but  nobody uses maps in Materialize
        // right now so it doesn't really matter.
        let key_len = decode_len(self.r)?;
        let mut key_buf = vec![];
        key_buf.resize_with(key_len, Default::default);
        self.r.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf)
            .map_err(|_e| AvroError::Decode(DecodeError::MapKeyUtf8Error))?;

        let a = AvroFieldAccess {
            schema: SchemaOrDefault::Schema(self.r, self.entry_schema),
        };
        Ok(Some((key, a)))
    }
}

struct SimpleArrayAccess<'a, R: AvroRead> {
    r: &'a mut R,
    schema: SchemaNode<'a>,
    remaining: usize,
    /// Lower bound on the `Value` nodes a single element materializes (see
    /// [`min_value_nodes`]). Charged against the shared [`DECODE_NODES`] budget
    /// per block; see [`charge_value_nodes`].
    element_nodes: usize,
    done: bool,
}

impl<'a, R: AvroRead> SimpleArrayAccess<'a, R> {
    fn new(r: &'a mut R, schema: SchemaNode<'a>) -> Self {
        Self {
            r,
            schema,
            remaining: 0,
            element_nodes: min_value_nodes(schema),
            done: false,
        }
    }
}

struct ValueArrayAccess<'a> {
    values: &'a [Value],
    i: usize,
}

impl<'a> ValueArrayAccess<'a> {
    fn new(values: &'a [Value]) -> Self {
        Self { values, i: 0 }
    }
}

impl<'a> AvroArrayAccess for ValueArrayAccess<'a> {
    fn decode_next<D: AvroDecode>(&mut self, d: D) -> Result<Option<D::Out>, AvroError> {
        assert!(self.i <= self.values.len());
        if self.i == self.values.len() {
            Ok(None)
        } else {
            let val = give_value(d, &self.values[self.i])?;
            self.i += 1;
            Ok(Some(val))
        }
    }
}

/// Sanity cap on the number of `Value` nodes one top-level decode may
/// materialize across *every* array and map in the datum. Arrays and maps apply
/// it per block (a fast reject for an absurd single-block count) and against the
/// shared cumulative budget threaded through the whole decode (see
/// [`charge_value_nodes`] / [`DECODE_NODES`]).
///
/// This bounds *memory*, not element count: each element is weighted by
/// [`min_value_nodes`], a lower bound on the `Value` nodes it decodes into. An
/// element-count cap alone is not enough, because a zero-width element — `null`,
/// or a record of only `null`/empty-record fields — occupies no input yet still
/// allocates a `Value` (a `Vec` slot, plus a record's own `Vec` and field-name
/// `String`s). The [`min_encoded_len`] byte-floor check below bounds a block by
/// the remaining input only when each element occupies at least one wire byte,
/// so a multi-million-element block of zero-width elements would otherwise
/// amplify a handful of bytes into gigabytes. Weighting the count and capping
/// the product bounds that amplification (as well as the analogous case of a
/// huge block of wide, positive-floor records read from a large input).
///
/// The budget is shared across the whole datum rather than reset per collection
/// so the bound *composes through nesting*: a per-collection budget would hand
/// every `array`/`map` a fresh ceiling, letting a schema like
/// `array<record{array<record{null}>}>` amplify a few wire bytes into roughly
/// this cap raised to the nesting depth. Sharing one budget keeps the worst case
/// flat regardless of nesting.
///
/// Without any cap, a malicious or corrupt file can claim up to `i64::MAX` items
/// and the generic array/map decode loop runs until it OOMs or hits `Vec`
/// capacity-overflow.
///
/// At `1 << 22` nodes the worst case (decoding zero-width records right up to the
/// cap) peaks around 750 MiB — including the transient doubling of the element
/// `Vec` mid-`push` — leaving comfortable headroom under the fuzzer's 2 GiB RSS
/// limit, while still admitting any realistically-sized array/map.
const MAX_VALUE_NODES: usize = 1 << 22;

/// A *lower* bound on the number of bytes any value of `schema` encodes to on
/// the wire.
///
/// Used to reject an array block whose claimed element count could not possibly
/// fit in the remaining input: a block of `len` elements occupies at least
/// `len * min_encoded_len` bytes. Only an under-estimate is ever safe here — an
/// over-estimate would reject valid data — so anything whose floor we can't
/// prove (schema-resolution pieces, named-type recursion cycles) contributes
/// `0`, which simply relaxes the bound.
///
/// Crucially this returns `0` for zero-width types — `null`, an empty record, a
/// record of only such fields — because those genuinely encode to no bytes.
/// Materialize's own writer emits a ten-element `array<null>` as `[20, 0]`, so a
/// blanket "count must not exceed remaining bytes" rule would reject valid
/// input. For zero-width element types the caller falls back to the cumulative
/// [`MAX_VALUE_NODES`] cap (weighted by [`min_value_nodes`]).
fn min_encoded_len(schema: SchemaNode) -> usize {
    let mut visited = BTreeSet::new();
    min_encoded_len_piece(schema.root, schema.inner, &mut visited)
}

/// Resolves a (possibly named) schema reference, guarding against named-type
/// cycles, then defers to [`min_encoded_len_piece`].
fn min_encoded_len_or_named(
    root: &Schema,
    node: SchemaPieceRefOrNamed,
    visited: &mut BTreeSet<usize>,
) -> usize {
    match node {
        SchemaPieceRefOrNamed::Piece(piece) => min_encoded_len_piece(root, piece, visited),
        SchemaPieceRefOrNamed::Named(idx) => {
            // A named-type cycle can only close through a record field; treat
            // the back-edge as zero-width so we never over-estimate.
            if !visited.insert(idx) {
                return 0;
            }
            let len = min_encoded_len_piece(root, &root.lookup(idx).piece, visited);
            visited.remove(&idx);
            len
        }
    }
}

fn min_encoded_len_piece(
    root: &Schema,
    piece: &SchemaPiece,
    visited: &mut BTreeSet<usize>,
) -> usize {
    match piece {
        // Encodes to nothing at all.
        SchemaPiece::Null => 0,
        // A single byte (zig-zag varint of 0 is one byte; a bool is one byte).
        SchemaPiece::Boolean
        | SchemaPiece::Int
        | SchemaPiece::Long
        | SchemaPiece::Date
        | SchemaPiece::TimestampMilli
        | SchemaPiece::TimestampMicro => 1,
        SchemaPiece::Float => 4,
        SchemaPiece::Double => 8,
        // `fixed`-backed decimals are exactly their size; `bytes`-backed ones,
        // like `bytes`/`string`, carry at least a one-byte length varint.
        SchemaPiece::Decimal {
            fixed_size: Some(size),
            ..
        } => *size,
        SchemaPiece::Decimal {
            fixed_size: None, ..
        }
        | SchemaPiece::Bytes
        | SchemaPiece::String
        | SchemaPiece::Json
        | SchemaPiece::Uuid => 1,
        // An empty array/map encodes as a single zero-count byte regardless of
        // the element type, so don't recurse into it.
        SchemaPiece::Array(_) | SchemaPiece::Map(_) => 1,
        // A union always writes at least its one-byte branch index.
        SchemaPiece::Union(_) => 1,
        // An enum writes a one-byte symbol index.
        SchemaPiece::Enum { .. } => 1,
        SchemaPiece::Fixed { size } => *size,
        // A record's encoding is its fields' encodings concatenated, so its
        // floor is the sum of the fields' floors — which can be `0` (the empty
        // record, or a record of only `null`/empty-record fields).
        SchemaPiece::Record { fields, .. } => fields.iter().fold(0, |acc, field| {
            acc.saturating_add(min_encoded_len_or_named(
                root,
                field.schema.as_ref(),
                visited,
            ))
        }),
        // Schema-resolution pieces only arise on the reader/writer-mismatch
        // path; we don't try to prove a floor for them.
        _ => 0,
    }
}

/// A *lower* bound on the number of `Value` nodes a single value of `schema`
/// materializes into when decoded.
///
/// Used to weight an array/map element so the cumulative [`MAX_VALUE_NODES`] cap
/// bounds decoded *memory*, not just element count. The amplifying case the cap
/// exists for — `null` and records of only zero-width fields — is counted
/// *exactly* here (a record always materializes every field, and none of these
/// types involve a union/array/map whose runtime size we couldn't predict), so
/// the bound is tight where it matters most.
///
/// As with [`min_encoded_len`], only an under-estimate is ever safe (an
/// over-estimate would reject valid data), so a nested array/map contributes
/// `1` — its empty-collection floor — and its actual contents are charged
/// against the shared [`MAX_VALUE_NODES`] budget as they are decoded (so the
/// cap still composes through nesting); a union contributes `1` (its count is
/// already bounded by the remaining input via its one-byte branch floor); and
/// unprovable schema-resolution pieces contribute `1`. Every value is at least
/// one node, so the weight is always `>= 1`.
fn min_value_nodes(schema: SchemaNode) -> usize {
    let mut visited = BTreeSet::new();
    min_value_nodes_piece(schema.root, schema.inner, &mut visited)
}

/// Resolves a (possibly named) schema reference, guarding against named-type
/// cycles, then defers to [`min_value_nodes_piece`].
fn min_value_nodes_or_named(
    root: &Schema,
    node: SchemaPieceRefOrNamed,
    visited: &mut BTreeSet<usize>,
) -> usize {
    match node {
        SchemaPieceRefOrNamed::Piece(piece) => min_value_nodes_piece(root, piece, visited),
        SchemaPieceRefOrNamed::Named(idx) => {
            // A named-type cycle can only close through a record field; treat the
            // back-edge as a single node so we never over-estimate (and never
            // recurse forever).
            if !visited.insert(idx) {
                return 1;
            }
            let nodes = min_value_nodes_piece(root, &root.lookup(idx).piece, visited);
            visited.remove(&idx);
            nodes
        }
    }
}

fn min_value_nodes_piece(
    root: &Schema,
    piece: &SchemaPiece,
    visited: &mut BTreeSet<usize>,
) -> usize {
    match piece {
        // A record materializes itself plus every one of its fields. This is the
        // only type that can be zero-width on the wire yet still allocate, so
        // counting its fields exactly is what makes the cap effective.
        SchemaPiece::Record { fields, .. } => fields.iter().fold(1, |acc, field| {
            acc.saturating_add(min_value_nodes_or_named(
                root,
                field.schema.as_ref(),
                visited,
            ))
        }),
        // Every other type materializes a single node for the purposes of this
        // lower bound: scalars and leaves trivially; an array/map at minimum an
        // empty collection (its contents bounded by its own cumulative cap); a
        // union its (input-bounded) branch index; and resolution pieces we don't
        // try to prove.
        _ => 1,
    }
}

impl<'a, R: AvroRead> AvroArrayAccess for SimpleArrayAccess<'a, R> {
    fn decode_next<D: AvroDecode>(&mut self, d: D) -> Result<Option<D::Out>, AvroError> {
        if self.done {
            return Ok(None);
        }
        if self.remaining == 0 {
            // TODO -- we can use len_in_bytes to quickly skip non-demanded arrays
            let (len, _len_in_bytes) = match zag_i64(self.r)? {
                len if len > 0 => (len as usize, None),
                neglen if neglen < 0 => (neglen.unsigned_abs() as usize, Some(decode_len(self.r)?)),
                0 => {
                    self.done = true;
                    return Ok(None);
                }
                _ => unreachable!(),
            };
            // Weight the count by the per-element node lower bound so the cap
            // bounds decoded memory, not just element count: a block of
            // zero-width-but-allocating elements (e.g. a record of `null`s)
            // amplifies a few wire bytes into millions of `Value`s otherwise.
            let block_nodes = len.saturating_mul(self.element_nodes);
            if block_nodes > MAX_VALUE_NODES {
                return Err(AvroError::Decode(DecodeError::Custom(format!(
                    "Avro array block length {len} exceeds limit {MAX_VALUE_NODES} \
                     decoded values"
                ))));
            }
            // Charge against the budget shared by every array/map in the datum,
            // so nested collections can't each get a fresh cap (see
            // `charge_value_nodes` / `MAX_VALUE_NODES`).
            charge_value_nodes("array", block_nodes)?;
            // A block of `len` items occupies at least `len * min_elem` bytes,
            // so a count needing more than the remaining input can't be honest;
            // reject it rather than let it drive an unbounded allocation (see
            // `Skip::remaining_input`). Unlike a map entry — which always carries
            // at least a one-byte key-length varint — an array item can encode to
            // zero bytes (`null`, an empty record), so this bound only applies
            // when the element type has a proven positive byte floor. For
            // zero-width element types (`min_elem == 0`) we rely on the
            // cumulative `MAX_VALUE_NODES` cap; otherwise a valid datum such as a
            // ten-element `array<null>` (encoded as `[20, 0]`) would be wrongly
            // rejected.
            if let Some(remaining) = self.r.remaining_input() {
                let min_elem = min_encoded_len(self.schema);
                if min_elem > 0 && len.saturating_mul(min_elem) > remaining {
                    return Err(AvroError::Decode(DecodeError::Custom(format!(
                        "Avro array block length {len} exceeds remaining input ({remaining} bytes)"
                    ))));
                }
            }
            self.remaining = len;
        }
        assert!(self.remaining > 0);
        self.remaining -= 1;
        let des = GeneralDeserializer {
            schema: self.schema,
        };
        des.deserialize(self.r, d).map(Some)
    }
}

#[macro_export]
macro_rules! define_unexpected {
    (record) => {
        fn record<R: $crate::AvroRead, A: $crate::AvroRecordAccess<R>>(
            self,
            _a: &mut A,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedRecord))
        }
    };
    (union_branch) => {
        fn union_branch<'avro_macro_lifetime, R: $crate::AvroRead, D: $crate::AvroDeserializer>(
            self,
            _idx: usize,
            _n_variants: usize,
            _null_variant: Option<usize>,
            _deserializer: D,
            _reader: &'avro_macro_lifetime mut R,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedUnion))
        }
    };
    (array) => {
        fn array<A: $crate::AvroArrayAccess>(
            self,
            _a: &mut A,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode(
                $crate::error::DecodeError::UnexpectedArray,
            ))
        }
    };
    (map) => {
        fn map<M: $crate::AvroMapAccess>(
            self,
            _m: &mut M,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode(
                $crate::error::DecodeError::UnexpectedMap,
            ))
        }
    };
    (enum_variant) => {
        fn enum_variant(
            self,
            _symbol: &str,
            _idx: usize,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode(
                $crate::error::DecodeError::UnexpectedEnum,
            ))
        }
    };
    (scalar) => {
        fn scalar(self, _scalar: $crate::types::Scalar) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedScalar))
        }
    };
    (decimal) => {
        fn decimal<'avro_macro_lifetime, R: AvroRead>(
            self,
            _precision: usize,
            _scale: usize,
            _r: $crate::ValueOrReader<'avro_macro_lifetime, &'avro_macro_lifetime [u8], R>,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedDecimal))
        }
    };
    (bytes) => {
        fn bytes<'avro_macro_lifetime, R: AvroRead>(
            self,
            _r: $crate::ValueOrReader<'avro_macro_lifetime, &'avro_macro_lifetime [u8], R>,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedBytes))
        }
    };
    (string) => {
        fn string<'avro_macro_lifetime, R: AvroRead>(
            self,
            _r: $crate::ValueOrReader<'avro_macro_lifetime, &'avro_macro_lifetime str, R>,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedString))
        }
    };
    (json) => {
        fn json<'avro_macro_lifetime, R: AvroRead>(
            self,
            _r: $crate::ValueOrReader<
                'avro_macro_lifetime,
                &'avro_macro_lifetime serde_json::Value,
                R,
            >,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedJson))
        }
    };
    (uuid) => {
        fn uuid<'avro_macro_lifetime, R: AvroRead>(
            self,
            _r: $crate::ValueOrReader<'avro_macro_lifetime, &'avro_macro_lifetime [u8], R>,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedUuid))
        }
    };
    (fixed) => {
        fn fixed<'avro_macro_lifetime, R: AvroRead>(
            self,
            _r: $crate::ValueOrReader<'avro_macro_lifetime, &'avro_macro_lifetime [u8], R>,
        ) -> Result<Self::Out, $crate::error::Error> {
            Err($crate::error::Error::Decode($crate::error::DecodeError::UnexpectedFixed))
        }
    };
    ($($kind:ident),+) => {
        $($crate::define_unexpected!{$kind})+
    }
}

pub trait AvroDecode: Sized {
    type Out;
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        _a: &mut A,
    ) -> Result<Self::Out, AvroError>;

    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        self,
        _idx: usize,
        _n_variants: usize,
        _null_variant: Option<usize>,
        _deserializer: D,
        _reader: &'a mut R,
    ) -> Result<Self::Out, AvroError>;

    fn array<A: AvroArrayAccess>(self, _a: &mut A) -> Result<Self::Out, AvroError>;

    fn map<M: AvroMapAccess>(self, _m: &mut M) -> Result<Self::Out, AvroError>;

    fn enum_variant(self, _symbol: &str, _idx: usize) -> Result<Self::Out, AvroError>;

    fn scalar(self, _scalar: Scalar) -> Result<Self::Out, AvroError>;

    fn decimal<'a, R: AvroRead>(
        self,
        _precision: usize,
        _scale: usize,
        _r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<Self::Out, AvroError>;

    fn bytes<'a, R: AvroRead>(
        self,
        _r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<Self::Out, AvroError>;
    fn string<'a, R: AvroRead>(
        self,
        _r: ValueOrReader<'a, &'a str, R>,
    ) -> Result<Self::Out, AvroError>;
    fn json<'a, R: AvroRead>(
        self,
        _r: ValueOrReader<'a, &'a serde_json::Value, R>,
    ) -> Result<Self::Out, AvroError>;
    fn uuid<'a, R: AvroRead>(
        self,
        _r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<Self::Out, AvroError>;
    fn fixed<'a, R: AvroRead>(
        self,
        _r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<Self::Out, AvroError>;
    fn map_decoder<T, F: FnMut(Self::Out) -> Result<T, AvroError>>(
        self,
        f: F,
    ) -> public_decoders::MappingDecoder<T, Self::Out, Self, F> {
        public_decoders::MappingDecoder::new(self, f)
    }
}

pub mod public_decoders {

    use std::collections::BTreeMap;

    use crate::error::{DecodeError, Error as AvroError};
    use crate::types::{DecimalValue, Scalar, Value};
    use crate::{
        AvroArrayAccess, AvroDecode, AvroDeserializer, AvroRead, AvroRecordAccess, ValueOrReader,
    };

    use super::{AvroDecodable, AvroMapAccess, StatefulAvroDecodable};

    macro_rules! define_simple_decoder {
        ($name:ident, $out:ty, $($scalar_branch:ident);*) => {
            pub struct $name;
            impl AvroDecode for $name {
                type Out = $out;
                fn scalar(self, scalar: Scalar) -> Result<$out, AvroError> {
                    let out = match scalar {
                        $(
                            Scalar::$scalar_branch(inner) => {inner.try_into()?}
                        ),*
                            other => return Err(AvroError::Decode(
                                DecodeError::UnexpectedScalarKind(other.into()),
                            ))
                    };
                    Ok(out)
                }
                define_unexpected! {
                    array, record, union_branch, map,
                    enum_variant, decimal, bytes, string,
                    json, uuid, fixed
                }
            }

            impl StatefulAvroDecodable for $out {
                type Decoder = $name;
                type State = ();
                fn new_decoder(_state: ()) -> $name {
                    $name
                }
            }
        }
    }

    define_simple_decoder!(I32Decoder, i32, Int;Long);
    define_simple_decoder!(I64Decoder, i64, Int;Long);
    define_simple_decoder!(U64Decoder, u64, Int;Long);
    define_simple_decoder!(UsizeDecoder, usize, Int;Long);
    define_simple_decoder!(IsizeDecoder, isize, Int;Long);

    pub struct MappingDecoder<
        T,
        InnerOut,
        Inner: AvroDecode<Out = InnerOut>,
        Conv: FnMut(InnerOut) -> Result<T, AvroError>,
    > {
        inner: Inner,
        conv: Conv,
    }

    impl<
        T,
        InnerOut,
        Inner: AvroDecode<Out = InnerOut>,
        Conv: FnMut(InnerOut) -> Result<T, AvroError>,
    > MappingDecoder<T, InnerOut, Inner, Conv>
    {
        pub fn new(inner: Inner, conv: Conv) -> Self {
            Self { inner, conv }
        }
    }

    impl<
        T,
        InnerOut,
        Inner: AvroDecode<Out = InnerOut>,
        Conv: FnMut(InnerOut) -> Result<T, AvroError>,
    > AvroDecode for MappingDecoder<T, InnerOut, Inner, Conv>
    {
        type Out = T;

        fn record<R: AvroRead, A: AvroRecordAccess<R>>(
            mut self,
            a: &mut A,
        ) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.record(a)?)
        }

        fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
            mut self,
            idx: usize,
            n_variants: usize,
            null_variant: Option<usize>,
            deserializer: D,
            reader: &'a mut R,
        ) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.union_branch(
                idx,
                n_variants,
                null_variant,
                deserializer,
                reader,
            )?)
        }

        fn array<A: AvroArrayAccess>(mut self, a: &mut A) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.array(a)?)
        }

        fn map<M: AvroMapAccess>(mut self, m: &mut M) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.map(m)?)
        }

        fn enum_variant(mut self, symbol: &str, idx: usize) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.enum_variant(symbol, idx)?)
        }

        fn scalar(mut self, scalar: Scalar) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.scalar(scalar)?)
        }

        fn decimal<'a, R: AvroRead>(
            mut self,
            precision: usize,
            scale: usize,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.decimal(precision, scale, r)?)
        }

        fn bytes<'a, R: AvroRead>(
            mut self,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.bytes(r)?)
        }

        fn string<'a, R: AvroRead>(
            mut self,
            r: ValueOrReader<'a, &'a str, R>,
        ) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.string(r)?)
        }

        fn json<'a, R: AvroRead>(
            mut self,
            r: ValueOrReader<'a, &'a serde_json::Value, R>,
        ) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.json(r)?)
        }

        fn uuid<'a, R: AvroRead>(
            mut self,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.uuid(r)?)
        }

        fn fixed<'a, R: AvroRead>(
            mut self,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<Self::Out, AvroError> {
            (self.conv)(self.inner.fixed(r)?)
        }
    }
    pub struct ArrayAsVecDecoder<
        InnerOut,
        Inner: AvroDecode<Out = InnerOut>,
        Ctor: FnMut() -> Inner,
    > {
        ctor: Ctor,
        buf: Vec<InnerOut>,
    }

    impl<InnerOut, Inner: AvroDecode<Out = InnerOut>, Ctor: FnMut() -> Inner>
        ArrayAsVecDecoder<InnerOut, Inner, Ctor>
    {
        pub fn new(ctor: Ctor) -> Self {
            Self { ctor, buf: vec![] }
        }
    }
    impl<InnerOut, Inner: AvroDecode<Out = InnerOut>, Ctor: FnMut() -> Inner> AvroDecode
        for ArrayAsVecDecoder<InnerOut, Inner, Ctor>
    {
        type Out = Vec<InnerOut>;
        fn array<A: AvroArrayAccess>(mut self, a: &mut A) -> Result<Self::Out, AvroError> {
            while let Some(next) = a.decode_next((self.ctor)())? {
                self.buf.push(next);
            }
            Ok(self.buf)
        }
        define_unexpected! {
            record, union_branch, map, enum_variant,
            scalar, decimal, bytes, string, json, uuid,
            fixed
        }
    }

    pub struct DefaultArrayAsVecDecoder<T> {
        buf: Vec<T>,
    }
    impl<T> Default for DefaultArrayAsVecDecoder<T> {
        fn default() -> Self {
            Self { buf: vec![] }
        }
    }
    impl<T: AvroDecodable> AvroDecode for DefaultArrayAsVecDecoder<T> {
        type Out = Vec<T>;
        fn array<A: AvroArrayAccess>(mut self, a: &mut A) -> Result<Self::Out, AvroError> {
            while let Some(next) = {
                let inner = T::new_decoder();
                a.decode_next(inner)?
            } {
                self.buf.push(next);
            }
            Ok(self.buf)
        }
        define_unexpected! {
            record, union_branch, map, enum_variant,
            scalar, decimal, bytes, string, json, uuid,
            fixed
        }
    }
    impl<T: AvroDecodable> StatefulAvroDecodable for Vec<T> {
        type Decoder = DefaultArrayAsVecDecoder<T>;
        type State = ();

        fn new_decoder(_state: Self::State) -> Self::Decoder {
            DefaultArrayAsVecDecoder::<T>::default()
        }
    }
    pub struct TrivialDecoder;

    impl TrivialDecoder {
        fn maybe_skip<'a, V, R: AvroRead>(
            self,
            r: ValueOrReader<'a, V, R>,
        ) -> Result<(), AvroError> {
            if let ValueOrReader::Reader { len, r } = r {
                Ok(r.skip(len)?)
            } else {
                Ok(())
            }
        }
    }

    impl AvroDecode for TrivialDecoder {
        type Out = ();
        fn record<R: AvroRead, A: AvroRecordAccess<R>>(self, a: &mut A) -> Result<(), AvroError> {
            while let Some((_, _, f)) = a.next_field()? {
                f.decode_field(TrivialDecoder)?;
            }
            Ok(())
        }
        fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
            self,
            _idx: usize,
            _n_variants: usize,
            _null_variant: Option<usize>,
            deserializer: D,
            reader: &'a mut R,
        ) -> Result<(), AvroError> {
            deserializer.deserialize(reader, self)
        }

        fn enum_variant(self, _symbol: &str, _idx: usize) -> Result<(), AvroError> {
            Ok(())
        }
        fn scalar(self, _scalar: Scalar) -> Result<(), AvroError> {
            Ok(())
        }
        fn decimal<'a, R: AvroRead>(
            self,
            _precision: usize,
            _scale: usize,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<(), AvroError> {
            self.maybe_skip(r)
        }
        fn bytes<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<(), AvroError> {
            self.maybe_skip(r)
        }
        fn string<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a str, R>,
        ) -> Result<(), AvroError> {
            self.maybe_skip(r)
        }
        fn json<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a serde_json::Value, R>,
        ) -> Result<(), AvroError> {
            self.maybe_skip(r)
        }
        fn uuid<'a, R: AvroRead>(self, r: ValueOrReader<'a, &'a [u8], R>) -> Result<(), AvroError> {
            self.maybe_skip(r)
        }
        fn fixed<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<(), AvroError> {
            self.maybe_skip(r)
        }
        fn array<A: AvroArrayAccess>(self, a: &mut A) -> Result<(), AvroError> {
            while a.decode_next(TrivialDecoder)?.is_some() {}
            Ok(())
        }

        fn map<M: AvroMapAccess>(self, m: &mut M) -> Result<(), AvroError> {
            while let Some((_n, entry)) = m.next_entry()? {
                entry.decode_field(TrivialDecoder)?
            }
            Ok(())
        }
    }
    pub struct ValueDecoder;
    impl AvroDecode for ValueDecoder {
        type Out = Value;
        fn record<R: AvroRead, A: AvroRecordAccess<R>>(
            self,
            a: &mut A,
        ) -> Result<Value, AvroError> {
            let mut fields = vec![];
            while let Some((name, idx, f)) = a.next_field()? {
                let next = ValueDecoder;
                let val = f.decode_field(next)?;
                fields.push((idx, (name.to_string(), val)));
            }
            fields.sort_by_key(|(idx, _)| *idx);

            Ok(Value::Record(
                fields
                    .into_iter()
                    .map(|(_idx, (name, val))| (name, val))
                    .collect(),
            ))
        }
        fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
            self,
            index: usize,
            n_variants: usize,
            null_variant: Option<usize>,
            deserializer: D,
            reader: &'a mut R,
        ) -> Result<Value, AvroError> {
            let next = ValueDecoder;
            let inner = Box::new(deserializer.deserialize(reader, next)?);
            Ok(Value::Union {
                index,
                inner,
                n_variants,
                null_variant,
            })
        }
        fn array<A: AvroArrayAccess>(self, a: &mut A) -> Result<Value, AvroError> {
            let mut items = vec![];
            loop {
                let next = ValueDecoder;

                if let Some(value) = a.decode_next(next)? {
                    items.push(value)
                } else {
                    break;
                }
            }
            Ok(Value::Array(items))
        }
        fn enum_variant(self, symbol: &str, idx: usize) -> Result<Value, AvroError> {
            Ok(Value::Enum(idx, symbol.to_string()))
        }
        fn scalar(self, scalar: Scalar) -> Result<Value, AvroError> {
            Ok(scalar.into())
        }
        fn decimal<'a, R: AvroRead>(
            self,
            precision: usize,
            scale: usize,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<Value, AvroError> {
            let unscaled = match r {
                ValueOrReader::Value(buf) => buf.to_vec(),
                ValueOrReader::Reader { len, r } => {
                    let mut buf = vec![];
                    buf.resize_with(len, Default::default);
                    r.read_exact(&mut buf)?;
                    buf
                }
            };
            Ok(Value::Decimal(DecimalValue {
                unscaled,
                precision,
                scale,
            }))
        }
        fn bytes<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<Value, AvroError> {
            let buf = match r {
                ValueOrReader::Value(buf) => buf.to_vec(),
                ValueOrReader::Reader { len, r } => {
                    let mut buf = vec![];
                    buf.resize_with(len, Default::default);
                    r.read_exact(&mut buf)?;
                    buf
                }
            };
            Ok(Value::Bytes(buf))
        }
        fn string<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a str, R>,
        ) -> Result<Value, AvroError> {
            let s = match r {
                ValueOrReader::Value(s) => s.to_string(),
                ValueOrReader::Reader { len, r } => {
                    let mut buf = vec![];
                    buf.resize_with(len, Default::default);
                    r.read_exact(&mut buf)?;
                    String::from_utf8(buf)
                        .map_err(|_e| AvroError::Decode(DecodeError::StringUtf8Error))?
                }
            };
            Ok(Value::String(s))
        }
        fn json<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a serde_json::Value, R>,
        ) -> Result<Value, AvroError> {
            let val = match r {
                ValueOrReader::Value(val) => val.clone(),
                ValueOrReader::Reader { len, r } => {
                    let mut buf = vec![];
                    buf.resize_with(len, Default::default);
                    r.read_exact(&mut buf)?;
                    serde_json::from_slice(&buf).map_err(|e| {
                        AvroError::Decode(DecodeError::BadJson {
                            category: e.classify(),
                            bytes: buf.to_owned(),
                        })
                    })?
                }
            };
            Ok(Value::Json(val))
        }
        fn uuid<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<Value, AvroError> {
            let buf = match r {
                ValueOrReader::Value(val) => val.to_vec(),
                ValueOrReader::Reader { len, r } => {
                    let mut buf = vec![];
                    buf.resize_with(len, Default::default);
                    r.read_exact(&mut buf)?;
                    buf
                }
            };
            let s = std::str::from_utf8(&buf)
                .map_err(|_| AvroError::Decode(DecodeError::UuidUtf8Error))?;
            let val =
                uuid::Uuid::parse_str(s).map_err(|e| AvroError::Decode(DecodeError::BadUuid(e)))?;
            Ok(Value::Uuid(val))
        }
        fn fixed<'a, R: AvroRead>(
            self,
            r: ValueOrReader<'a, &'a [u8], R>,
        ) -> Result<Value, AvroError> {
            let buf = match r {
                ValueOrReader::Value(buf) => buf.to_vec(),
                ValueOrReader::Reader { len, r } => {
                    let mut buf = vec![];
                    buf.resize_with(len, Default::default);
                    r.read_exact(&mut buf)?;
                    buf
                }
            };
            Ok(Value::Fixed(buf.len(), buf))
        }
        fn map<M: AvroMapAccess>(self, m: &mut M) -> Result<Value, AvroError> {
            let mut entries = BTreeMap::new();
            while let Some((name, a)) = m.next_entry()? {
                let d = ValueDecoder;
                let val = a.decode_field(d)?;
                entries.insert(name, val);
            }
            Ok(Value::Map(entries))
        }
    }
}

impl<'a> AvroDeserializer for &'a Value {
    fn deserialize<R: AvroRead, D: AvroDecode>(
        self,
        _r: &mut R,
        d: D,
    ) -> Result<D::Out, AvroError> {
        give_value(d, self)
    }
}

pub fn give_value<D: AvroDecode>(d: D, v: &Value) -> Result<D::Out, AvroError> {
    use ValueOrReader::Value as V;
    match v {
        Value::Null => d.scalar(Scalar::Null),
        Value::Boolean(val) => d.scalar(Scalar::Boolean(*val)),
        Value::Int(val) => d.scalar(Scalar::Int(*val)),
        Value::Long(val) => d.scalar(Scalar::Long(*val)),
        Value::Float(val) => d.scalar(Scalar::Float(*val)),
        Value::Double(val) => d.scalar(Scalar::Double(*val)),
        Value::Date(val) => d.scalar(Scalar::Date(*val)),
        Value::Timestamp(val) => d.scalar(Scalar::Timestamp(*val)),
        // The &[u8] parameter here (and elsewhere in this function) is arbitrary, but we have to put in something in order for the function
        // to type-check
        Value::Decimal(val) => d.decimal::<&[u8]>(val.precision, val.scale, V(&val.unscaled)),
        Value::Bytes(val) => d.bytes::<&[u8]>(V(val)),
        Value::String(val) => d.string::<&[u8]>(V(val)),
        Value::Fixed(_len, val) => d.fixed::<&[u8]>(V(val)),
        Value::Enum(idx, symbol) => d.enum_variant(symbol, *idx),
        Value::Union {
            index,
            inner,
            n_variants,
            null_variant,
        } => {
            let mut empty_reader: &[u8] = &[];
            d.union_branch(
                *index,
                *n_variants,
                *null_variant,
                &**inner,
                &mut empty_reader,
            )
        }
        Value::Array(val) => {
            let mut a = ValueArrayAccess::new(val);
            d.array(&mut a)
        }
        Value::Map(val) => {
            let vals: Vec<_> = val.clone().into_iter().collect();
            let mut m = ValueMapAccess::new(vals.as_slice());
            d.map(&mut m)
        }
        Value::Record(val) => {
            let mut a = ValueRecordAccess::new(val);
            d.record(&mut a)
        }
        Value::Json(val) => d.json::<&[u8]>(V(val)),
        Value::Uuid(val) => d.uuid::<&[u8]>(V(val.to_string().as_bytes())),
    }
}

pub trait AvroDeserializer {
    fn deserialize<R: AvroRead, D: AvroDecode>(self, r: &mut R, d: D) -> Result<D::Out, AvroError>;
}

#[derive(Clone, Copy)]
pub struct GeneralDeserializer<'a> {
    pub schema: SchemaNode<'a>,
}

/// Cap on recursive `GeneralDeserializer::deserialize` calls. Avro records
/// may reference themselves (`{"name":"X","type":"record","fields":[
/// {"name":"x","type":"X"}]}`), so a malicious file plus matching wire
/// bytes can recurse forever and overflow the stack.
const MAX_DECODE_DEPTH: usize = 128;

thread_local! {
    static DECODE_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Cumulative `Value` nodes decoded so far in the current top-level decode,
    /// shared by every array and map in the datum and bounded by
    /// [`MAX_VALUE_NODES`]. Reset to `0` at each top-level entry (see
    /// [`DecodeDepthGuard::enter`]) so the budget composes across nesting
    /// instead of resetting per collection. Charged via [`charge_value_nodes`].
    static DECODE_NODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

struct DecodeDepthGuard;
impl DecodeDepthGuard {
    fn enter() -> Result<Self, AvroError> {
        DECODE_DEPTH.with(|d| {
            let new = d.get() + 1;
            if new > MAX_DECODE_DEPTH {
                return Err(AvroError::Decode(DecodeError::Custom(format!(
                    "Avro decode depth exceeds limit {MAX_DECODE_DEPTH}"
                ))));
            }
            d.set(new);
            // The `Value`-node budget is shared across every array/map in one
            // datum so nesting can't multiply the cap (see `MAX_VALUE_NODES`).
            // This is the top-level entry (depth 0 -> 1), so reset it: each datum
            // starts fresh even if a previous decode on this thread errored out
            // partway and left the counter non-zero.
            if new == 1 {
                DECODE_NODES.with(|n| n.set(0));
            }
            Ok(DecodeDepthGuard)
        })
    }
}
impl Drop for DecodeDepthGuard {
    fn drop(&mut self) {
        DECODE_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Charges `nodes` against the per-datum [`DECODE_NODES`] budget shared by every
/// array and map in a single top-level decode, rejecting once the cumulative
/// total exceeds [`MAX_VALUE_NODES`]. `kind` (`"array"` / `"map"`) only labels
/// the error.
///
/// The budget is shared — rather than tracked per collection instance — so the
/// cap composes across nesting; see [`MAX_VALUE_NODES`] for why a per-collection
/// budget would let nested zero-width collections amplify past it.
fn charge_value_nodes(kind: &str, nodes: usize) -> Result<(), AvroError> {
    DECODE_NODES.with(|n| {
        let total = n.get().saturating_add(nodes);
        if total > MAX_VALUE_NODES {
            return Err(AvroError::Decode(DecodeError::Custom(format!(
                "Avro {kind} decode exceeds cumulative limit {MAX_VALUE_NODES} decoded values"
            ))));
        }
        n.set(total);
        Ok(())
    })
}

impl<'a> AvroDeserializer for GeneralDeserializer<'a> {
    fn deserialize<R: AvroRead, D: AvroDecode>(self, r: &mut R, d: D) -> Result<D::Out, AvroError> {
        let _guard = DecodeDepthGuard::enter()?;
        use ValueOrReader::Reader;
        match self.schema.inner {
            SchemaPiece::Null => d.scalar(Scalar::Null),
            SchemaPiece::Boolean => {
                let mut buf = [0u8; 1];
                r.read_exact(&mut buf[..])?;
                let val = match buf[0] {
                    0u8 => false,
                    1u8 => true,
                    other => return Err(AvroError::Decode(DecodeError::BadBoolean(other))),
                };
                d.scalar(Scalar::Boolean(val))
            }
            SchemaPiece::Int => {
                let val = zag_i32(r)?;
                d.scalar(Scalar::Int(val))
            }
            SchemaPiece::Long => {
                let val = zag_i64(r)?;
                d.scalar(Scalar::Long(val))
            }
            SchemaPiece::Float => {
                let val = decode_float(r)?;
                d.scalar(Scalar::Float(val))
            }
            SchemaPiece::Double => {
                let val = decode_double(r)?;
                d.scalar(Scalar::Double(val))
            }
            SchemaPiece::Date => {
                let days = zag_i32(r)?;
                d.scalar(Scalar::Date(days))
            }
            SchemaPiece::TimestampMilli => {
                let total_millis = zag_i64(r)?;
                let scalar = match build_ts_value(total_millis, TsUnit::Millis)? {
                    Value::Timestamp(ts) => Scalar::Timestamp(ts),
                    _ => unreachable!(),
                };
                d.scalar(scalar)
            }
            SchemaPiece::TimestampMicro => {
                let total_micros = zag_i64(r)?;
                let scalar = match build_ts_value(total_micros, TsUnit::Micros)? {
                    Value::Timestamp(ts) => Scalar::Timestamp(ts),
                    _ => unreachable!(),
                };
                d.scalar(scalar)
            }
            SchemaPiece::Decimal {
                precision,
                scale,
                fixed_size,
            } => {
                let len = fixed_size.map(Ok).unwrap_or_else(|| decode_len(r))?;
                d.decimal(*precision, *scale, Reader { len, r })
            }
            SchemaPiece::Bytes => {
                let len = decode_len(r)?;
                d.bytes(Reader { len, r })
            }
            SchemaPiece::String => {
                let len = decode_len(r)?;
                d.string(Reader { len, r })
            }
            SchemaPiece::Json => {
                let len = decode_len(r)?;
                d.json(Reader { len, r })
            }
            SchemaPiece::Uuid => {
                let len = decode_len(r)?;
                d.uuid(Reader { len, r })
            }
            SchemaPiece::Array(inner) => {
                // From the spec:
                // Arrays are encoded as a series of blocks. Each block consists of a long count value, followed by that many array items. A block with count zero indicates the end of the array. Each item is encoded per the array's item schema.
                // If a block's count is negative, its absolute value is used, and the count is followed immediately by a long block size indicating the number of bytes in the block. This block size permits fast skipping through data, e.g., when projecting a record to a subset of its fields.

                let mut a = SimpleArrayAccess::new(r, self.schema.step(inner));
                d.array(&mut a)
            }
            SchemaPiece::Map(inner) => {
                // See logic for `SchemaPiece::Array` above. Maps are encoded similarly.
                let mut m = SimpleMapAccess::new(self.schema.step(inner), r);
                d.map(&mut m)
            }
            SchemaPiece::Union(inner) => {
                let index = decode_long_nonneg(r)? as usize;
                let variants = inner.variants();
                match variants.get(index) {
                    Some(variant) => {
                        let n_variants = variants.len();
                        let null_variant = variants
                            .iter()
                            .position(|v| v == &SchemaPieceOrNamed::Piece(SchemaPiece::Null));
                        let dsr = GeneralDeserializer {
                            schema: self.schema.step(variant),
                        };
                        d.union_branch(index, n_variants, null_variant, dsr, r)
                    }
                    None => Err(AvroError::Decode(DecodeError::BadUnionIndex {
                        index,
                        len: variants.len(),
                    })),
                }
            }
            SchemaPiece::ResolveIntLong => {
                let val = zag_i32(r)? as i64;
                d.scalar(Scalar::Long(val))
            }
            SchemaPiece::ResolveIntFloat => {
                let val = zag_i32(r)? as f32;
                d.scalar(Scalar::Float(val))
            }
            SchemaPiece::ResolveIntDouble => {
                let val = zag_i32(r)? as f64;
                d.scalar(Scalar::Double(val))
            }
            SchemaPiece::ResolveLongFloat => {
                let val = zag_i64(r)? as f32;
                d.scalar(Scalar::Float(val))
            }
            SchemaPiece::ResolveLongDouble => {
                let val = zag_i64(r)? as f64;
                d.scalar(Scalar::Double(val))
            }
            SchemaPiece::ResolveFloatDouble => {
                let val = decode_float(r)? as f64;
                d.scalar(Scalar::Double(val))
            }
            SchemaPiece::ResolveConcreteUnion {
                index,
                inner,
                n_reader_variants,
                reader_null_variant,
            } => {
                let dsr = GeneralDeserializer {
                    schema: self.schema.step(&**inner),
                };
                d.union_branch(*index, *n_reader_variants, *reader_null_variant, dsr, r)
            }
            SchemaPiece::ResolveUnionUnion {
                permutation,
                n_reader_variants,
                reader_null_variant,
            } => {
                let index = decode_long_nonneg(r)? as usize;
                if index >= permutation.len() {
                    return Err(AvroError::Decode(DecodeError::BadUnionIndex {
                        index,
                        len: permutation.len(),
                    }));
                }
                match &permutation[index] {
                    Err(e) => Err(e.clone()),
                    Ok((index, variant)) => {
                        let dsr = GeneralDeserializer {
                            schema: self.schema.step(variant),
                        };
                        d.union_branch(*index, *n_reader_variants, *reader_null_variant, dsr, r)
                    }
                }
            }
            SchemaPiece::ResolveUnionConcrete { index, inner } => {
                let found_index = decode_long_nonneg(r)? as usize;
                if *index != found_index {
                    Err(AvroError::Decode(DecodeError::WrongUnionIndex {
                        expected: *index,
                        actual: found_index,
                    }))
                } else {
                    let dsr = GeneralDeserializer {
                        schema: self.schema.step(inner.as_ref()),
                    };
                    // The reader is not expecting a union here, so don't call `D::union_branch`
                    dsr.deserialize(r, d)
                }
            }
            SchemaPiece::Record {
                doc: _,
                fields,
                lookup: _,
            } => {
                let mut a = SimpleRecordAccess::new(self.schema, r, fields);
                d.record(&mut a)
            }
            SchemaPiece::Enum {
                symbols,
                doc: _,
                default_idx: _,
            } => {
                let index = decode_int_nonneg(r)? as usize;
                match symbols.get(index) {
                    None => Err(AvroError::Decode(DecodeError::BadEnumIndex {
                        index,
                        len: symbols.len(),
                    })),
                    Some(symbol) => d.enum_variant(symbol, index),
                }
            }
            SchemaPiece::Fixed { size } => d.fixed(Reader { len: *size, r }),
            // XXX - This does not deliver fields to the consumer in the same order they were
            // declared in the reader schema, which might cause headache for consumers...
            // Unfortunately, there isn't a good way to do so without pre-decoding the whole record
            // (which would require a lot of allocations)
            // and then sorting the fields. So, just let the consumer deal with re-ordering.
            SchemaPiece::ResolveRecord {
                defaults,
                fields,
                n_reader_fields: _,
            } => {
                let mut a = ResolvedRecordAccess::new(defaults, fields, r, self.schema);
                d.record(&mut a)
            }
            SchemaPiece::ResolveEnum {
                doc: _,
                symbols,
                default,
            } => {
                let index = decode_int_nonneg(r)? as usize;
                match symbols.get(index) {
                    None => Err(AvroError::Decode(DecodeError::BadEnumIndex {
                        index,
                        len: symbols.len(),
                    })),
                    Some(op) => match op {
                        Err(missing) => {
                            if let Some((reader_index, symbol)) = default.clone() {
                                d.enum_variant(&symbol, reader_index)
                            } else {
                                Err(AvroError::Decode(DecodeError::MissingEnumIndex {
                                    index,
                                    symbol: missing.clone(),
                                }))
                            }
                        }
                        Ok((index, name)) => d.enum_variant(name, *index),
                    },
                }
            }
            SchemaPiece::ResolveIntTsMilli => {
                let total_millis = zag_i32(r)?;
                let scalar = match build_ts_value(total_millis as i64, TsUnit::Millis)? {
                    Value::Timestamp(ts) => Scalar::Timestamp(ts),
                    _ => unreachable!(),
                };
                d.scalar(scalar)
            }
            SchemaPiece::ResolveIntTsMicro => {
                let total_micros = zag_i32(r)?;
                let scalar = match build_ts_value(total_micros as i64, TsUnit::Micros)? {
                    Value::Timestamp(ts) => Scalar::Timestamp(ts),
                    _ => unreachable!(),
                };
                d.scalar(scalar)
            }
            SchemaPiece::ResolveDateTimestamp => {
                let days = zag_i32(r)?;

                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .expect("naive date known valid")
                    .checked_add_signed(
                        chrono::Duration::try_days(days.into())
                            .ok_or(AvroError::Decode(DecodeError::BadDate(days)))?,
                    )
                    .ok_or(AvroError::Decode(DecodeError::BadDate(days)))?;
                let dt = date.and_hms_opt(0, 0, 0).expect("HMS known valid");
                d.scalar(Scalar::Timestamp(dt))
            }
        }
    }
}
/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<'a, R: AvroRead>(
    schema: SchemaNode<'a>,
    reader: &'a mut R,
) -> Result<Value, AvroError> {
    let d = ValueDecoder;
    let dsr = GeneralDeserializer { schema };
    let val = dsr.deserialize(reader, d)?;
    Ok(val)
}
