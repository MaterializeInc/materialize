// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A permanent storage encoding for rows.
//!
//! See row.proto for details.

use bytes::BufMut;
use chrono::Timelike;
use dec::Decimal;
use mz_persist_types::columnar::{
    ColumnFormat, ColumnGet, ColumnPush, Data, DataType, PartDecoder, PartEncoder, Schema,
};
use mz_persist_types::part::{ColumnsMut, ColumnsRef};
use mz_persist_types::Codec;
use prost::Message;
use uuid::Uuid;

use mz_ore::cast::CastFrom;
use mz_proto::{ProtoType, RustType, TryFromProtoError};

use crate::adt::array::ArrayDimension;
use crate::adt::numeric::Numeric;
use crate::adt::range::{Range, RangeInner, RangeLowerBound, RangeUpperBound};
use crate::chrono::ProtoNaiveTime;
use crate::row::proto_datum::DatumType;
use crate::row::{
    ProtoArray, ProtoArrayDimension, ProtoDatum, ProtoDatumOther, ProtoDict, ProtoDictElement,
    ProtoNumeric, ProtoRange, ProtoRangeInner, ProtoRow,
};
use crate::{Datum, RelationDesc, Row, RowPacker, ScalarType};

impl Codec for Row {
    type Schema = RelationDesc;

    fn codec_name() -> String {
        "protobuf[Row]".into()
    }

    /// Encodes a row into the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::decode]. It's guaranteed to be
    /// readable by future versions of Materialize through v(TODO: Figure out
    /// our policy).
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    /// Decodes a row from the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::encode]. It can read rows
    /// encoded by historical versions of Materialize back to v(TODO: Figure out
    /// our policy).
    fn decode(buf: &[u8]) -> Result<Row, String> {
        let proto_row = ProtoRow::decode(buf).map_err(|err| err.to_string())?;
        Row::try_from(&proto_row)
    }
}

impl From<&ScalarType> for ColumnFormat {
    fn from(value: &ScalarType) -> Self {
        match value {
            ScalarType::Bool => ColumnFormat::Bool,
            ScalarType::Int16 => ColumnFormat::I16,
            ScalarType::Int32 => ColumnFormat::I32,
            ScalarType::Int64 => ColumnFormat::I64,
            ScalarType::UInt16 => ColumnFormat::U16,
            ScalarType::UInt32 => ColumnFormat::U32,
            ScalarType::UInt64 => ColumnFormat::U64,
            ScalarType::Float32 => ColumnFormat::F32,
            ScalarType::Float64 => ColumnFormat::F64,
            ScalarType::Bytes => ColumnFormat::Bytes,
            ScalarType::String => ColumnFormat::String,
            _ => panic!("TODO: finish implementing all the ScalarType variants"),
        }
    }
}

#[derive(Debug)]
enum DatumEncoder<'a> {
    Bool(&'a mut <bool as Data>::Mut),
    BoolOpt(&'a mut <Option<bool> as Data>::Mut),
    String(&'a mut <String as Data>::Mut),
    StringOpt(&'a mut <Option<String> as Data>::Mut),
}

impl<'a> DatumEncoder<'a> {
    fn encode(&mut self, datum: Datum) {
        match self {
            DatumEncoder::Bool(col) => {
                let x = match datum {
                    Datum::True => true,
                    Datum::False => false,
                    _ => panic!("Datum cannot be converted into bool: {}", datum),
                };
                col.push(x);
            }
            DatumEncoder::BoolOpt(col) => {
                let x = match datum {
                    Datum::True => Some(true),
                    Datum::False => Some(false),
                    Datum::Null => None,
                    _ => panic!("Datum cannot be converted into Option<bool>: {}", datum),
                };
                col.push(x)
            }
            DatumEncoder::String(col) => {
                let x = match datum {
                    Datum::String(x) => x,
                    _ => panic!("Datum cannot be converted into String: {}", datum),
                };
                ColumnPush::<String>::push(*col, x);
            }
            DatumEncoder::StringOpt(col) => {
                let x = match datum {
                    Datum::String(x) => Some(x),
                    Datum::Null => None,
                    _ => panic!("Datum cannot be converted into Option<String>: {}", datum),
                };
                ColumnPush::<Option<String>>::push(*col, x);
            }
        }
    }
}

/// An implementation of [PartEncoder] for [Row].
#[derive(Debug)]
pub struct RowEncoder<'a>(Vec<DatumEncoder<'a>>);

impl<'a> PartEncoder<'a, Row> for RowEncoder<'a> {
    fn encode(&mut self, val: &Row) {
        for (encoder, datum) in self.0.iter_mut().zip(val.iter()) {
            encoder.encode(datum);
        }
    }
}

#[derive(Debug)]
enum DatumDecoder<'a> {
    Bool(&'a <bool as Data>::Col),
    BoolOpt(&'a <Option<bool> as Data>::Col),
    String(&'a <String as Data>::Col),
    StringOpt(&'a <Option<String> as Data>::Col),
}

impl<'a> DatumDecoder<'a> {
    fn decode(&self, idx: usize) -> Datum<'a> {
        match self {
            DatumDecoder::Bool(col) => Datum::from(col.get(idx)),
            DatumDecoder::BoolOpt(col) => Datum::from(col.get(idx)),
            DatumDecoder::String(col) => Datum::from(ColumnGet::<String>::get(*col, idx)),
            DatumDecoder::StringOpt(col) => {
                Datum::from(ColumnGet::<Option<String>>::get(*col, idx))
            }
        }
    }
}

/// An implementation of [PartDecoder] for [Row].
#[derive(Debug)]
pub struct RowDecoder<'a>(Vec<DatumDecoder<'a>>);

impl<'a> PartDecoder<'a, Row> for RowDecoder<'a> {
    fn decode(&self, idx: usize, val: &mut Row) {
        let mut packer = val.packer();
        for decoder in self.0.iter() {
            packer.push(decoder.decode(idx));
        }
    }
}

impl Schema<Row> for RelationDesc {
    type Encoder<'a> = RowEncoder<'a>;
    type Decoder<'a> = RowDecoder<'a>;

    fn columns(&self) -> Vec<(String, DataType)> {
        self.iter()
            .map(|(name, typ)| {
                let data_type = DataType {
                    optional: typ.nullable,
                    format: ColumnFormat::from(&typ.scalar_type),
                };
                (name.0.clone(), data_type)
            })
            .collect()
    }

    fn decoder<'a>(&self, mut part: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        let mut decoders = Vec::new();
        for (name, typ) in self.iter() {
            match (typ.nullable, &typ.scalar_type) {
                (false, ScalarType::Bool) => {
                    decoders.push(DatumDecoder::Bool(part.col::<bool>(name.as_str())?));
                }
                (true, ScalarType::Bool) => {
                    decoders.push(DatumDecoder::BoolOpt(
                        part.col::<Option<bool>>(name.as_str())?,
                    ));
                }
                (false, ScalarType::String) => {
                    decoders.push(DatumDecoder::String(part.col::<String>(name.as_str())?));
                }
                (true, ScalarType::String) => {
                    decoders.push(DatumDecoder::StringOpt(
                        part.col::<Option<String>>(name.as_str())?,
                    ));
                }
                _ => panic!("TODO: finish implementing all the Datum variants"),
            };
        }
        let () = part.finish()?;
        Ok(RowDecoder(decoders))
    }

    fn encoder<'a>(&self, mut part: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        let mut encoders = Vec::new();
        for (name, typ) in self.iter() {
            match (typ.nullable, &typ.scalar_type) {
                (false, ScalarType::Bool) => {
                    encoders.push(DatumEncoder::Bool(part.col::<bool>(name.as_str())?));
                }
                (true, ScalarType::Bool) => {
                    encoders.push(DatumEncoder::BoolOpt(
                        part.col::<Option<bool>>(name.as_str())?,
                    ));
                }
                (false, ScalarType::String) => {
                    encoders.push(DatumEncoder::String(part.col::<String>(name.as_str())?));
                }
                (true, ScalarType::String) => {
                    encoders.push(DatumEncoder::StringOpt(
                        part.col::<Option<String>>(name.as_str())?,
                    ));
                }
                _ => panic!("TODO: finish implementing all the Datum variants"),
            }
        }
        let () = part.finish()?;
        Ok(RowEncoder(encoders))
    }
}

impl<'a> From<Datum<'a>> for ProtoDatum {
    fn from(x: Datum<'a>) -> Self {
        let datum_type = match x {
            Datum::False => DatumType::Other(ProtoDatumOther::False.into()),
            Datum::True => DatumType::Other(ProtoDatumOther::True.into()),
            Datum::Int16(x) => DatumType::Int16(x.into()),
            Datum::Int32(x) => DatumType::Int32(x),
            Datum::UInt8(x) => DatumType::Uint8(x.into()),
            Datum::UInt16(x) => DatumType::Uint16(x.into()),
            Datum::UInt32(x) => DatumType::Uint32(x),
            Datum::UInt64(x) => DatumType::Uint64(x),
            Datum::Int64(x) => DatumType::Int64(x),
            Datum::Float32(x) => DatumType::Float32(x.into_inner()),
            Datum::Float64(x) => DatumType::Float64(x.into_inner()),
            Datum::Date(x) => DatumType::Date(x.into_proto()),
            Datum::Time(x) => DatumType::Time(ProtoNaiveTime {
                secs: x.num_seconds_from_midnight(),
                frac: x.nanosecond(),
            }),
            Datum::Timestamp(x) => DatumType::Timestamp(x.into_proto()),
            Datum::TimestampTz(x) => DatumType::TimestampTz(x.into_proto()),
            Datum::Interval(x) => DatumType::Interval(x.into_proto()),
            Datum::Bytes(x) => DatumType::Bytes(x.to_vec()),
            Datum::String(x) => DatumType::String(x.to_owned()),
            Datum::Array(x) => DatumType::Array(ProtoArray {
                elements: Some(ProtoRow {
                    datums: x.elements().iter().map(|x| x.into()).collect(),
                }),
                dims: x
                    .dims()
                    .into_iter()
                    .map(|x| ProtoArrayDimension {
                        lower_bound: u64::cast_from(x.lower_bound),
                        length: u64::cast_from(x.length),
                    })
                    .collect(),
            }),
            Datum::List(x) => DatumType::List(ProtoRow {
                datums: x.iter().map(|x| x.into()).collect(),
            }),
            Datum::Map(x) => DatumType::Dict(ProtoDict {
                elements: x
                    .iter()
                    .map(|(k, v)| ProtoDictElement {
                        key: k.to_owned(),
                        val: Some(v.into()),
                    })
                    .collect(),
            }),
            Datum::Numeric(x) => {
                // TODO: Do we need this defensive clone?
                let mut x = x.0.clone();
                if let Some((bcd, scale)) = x.to_packed_bcd() {
                    DatumType::Numeric(ProtoNumeric { bcd, scale })
                } else if x.is_nan() {
                    DatumType::Other(ProtoDatumOther::NumericNaN.into())
                } else if x.is_infinite() {
                    if x.is_negative() {
                        DatumType::Other(ProtoDatumOther::NumericNegInf.into())
                    } else {
                        DatumType::Other(ProtoDatumOther::NumericPosInf.into())
                    }
                } else if x.is_special() {
                    panic!("internal error: unhandled special numeric value: {}", x);
                } else {
                    panic!(
                        "internal error: to_packed_bcd returned None for non-special value: {}",
                        x
                    )
                }
            }
            Datum::JsonNull => DatumType::Other(ProtoDatumOther::JsonNull.into()),
            Datum::Uuid(x) => DatumType::Uuid(x.as_bytes().to_vec()),
            Datum::MzTimestamp(x) => DatumType::MzTimestamp(x.into()),
            Datum::Dummy => DatumType::Other(ProtoDatumOther::Dummy.into()),
            Datum::Null => DatumType::Other(ProtoDatumOther::Null.into()),
            Datum::Range(super::Range { inner }) => DatumType::Range(Box::new(ProtoRange {
                inner: inner.map(|RangeInner { lower, upper }| {
                    Box::new(ProtoRangeInner {
                        lower_inclusive: lower.inclusive,
                        lower: lower.bound.map(|bound| Box::new(bound.datum().into())),
                        upper_inclusive: upper.inclusive,
                        upper: upper.bound.map(|bound| Box::new(bound.datum().into())),
                    })
                }),
            })),
        };
        ProtoDatum {
            datum_type: Some(datum_type),
        }
    }
}

impl RowPacker<'_> {
    fn try_push_proto(&mut self, x: &ProtoDatum) -> Result<(), String> {
        match &x.datum_type {
            Some(DatumType::Other(o)) => match ProtoDatumOther::from_i32(*o) {
                Some(ProtoDatumOther::Unknown) => return Err("unknown datum type".into()),
                Some(ProtoDatumOther::Null) => self.push(Datum::Null),
                Some(ProtoDatumOther::False) => self.push(Datum::False),
                Some(ProtoDatumOther::True) => self.push(Datum::True),
                Some(ProtoDatumOther::JsonNull) => self.push(Datum::JsonNull),
                Some(ProtoDatumOther::Dummy) => {
                    // We plan to remove the `Dummy` variant soon (#17099). To prepare for that, we
                    // emit a log to Sentry here, to notify us of any instances that might have
                    // been made durable.
                    #[cfg(feature = "tracing_")]
                    tracing::error!("protobuf decoding found Dummy datum");
                    self.push(Datum::Dummy);
                }
                Some(ProtoDatumOther::NumericPosInf) => self.push(Datum::from(Numeric::infinity())),
                Some(ProtoDatumOther::NumericNegInf) => {
                    self.push(Datum::from(-Numeric::infinity()))
                }
                Some(ProtoDatumOther::NumericNaN) => self.push(Datum::from(Numeric::nan())),
                None => return Err(format!("unknown datum type: {}", o)),
            },
            Some(DatumType::Int16(x)) => {
                let x = i16::try_from(*x)
                    .map_err(|_| format!("int16 field stored with out of range value: {}", *x))?;
                self.push(Datum::Int16(x))
            }
            Some(DatumType::Int32(x)) => self.push(Datum::Int32(*x)),
            Some(DatumType::Int64(x)) => self.push(Datum::Int64(*x)),
            Some(DatumType::Uint8(x)) => {
                let x = u8::try_from(*x)
                    .map_err(|_| format!("uint8 field stored with out of range value: {}", *x))?;
                self.push(Datum::UInt8(x))
            }
            Some(DatumType::Uint16(x)) => {
                let x = u16::try_from(*x)
                    .map_err(|_| format!("uint16 field stored with out of range value: {}", *x))?;
                self.push(Datum::UInt16(x))
            }
            Some(DatumType::Uint32(x)) => self.push(Datum::UInt32(*x)),
            Some(DatumType::Uint64(x)) => self.push(Datum::UInt64(*x)),
            Some(DatumType::Float32(x)) => self.push(Datum::Float32((*x).into())),
            Some(DatumType::Float64(x)) => self.push(Datum::Float64((*x).into())),
            Some(DatumType::Bytes(x)) => self.push(Datum::Bytes(x)),
            Some(DatumType::String(x)) => self.push(Datum::String(x)),
            Some(DatumType::Uuid(x)) => {
                // Uuid internally has a [u8; 16] so we'll have to do at least
                // one copy, but there's currently an additional one when the
                // Vec is created. Perhaps the protobuf Bytes support will let
                // us fix one of them.
                let u = Uuid::from_slice(x).map_err(|err| err.to_string())?;
                self.push(Datum::Uuid(u));
            }
            Some(DatumType::Date(x)) => self.push(Datum::Date(x.clone().into_rust()?)),
            Some(DatumType::Time(x)) => self.push(Datum::Time(x.clone().into_rust()?)),
            Some(DatumType::Timestamp(x)) => self.push(Datum::Timestamp(x.clone().into_rust()?)),
            Some(DatumType::TimestampTz(x)) => {
                self.push(Datum::TimestampTz(x.clone().into_rust()?))
            }
            Some(DatumType::Interval(x)) => self.push(Datum::Interval(
                x.clone()
                    .into_rust()
                    .map_err(|e: TryFromProtoError| e.to_string())?,
            )),
            Some(DatumType::List(x)) => self.push_list_with(|row| -> Result<(), String> {
                for d in x.datums.iter() {
                    row.try_push_proto(d)?;
                }
                Ok(())
            })?,
            Some(DatumType::Array(x)) => {
                let dims = x
                    .dims
                    .iter()
                    .map(|x| ArrayDimension {
                        lower_bound: usize::cast_from(x.lower_bound),
                        length: usize::cast_from(x.length),
                    })
                    .collect::<Vec<_>>();
                match x.elements.as_ref() {
                    None => self.push_array(&dims, vec![].iter()),
                    Some(elements) => {
                        // TODO: Could we avoid this Row alloc if we made a
                        // push_array_with?
                        let elements_row = Row::try_from(elements)?;
                        self.push_array(&dims, elements_row.iter())
                    }
                }
                .map_err(|err| err.to_string())?
            }
            Some(DatumType::Dict(x)) => self.push_dict_with(|row| -> Result<(), String> {
                for e in x.elements.iter() {
                    row.push(Datum::from(e.key.as_str()));
                    let val = e
                        .val
                        .as_ref()
                        .ok_or_else(|| format!("missing val for key: {}", e.key))?;
                    row.try_push_proto(val)?;
                }
                Ok(())
            })?,
            Some(DatumType::Numeric(x)) => {
                // Reminder that special values like NaN, PosInf, and NegInf are
                // represented as variants of ProtoDatumOther.
                let n = Decimal::from_packed_bcd(&x.bcd, x.scale).map_err(|err| err.to_string())?;
                self.push(Datum::from(n))
            }
            Some(DatumType::MzTimestamp(x)) => self.push(Datum::MzTimestamp((*x).into())),
            Some(DatumType::Range(inner)) => {
                let ProtoRange { inner } = &**inner;
                match inner {
                    None => self.push_range(Range { inner: None }).unwrap(),
                    Some(inner) => {
                        let ProtoRangeInner {
                            lower_inclusive,
                            lower,
                            upper_inclusive,
                            upper,
                        } = &**inner;

                        self.push_range_with(
                            RangeLowerBound {
                                inclusive: *lower_inclusive,
                                bound: lower
                                    .as_ref()
                                    .map(|d| |row: &mut RowPacker| row.try_push_proto(&*d)),
                            },
                            RangeUpperBound {
                                inclusive: *upper_inclusive,
                                bound: upper
                                    .as_ref()
                                    .map(|d| |row: &mut RowPacker| row.try_push_proto(&*d)),
                            },
                        )
                        .expect("decoding ProtoRow must succeed");
                    }
                }
            }
            None => return Err("unknown datum type".into()),
        };
        Ok(())
    }
}

/// TODO: remove this in favor of [`RustType::from_proto`].
impl TryFrom<&ProtoRow> for Row {
    type Error = String;

    fn try_from(x: &ProtoRow) -> Result<Self, Self::Error> {
        // TODO: Try to pre-size this.
        // see https://github.com/MaterializeInc/materialize/issues/12631
        let mut row = Row::default();
        let mut packer = row.packer();
        for d in x.datums.iter() {
            packer.try_push_proto(d)?;
        }
        Ok(row)
    }
}

impl RustType<ProtoRow> for Row {
    fn into_proto(&self) -> ProtoRow {
        let datums = self.iter().map(|x| x.into()).collect();
        ProtoRow { datums }
    }

    fn from_proto(proto: ProtoRow) -> Result<Self, TryFromProtoError> {
        // TODO: Try to pre-size this.
        // see https://github.com/MaterializeInc/materialize/issues/12631
        let mut row = Row::default();
        let mut packer = row.packer();
        for d in proto.datums.iter() {
            packer
                .try_push_proto(d)
                .map_err(TryFromProtoError::RowConversionError)?;
        }
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use mz_persist_types::Codec;
    use uuid::Uuid;

    use crate::adt::array::ArrayDimension;
    use crate::adt::interval::Interval;
    use crate::adt::numeric::Numeric;
    use crate::adt::timestamp::CheckedTimestamp;
    use crate::{ColumnType, Datum, RelationDesc, Row, ScalarType};

    // TODO: datadriven golden tests for various interesting Datums and Rows to
    // catch any changes in the encoding.

    #[test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn roundtrip() {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.extend([
            Datum::False,
            Datum::True,
            Datum::Int16(1),
            Datum::Int32(2),
            Datum::Int64(3),
            Datum::Float32(4f32.into()),
            Datum::Float64(5f64.into()),
            Datum::Date(
                NaiveDate::from_ymd_opt(6, 7, 8)
                    .unwrap()
                    .try_into()
                    .unwrap(),
            ),
            Datum::Time(NaiveTime::from_hms_opt(9, 10, 11).unwrap()),
            Datum::Timestamp(
                CheckedTimestamp::from_timestamplike(
                    NaiveDate::from_ymd_opt(12, 13 % 12, 14)
                        .unwrap()
                        .and_time(NaiveTime::from_hms_opt(15, 16, 17).unwrap()),
                )
                .unwrap(),
            ),
            Datum::TimestampTz(
                CheckedTimestamp::from_timestamplike(DateTime::from_utc(
                    NaiveDate::from_ymd_opt(18, 19 % 12, 20)
                        .unwrap()
                        .and_time(NaiveTime::from_hms_opt(21, 22, 23).unwrap()),
                    Utc,
                ))
                .unwrap(),
            ),
            Datum::Interval(Interval {
                months: 24,
                days: 42,
                micros: 25,
            }),
            Datum::Bytes(&[26, 27]),
            Datum::String("28"),
            Datum::from(Numeric::from(29)),
            Datum::from(Numeric::infinity()),
            Datum::from(-Numeric::infinity()),
            Datum::from(Numeric::nan()),
            Datum::JsonNull,
            Datum::Uuid(Uuid::from_u128(30)),
            Datum::Dummy,
            Datum::Null,
        ]);
        packer
            .push_array(
                &[ArrayDimension {
                    lower_bound: 2,
                    length: 2,
                }],
                vec![Datum::Int32(31), Datum::Int32(32)],
            )
            .expect("valid array");
        packer.push_list_with(|packer| {
            packer.push(Datum::String("33"));
            packer.push_list_with(|packer| {
                packer.push(Datum::String("34"));
                packer.push(Datum::String("35"));
            });
            packer.push(Datum::String("36"));
            packer.push(Datum::String("37"));
        });
        packer.push_dict_with(|row| {
            // Add a bunch of data to the hash to ensure we don't get a
            // HashMap's random iteration anywhere in the encode/decode path.
            let mut i = 38;
            for _ in 0..20 {
                row.push(Datum::String(&i.to_string()));
                row.push(Datum::Int32(i + 1));
                i += 2;
            }
        });

        let mut encoded = Vec::new();
        row.encode(&mut encoded);
        assert_eq!(Row::decode(&encoded), Ok(row));
    }

    fn schema_and_row() -> (RelationDesc, Row) {
        let row = Row::pack(vec![
            Datum::True,
            Datum::False,
            Datum::True,
            Datum::False,
            Datum::Null,
        ]);
        let schema = RelationDesc::from_names_and_types(vec![
            (
                "a",
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::Bool,
                },
            ),
            (
                "b",
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::Bool,
                },
            ),
            (
                "c",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::Bool,
                },
            ),
            (
                "d",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::Bool,
                },
            ),
            (
                "e",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::Bool,
                },
            ),
        ]);
        (schema, row)
    }

    #[test]
    fn columnar_roundtrip() {
        let (schema, row) = schema_and_row();
        assert_eq!(
            mz_persist_types::columnar::validate_roundtrip(&schema, &row),
            Ok(())
        );
    }

    #[test]
    fn parquet_roundtrip() {
        let (schema, row) = schema_and_row();
        assert_eq!(
            mz_persist_types::parquet::validate_roundtrip(&schema, &row),
            Ok(())
        );
    }
}
