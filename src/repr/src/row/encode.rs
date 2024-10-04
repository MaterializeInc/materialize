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

use bytes::{BufMut, Bytes};
use chrono::Timelike;
use dec::Decimal;
use itertools::{EitherOrBoth, Itertools};
use mz_ore::cast::CastFrom;
use mz_persist_types::Codec;
use mz_proto::chrono::ProtoNaiveTime;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use prost::Message;
use uuid::Uuid;

use crate::adt::array::ArrayDimension;

use crate::adt::numeric::Numeric;
use crate::adt::range::{Range, RangeInner, RangeLowerBound, RangeUpperBound};
use crate::row::proto_datum::DatumType;
use crate::row::{
    ProtoArray, ProtoArrayDimension, ProtoDatum, ProtoDatumOther, ProtoDict, ProtoDictElement,
    ProtoNumeric, ProtoRange, ProtoRangeInner, ProtoRow,
};
use crate::{Datum, ProtoRelationDesc, RelationDesc, Row, RowPacker};

impl Codec for Row {
    type Storage = ProtoRow;
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
    fn decode(buf: &[u8], schema: &RelationDesc) -> Result<Row, String> {
        // NB: We could easily implement this directly instead of via
        // `decode_from`, but do this so that we get maximal coverage of the
        // more complicated codepath.
        //
        // The length of the encoded ProtoRow (i.e. `buf.len()`) doesn't perfect
        // predict the length of the resulting Row, but it's definitely
        // correlated, so probably a decent estimate.
        let mut row = Row::with_capacity(buf.len());
        <Self as Codec>::decode_from(&mut row, buf, &mut None, schema)?;
        Ok(row)
    }

    fn decode_from<'a>(
        &mut self,
        buf: &'a [u8],
        storage: &mut Option<ProtoRow>,
        schema: &RelationDesc,
    ) -> Result<(), String> {
        let mut proto = storage.take().unwrap_or_default();
        proto.clear();
        proto.merge(buf).map_err(|err| err.to_string())?;
        let ret = self.decode_from_proto(&proto, schema);
        storage.replace(proto);
        ret
    }

    fn validate(row: &Self, desc: &Self::Schema) -> Result<(), String> {
        for x in Itertools::zip_longest(desc.iter_types(), row.iter()) {
            match x {
                EitherOrBoth::Both(typ, datum) if datum.is_instance_of(typ) => continue,
                _ => return Err(format!("row {:?} did not match desc {:?}", row, desc)),
            };
        }
        Ok(())
    }

    fn encode_schema(schema: &Self::Schema) -> Bytes {
        schema.into_proto().encode_to_vec().into()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        let proto = ProtoRelationDesc::decode(buf.as_ref()).expect("valid schema");
        proto.into_rust().expect("valid schema")
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
            Datum::Bytes(x) => DatumType::Bytes(Bytes::copy_from_slice(x)),
            Datum::String(x) => DatumType::String(x.to_owned()),
            Datum::Array(x) => DatumType::Array(ProtoArray {
                elements: Some(ProtoRow {
                    datums: x.elements().iter().map(|x| x.into()).collect(),
                }),
                dims: x
                    .dims()
                    .into_iter()
                    .map(|x| ProtoArrayDimension {
                        lower_bound: i64::cast_from(x.lower_bound),
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
            Datum::MzAclItem(x) => DatumType::MzAclItem(x.into_proto()),
            Datum::AclItem(x) => DatumType::AclItem(x.into_proto()),
        };
        ProtoDatum {
            datum_type: Some(datum_type),
        }
    }
}

impl RowPacker<'_> {
    pub(crate) fn try_push_proto(&mut self, x: &ProtoDatum) -> Result<(), String> {
        match &x.datum_type {
            Some(DatumType::Other(o)) => match ProtoDatumOther::try_from(*o) {
                Ok(ProtoDatumOther::Unknown) => return Err("unknown datum type".into()),
                Ok(ProtoDatumOther::Null) => self.push(Datum::Null),
                Ok(ProtoDatumOther::False) => self.push(Datum::False),
                Ok(ProtoDatumOther::True) => self.push(Datum::True),
                Ok(ProtoDatumOther::JsonNull) => self.push(Datum::JsonNull),
                Ok(ProtoDatumOther::Dummy) => {
                    // We plan to remove the `Dummy` variant soon (materialize#17099). To prepare for that, we
                    // emit a log to Sentry here, to notify us of any instances that might have
                    // been made durable.
                    #[cfg(feature = "tracing_")]
                    tracing::error!("protobuf decoding found Dummy datum");
                    self.push(Datum::Dummy);
                }
                Ok(ProtoDatumOther::NumericPosInf) => self.push(Datum::from(Numeric::infinity())),
                Ok(ProtoDatumOther::NumericNegInf) => self.push(Datum::from(-Numeric::infinity())),
                Ok(ProtoDatumOther::NumericNaN) => self.push(Datum::from(Numeric::nan())),
                Err(_) => return Err(format!("unknown datum type: {}", o)),
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
                        lower_bound: isize::cast_from(x.lower_bound),
                        length: usize::cast_from(x.length),
                    })
                    .collect::<Vec<_>>();
                match x.elements.as_ref() {
                    None => self.push_array(&dims, [].iter()),
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
            Some(DatumType::MzAclItem(x)) => self.push(Datum::MzAclItem(x.clone().into_rust()?)),
            Some(DatumType::AclItem(x)) => self.push(Datum::AclItem(x.clone().into_rust()?)),
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
        // see https://github.com/MaterializeInc/database-issues/issues/3640
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
        // see https://github.com/MaterializeInc/database-issues/issues/3640
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
    use crate::{Datum, RelationDesc, Row, ScalarType};

    #[mz_ore::test]
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
                CheckedTimestamp::from_timestamplike(DateTime::from_naive_utc_and_offset(
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

        let mut desc = RelationDesc::builder();
        for (idx, _) in row.iter().enumerate() {
            // HACK(parkmycar): We don't currently validate the types of the `RelationDesc` are
            // correct, just the number of columns. So we can fill in any type here.
            desc = desc.with_column(idx.to_string(), ScalarType::Int32.nullable(true));
        }
        let desc = desc.finish();

        let encoded = row.encode_to_vec();
        assert_eq!(Row::decode(&encoded, &desc), Ok(row));
    }
}
