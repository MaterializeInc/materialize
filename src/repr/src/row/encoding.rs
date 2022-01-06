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

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use dec::Decimal;
use ore::cast::CastFrom;
use persist_types::Codec;
use protobuf::{Message, MessageField};
use uuid::Uuid;

use crate::adt::array::ArrayDimension;
use crate::adt::interval::Interval;
use crate::gen::row::proto_datum::Datum_type;
use crate::gen::row::{
    ProtoArray, ProtoArrayDimension, ProtoDate, ProtoDatum, ProtoDatumOther, ProtoDict,
    ProtoDictElement, ProtoInterval, ProtoNumeric, ProtoRow, ProtoTime, ProtoTimestamp,
};
use crate::{Datum, Row};

impl Codec for Row {
    fn codec_name() -> &'static str {
        "protobuf[Row]"
    }

    fn size_hint(&self) -> usize {
        // The row's internal encoding isn't a perfect proxy for the proto
        // equivalent, but it's probably good enough and this is fast.
        self.data.len()
    }

    /// Encodes a row into the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::decode]. It's guaranteed to be
    /// readable by future versions of Materialize through v(TODO: Figure out
    /// our policy).
    fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
        let temp = ProtoRow::from(self)
            .write_to_bytes()
            .expect("no required fields means no initialization errors");
        buf.extend(&temp);
    }

    /// Decodes a row from the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::encode]. It can read rows
    /// encoded by historical versions of Materialize back to v(TODO: Figure out
    /// our policy).
    fn decode(buf: &[u8]) -> Result<Row, String> {
        let proto_row = ProtoRow::parse_from_bytes(buf).map_err(|err| err.to_string())?;
        Row::try_from(&proto_row)
    }
}

impl<'a> From<Datum<'a>> for ProtoDatum {
    fn from(x: Datum<'a>) -> Self {
        let datum_type = match x {
            Datum::False => Datum_type::other(ProtoDatumOther::False.into()),
            Datum::True => Datum_type::other(ProtoDatumOther::True.into()),
            Datum::Int16(x) => Datum_type::int16(x.into()),
            Datum::Int32(x) => Datum_type::int32(x),
            Datum::Int64(x) => Datum_type::int64(x),
            Datum::Float32(x) => Datum_type::float32(x.into_inner()),
            Datum::Float64(x) => Datum_type::float64(x.into_inner()),
            Datum::Date(x) => Datum_type::date(ProtoDate {
                year: x.year(),
                ordinal: x.ordinal(),
                unknown_fields: Default::default(),
                cached_size: Default::default(),
            }),
            Datum::Time(x) => Datum_type::time(ProtoTime {
                secs: x.num_seconds_from_midnight(),
                nanos: x.nanosecond(),
                unknown_fields: Default::default(),
                cached_size: Default::default(),
            }),
            Datum::Timestamp(x) => Datum_type::timestamp(ProtoTimestamp {
                year: x.date().year(),
                ordinal: x.date().ordinal(),
                secs: x.time().num_seconds_from_midnight(),
                nanos: x.time().nanosecond(),
                is_tz: false,
                unknown_fields: Default::default(),
                cached_size: Default::default(),
            }),
            Datum::TimestampTz(x) => {
                let date = x.date().naive_utc();
                Datum_type::timestamp(ProtoTimestamp {
                    year: date.year(),
                    ordinal: date.ordinal(),
                    secs: x.time().num_seconds_from_midnight(),
                    nanos: x.time().nanosecond(),
                    is_tz: true,
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                })
            }
            Datum::Interval(x) => {
                let duration = x.duration.to_le_bytes();
                let (mut duration_lo, mut duration_hi) = ([0u8; 8], [0u8; 8]);
                duration_lo.copy_from_slice(&duration[..8]);
                duration_hi.copy_from_slice(&duration[8..]);
                Datum_type::interval(ProtoInterval {
                    months: x.months,
                    duration_lo: i64::from_le_bytes(duration_lo),
                    duration_hi: i64::from_le_bytes(duration_hi),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                })
            }
            Datum::Bytes(x) => Datum_type::bytes(x.to_vec()),
            Datum::String(x) => Datum_type::string(x.to_owned()),
            Datum::Array(x) => Datum_type::array(ProtoArray {
                elements: MessageField::some(ProtoRow {
                    datums: x.elements().iter().map(|x| x.into()).collect(),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                }),
                dims: x
                    .dims()
                    .into_iter()
                    .map(|x| ProtoArrayDimension {
                        lower_bound: u64::cast_from(x.lower_bound),
                        length: u64::cast_from(x.length),
                        unknown_fields: Default::default(),
                        cached_size: Default::default(),
                    })
                    .collect(),
                unknown_fields: Default::default(),
                cached_size: Default::default(),
            }),
            Datum::List(x) => Datum_type::list(ProtoRow {
                datums: x.iter().map(|x| x.into()).collect(),
                unknown_fields: Default::default(),
                cached_size: Default::default(),
            }),
            Datum::Map(x) => Datum_type::dict(ProtoDict {
                elements: x
                    .iter()
                    .map(|(k, v)| ProtoDictElement {
                        key: k.to_owned(),
                        val: MessageField::some(v.into()),
                        unknown_fields: Default::default(),
                        cached_size: Default::default(),
                    })
                    .collect(),
                unknown_fields: Default::default(),
                cached_size: Default::default(),
            }),
            Datum::Numeric(x) => {
                // NB: This intentionally doesn't reduce this like push_datum
                // does. That reduction doesn't effect equality, but it _is_
                // destructive and we don't want to lose e.g. how many zeros
                // were after the decimal.
                let (digits, exponent, bits, lsu) = x.0.to_raw_parts();
                Datum_type::numeric(ProtoNumeric {
                    digits,
                    exponent,
                    bits: u32::from(bits),
                    lsu: lsu.to_vec(),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                })
            }
            Datum::JsonNull => Datum_type::other(ProtoDatumOther::JsonNull.into()),
            Datum::Uuid(x) => Datum_type::uuid(x.as_bytes().to_vec()),
            Datum::Dummy => Datum_type::other(ProtoDatumOther::Dummy.into()),
            Datum::Null => Datum_type::other(ProtoDatumOther::Null.into()),
        };
        ProtoDatum {
            datum_type: Some(datum_type),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

impl From<&Row> for ProtoRow {
    fn from(x: &Row) -> Self {
        let datums = x.iter().map(|x| x.into()).collect();
        ProtoRow {
            datums,
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

impl Row {
    fn try_push_proto(&mut self, x: &ProtoDatum) -> Result<(), String> {
        match &x.datum_type {
            Some(Datum_type::other(o)) => match o.enum_value() {
                Ok(ProtoDatumOther::Unknown) => return Err("unknown datum type".into()),
                Ok(ProtoDatumOther::Null) => self.push(Datum::Null),
                Ok(ProtoDatumOther::False) => self.push(Datum::False),
                Ok(ProtoDatumOther::True) => self.push(Datum::True),
                Ok(ProtoDatumOther::JsonNull) => self.push(Datum::JsonNull),
                Ok(ProtoDatumOther::Dummy) => self.push(Datum::Dummy),
                Err(id) => return Err(format!("unknown datum type: {}", id)),
            },
            Some(Datum_type::int16(x)) => {
                let x = i16::try_from(*x)
                    .map_err(|_| format!("int16 field stored with out of range value: {}", *x))?;
                self.push(Datum::Int16(x))
            }
            Some(Datum_type::int32(x)) => self.push(Datum::Int32(*x)),
            Some(Datum_type::int64(x)) => self.push(Datum::Int64(*x)),
            Some(Datum_type::float32(x)) => self.push(Datum::Float32((*x).into())),
            Some(Datum_type::float64(x)) => self.push(Datum::Float64((*x).into())),
            Some(Datum_type::bytes(x)) => self.push(Datum::Bytes(x)),
            Some(Datum_type::string(x)) => self.push(Datum::String(x)),
            Some(Datum_type::uuid(x)) => {
                // Uuid internally has a [u8; 16] so we'll have to do at least
                // one copy, but there's currently an additional one when the
                // Vec is created. Perhaps the protobuf Bytes support will let
                // us fix one of them.
                let u = Uuid::from_slice(&x).map_err(|err| err.to_string())?;
                self.push(Datum::Uuid(u));
            }
            Some(Datum_type::date(x)) => {
                self.push(Datum::Date(NaiveDate::from_yo(x.year, x.ordinal)))
            }
            Some(Datum_type::time(x)) => self.push(Datum::Time(
                NaiveTime::from_num_seconds_from_midnight(x.secs, x.nanos),
            )),
            Some(Datum_type::timestamp(x)) => {
                let date = NaiveDate::from_yo(x.year, x.ordinal);
                let time = NaiveTime::from_num_seconds_from_midnight(x.secs, x.nanos);
                let datetime = date.and_time(time);
                if x.is_tz {
                    self.push(Datum::TimestampTz(DateTime::from_utc(datetime, Utc)));
                } else {
                    self.push(Datum::Timestamp(datetime));
                }
            }
            Some(Datum_type::interval(x)) => {
                let mut duration = [0u8; 16];
                duration[..8].copy_from_slice(&x.duration_lo.to_le_bytes());
                duration[8..].copy_from_slice(&x.duration_hi.to_le_bytes());
                let duration = i128::from_le_bytes(duration);
                self.push(Datum::Interval(Interval {
                    months: x.months,
                    duration,
                }))
            }
            Some(Datum_type::list(x)) => self.push_list_with(|row| -> Result<(), String> {
                for d in x.datums.iter() {
                    row.try_push_proto(d)?;
                }
                Ok(())
            })?,
            Some(Datum_type::array(x)) => {
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
            Some(Datum_type::dict(x)) => self.push_dict_with(|row| -> Result<(), String> {
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
            Some(Datum_type::numeric(x)) => {
                let bits = u8::try_from(x.bits).map_err(|err| err.to_string())?;
                // SAFETY: The parts are always derived from a previous call to
                // to_raw_parts and faithfully roundtrip them.
                //
                // TODO: Add a version of from_raw_parts to the Decimal library
                // that validates and returns an Option instead of panic'ing and
                // unsafe.
                let n = unsafe { Decimal::from_raw_parts(x.digits, x.exponent, bits, &x.lsu) };
                self.push(Datum::from(n))
            }
            None => return Err("unknown datum type".into()),
        };
        Ok(())
    }
}

impl TryFrom<&ProtoRow> for Row {
    type Error = String;

    fn try_from(x: &ProtoRow) -> Result<Self, Self::Error> {
        // TODO: Try to pre-size this.
        let mut row = Row::default();
        for d in x.datums.iter() {
            row.try_push_proto(d)?;
        }
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use persist_types::Codec;
    use uuid::Uuid;

    use crate::adt::array::ArrayDimension;
    use crate::adt::interval::Interval;
    use crate::adt::numeric::Numeric;
    use crate::{Datum, Row};

    // TODO: datadriven golden tests for various interesting Datums and Rows to
    // catch any changes in the encoding.

    #[test]
    fn roundtrip() {
        let mut row = Row::pack(vec![
            Datum::False,
            Datum::True,
            Datum::Int16(1),
            Datum::Int32(2),
            Datum::Int64(3),
            Datum::Float32(4f32.into()),
            Datum::Float64(5f64.into()),
            Datum::Date(NaiveDate::from_ymd(6, 7, 8)),
            Datum::Time(NaiveTime::from_hms(9, 10, 11)),
            Datum::Timestamp(
                NaiveDate::from_ymd(12, 13 % 12, 14).and_time(NaiveTime::from_hms(15, 16, 17)),
            ),
            Datum::TimestampTz(DateTime::from_utc(
                NaiveDate::from_ymd(18, 19 % 12, 20).and_time(NaiveTime::from_hms(21, 22, 23)),
                Utc,
            )),
            Datum::Interval(Interval {
                months: 24,
                duration: 25,
            }),
            Datum::Bytes(&[26, 27]),
            Datum::String("28".into()),
            Datum::from(Numeric::from(29)),
            Datum::JsonNull,
            Datum::Uuid(Uuid::from_u128(30)),
            Datum::Dummy,
            Datum::Null,
        ]);
        row.push_array(
            &[ArrayDimension {
                lower_bound: 2,
                length: 2,
            }],
            vec![Datum::Int32(31), Datum::Int32(32)],
        )
        .expect("valid array");
        row.push_list_with(|row| {
            row.push(Datum::String("33"));
            row.push_list_with(|row| {
                row.push(Datum::String("34"));
                row.push(Datum::String("35"));
            });
            row.push(Datum::String("36"));
            row.push(Datum::String("37"));
        });
        row.push_dict_with(|row| {
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
}
