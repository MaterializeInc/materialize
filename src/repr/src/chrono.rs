// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Custom [`proptest::strategy::Strategy`] implementations and Protobuf types
//! for the [`chrono`] fields used in the codebase.
//!
//! See the [`proptest`] docs[^1] for an example.
//!
//! [^1]: <https://altsysrq.github.io/proptest-book/proptest-derive/modifiers.html#strategy>

use std::str::FromStr;

use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc,
};
use chrono_tz::{Tz, TZ_VARIANTS};
use proptest::prelude::Strategy;

use crate::proto::{RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_repr.chrono.rs"));

impl RustType<ProtoNaiveDate> for NaiveDate {
    fn into_proto(&self) -> ProtoNaiveDate {
        ProtoNaiveDate {
            year: self.year(),
            ordinal: self.ordinal(),
        }
    }

    fn from_proto(proto: ProtoNaiveDate) -> Result<Self, TryFromProtoError> {
        NaiveDate::from_yo_opt(proto.year, proto.ordinal).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "NaiveDate::from_yo_opt({},{}) failed",
                proto.year, proto.ordinal
            ))
        })
    }
}

impl RustType<ProtoNaiveTime> for NaiveTime {
    fn into_proto(&self) -> ProtoNaiveTime {
        ProtoNaiveTime {
            secs: self.num_seconds_from_midnight(),
            frac: self.nanosecond(),
        }
    }

    fn from_proto(proto: ProtoNaiveTime) -> Result<Self, TryFromProtoError> {
        NaiveTime::from_num_seconds_from_midnight_opt(proto.secs, proto.frac).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "NaiveTime::from_num_seconds_from_midnight_opt({},{}) failed",
                proto.secs, proto.frac
            ))
        })
    }
}

impl RustType<ProtoNaiveDateTime> for NaiveDateTime {
    fn into_proto(&self) -> ProtoNaiveDateTime {
        ProtoNaiveDateTime {
            year: self.year(),
            ordinal: self.ordinal(),
            secs: self.num_seconds_from_midnight(),
            frac: self.nanosecond(),
        }
    }

    fn from_proto(proto: ProtoNaiveDateTime) -> Result<Self, TryFromProtoError> {
        let date = NaiveDate::from_yo_opt(proto.year, proto.ordinal).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "NaiveDate::from_yo_opt({},{}) failed",
                proto.year, proto.ordinal
            ))
        })?;

        let time = NaiveTime::from_num_seconds_from_midnight_opt(proto.secs, proto.frac)
            .ok_or_else(|| {
                TryFromProtoError::DateConversionError(format!(
                    "NaiveTime::from_num_seconds_from_midnight_opt({},{}) failed",
                    proto.secs, proto.frac
                ))
            })?;

        Ok(NaiveDateTime::new(date, time))
    }
}

impl RustType<ProtoNaiveDateTime> for DateTime<Utc> {
    fn into_proto(&self) -> ProtoNaiveDateTime {
        self.naive_utc().into_proto()
    }

    fn from_proto(proto: ProtoNaiveDateTime) -> Result<Self, TryFromProtoError> {
        Ok(DateTime::from_utc(NaiveDateTime::from_proto(proto)?, Utc))
    }
}

impl RustType<ProtoFixedOffset> for FixedOffset {
    fn into_proto(&self) -> ProtoFixedOffset {
        ProtoFixedOffset {
            local_minus_utc: self.local_minus_utc(),
        }
    }

    fn from_proto(proto: ProtoFixedOffset) -> Result<Self, TryFromProtoError> {
        FixedOffset::east_opt(proto.local_minus_utc).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "FixedOffset::east_opt({}) failed.",
                proto.local_minus_utc
            ))
        })
    }
}

/// Encode a Tz as string representation. This is not the most space efficient solution, but
/// it is immune to changes in the chrono_tz (and is fully compatible with its public API).
impl RustType<ProtoTz> for chrono_tz::Tz {
    fn into_proto(&self) -> ProtoTz {
        ProtoTz {
            name: self.name().into(),
        }
    }

    fn from_proto(proto: ProtoTz) -> Result<Self, TryFromProtoError> {
        Tz::from_str(&proto.name).map_err(TryFromProtoError::DateConversionError)
    }
}

pub fn any_naive_date() -> impl Strategy<Value = NaiveDate> {
    (0..1000000).prop_map(NaiveDate::from_num_days_from_ce)
}

pub fn any_naive_datetime() -> impl Strategy<Value = NaiveDateTime> {
    use ::chrono::naive::{MAX_DATETIME, MIN_DATETIME};
    (0..(MAX_DATETIME.nanosecond() - MIN_DATETIME.nanosecond()))
        .prop_map(|x| MIN_DATETIME + Duration::nanoseconds(x as i64))
}

pub fn any_datetime() -> impl Strategy<Value = DateTime<Utc>> {
    any_naive_datetime().prop_map(|x| DateTime::from_utc(x, Utc))
}

pub fn any_fixed_offset() -> impl Strategy<Value = FixedOffset> {
    (-86_399..86_400).prop_map(FixedOffset::east)
}

pub fn any_timezone() -> impl Strategy<Value = Tz> {
    (0..TZ_VARIANTS.len()).prop_map(|idx| *TZ_VARIANTS.get(idx).unwrap())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::proto::protobuf_roundtrip;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[test]
        fn naive_date_protobuf_roundtrip(expect in any_naive_date() ) {
            let actual = protobuf_roundtrip::<_, ProtoNaiveDate>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn naive_date_time_protobuf_roundtrip(expect in any_naive_datetime() ) {
            let actual = protobuf_roundtrip::<_, ProtoNaiveDateTime>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn date_time_protobuf_roundtrip(expect in any_datetime() ) {
            let actual = protobuf_roundtrip::<_, ProtoNaiveDateTime>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn fixed_offset_protobuf_roundtrip(expect in any_fixed_offset() ) {
            let actual = protobuf_roundtrip::<_, ProtoFixedOffset>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
