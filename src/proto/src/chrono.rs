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

use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc,
};
use chrono_tz::{TZ_VARIANTS, Tz};
use proptest::prelude::Strategy;

use crate::{RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_proto.chrono.rs"));

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
        Ok(DateTime::from_naive_utc_and_offset(
            NaiveDateTime::from_proto(proto)?,
            Utc,
        ))
    }
}

pub fn any_naive_date() -> impl Strategy<Value = NaiveDate> {
    (0..1000000).prop_map(|d| NaiveDate::from_num_days_from_ce_opt(d).unwrap())
}

pub fn any_naive_datetime() -> impl Strategy<Value = NaiveDateTime> {
    (0..(NaiveDateTime::MAX.nanosecond() - NaiveDateTime::MIN.nanosecond()))
        .prop_map(|x| NaiveDateTime::MIN + Duration::nanoseconds(i64::from(x)))
}

pub fn any_datetime() -> impl Strategy<Value = DateTime<Utc>> {
    any_naive_datetime().prop_map(|x| DateTime::from_naive_utc_and_offset(x, Utc))
}

pub fn any_fixed_offset() -> impl Strategy<Value = FixedOffset> {
    (-86_399..86_400).prop_map(|o| FixedOffset::east_opt(o).unwrap())
}

pub fn any_timezone() -> impl Strategy<Value = Tz> {
    (0..TZ_VARIANTS.len()).prop_map(|idx| *TZ_VARIANTS.get(idx).unwrap())
}

#[cfg(test)]
mod tests {
    use crate::protobuf_roundtrip;
    use mz_ore::assert_ok;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn naive_date_protobuf_roundtrip(expect in any_naive_date() ) {
            let actual = protobuf_roundtrip::<_, ProtoNaiveDate>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn naive_date_time_protobuf_roundtrip(expect in any_naive_datetime() ) {
            let actual = protobuf_roundtrip::<_, ProtoNaiveDateTime>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn date_time_protobuf_roundtrip(expect in any_datetime() ) {
            let actual = protobuf_roundtrip::<_, ProtoNaiveDateTime>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
