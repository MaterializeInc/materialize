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

use crate::proto::{ProtoRepr, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_repr.chrono.rs"));

impl ProtoRepr for NaiveDate {
    type Repr = ProtoNaiveDate;

    fn into_proto(self: Self) -> Self::Repr {
        ProtoNaiveDate {
            year: self.year(),
            ordinal: self.ordinal(),
        }
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        NaiveDate::from_yo_opt(repr.year, repr.ordinal).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "NaiveDate::from_yo_opt({},{}) failed",
                repr.year, repr.ordinal
            ))
        })
    }
}

impl ProtoRepr for NaiveTime {
    type Repr = ProtoNaiveTime;

    fn into_proto(self: Self) -> Self::Repr {
        ProtoNaiveTime {
            secs: self.num_seconds_from_midnight(),
            frac: self.nanosecond(),
        }
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        NaiveTime::from_num_seconds_from_midnight_opt(repr.secs, repr.frac).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "NaiveTime::from_num_seconds_from_midnight_opt({},{}) failed",
                repr.secs, repr.frac
            ))
        })
    }
}

impl ProtoRepr for NaiveDateTime {
    type Repr = ProtoNaiveDateTime;

    fn into_proto(self: Self) -> Self::Repr {
        ProtoNaiveDateTime {
            year: self.year(),
            ordinal: self.ordinal(),
            secs: self.num_seconds_from_midnight(),
            frac: self.nanosecond(),
        }
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        let date = NaiveDate::from_yo_opt(repr.year, repr.ordinal).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "NaiveDate::from_yo_opt({},{}) failed",
                repr.year, repr.ordinal
            ))
        })?;

        let time = NaiveTime::from_num_seconds_from_midnight_opt(repr.secs, repr.frac).ok_or_else(
            || {
                TryFromProtoError::DateConversionError(format!(
                    "NaiveTime::from_num_seconds_from_midnight_opt({},{}) failed",
                    repr.secs, repr.frac
                ))
            },
        )?;

        Ok(NaiveDateTime::new(date, time))
    }
}

impl ProtoRepr for DateTime<Utc> {
    type Repr = ProtoNaiveDateTime;

    fn into_proto(self: Self) -> Self::Repr {
        self.naive_utc().into_proto()
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        Ok(DateTime::from_utc(NaiveDateTime::from_proto(repr)?, Utc))
    }
}

impl ProtoRepr for FixedOffset {
    type Repr = ProtoFixedOffset;

    fn into_proto(self: Self) -> Self::Repr {
        ProtoFixedOffset {
            local_minus_utc: self.local_minus_utc(),
        }
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        FixedOffset::east_opt(repr.local_minus_utc).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "FixedOffset::east_opt({}) failed.",
                repr.local_minus_utc
            ))
        })
    }
}

/// Encode a Tz as string representation. This is not the most space efficient solution, but
/// it is immune to changes in the chrono_tz (and is fully compatible with its public API).
impl ProtoRepr for chrono_tz::Tz {
    type Repr = ProtoTz;

    fn into_proto(self: Self) -> Self::Repr {
        ProtoTz {
            name: self.name().into(),
        }
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        Tz::from_str(&repr.name).map_err(TryFromProtoError::DateConversionError)
    }
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
    use crate::proto::protobuf_repr_roundtrip;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[test]
        fn naive_date_time_protobuf_roundtrip(expect in any_naive_datetime() ) {
            let actual =  protobuf_repr_roundtrip(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn date_time_protobuf_roundtrip(expect in any_datetime() ) {
            let actual =  protobuf_repr_roundtrip(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn fixed_offset_protobuf_roundtrip(expect in any_fixed_offset() ) {
            let actual =  protobuf_repr_roundtrip(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
