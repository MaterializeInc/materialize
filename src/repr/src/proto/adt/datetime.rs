// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::adt::datetime`].

include!(concat!(env!("OUT_DIR"), "/adt.datetime.rs"));

use super::super::{ProtoRepr, TryFromProtoError};
use crate::adt::datetime::Timezone::*;
use crate::adt::datetime::{DateTimeUnits, Timezone};
use chrono::FixedOffset;
use chrono_tz::Tz;

impl From<&Timezone> for ProtoTimezone {
    fn from(value: &Timezone) -> Self {
        use proto_timezone::Kind;
        ProtoTimezone {
            kind: Some(match value {
                FixedOffset(fo) => Kind::FixedOffset(fo.into_proto()),
                Tz(tz) => Kind::Tz(tz.into_proto()),
            }),
        }
    }
}

impl TryFrom<ProtoTimezone> for Timezone {
    type Error = TryFromProtoError;

    fn try_from(value: ProtoTimezone) -> Result<Self, Self::Error> {
        use proto_timezone::Kind;
        let kind = value
            .kind
            .ok_or_else(|| TryFromProtoError::MissingField("ProtoTimezone::kind".into()))?;
        Ok(match kind {
            Kind::FixedOffset(pof) => Timezone::FixedOffset(FixedOffset::from_proto(pof)?),
            Kind::Tz(ptz) => Timezone::Tz(Tz::from_proto(ptz)?),
        })
    }
}

impl From<&DateTimeUnits> for ProtoDateTimeUnits {
    fn from(value: &DateTimeUnits) -> Self {
        use proto_date_time_units::Kind;
        ProtoDateTimeUnits {
            kind: Some(match value {
                DateTimeUnits::Epoch => Kind::Epoch(()),
                DateTimeUnits::Millennium => Kind::Millennium(()),
                DateTimeUnits::Century => Kind::Century(()),
                DateTimeUnits::Decade => Kind::Decade(()),
                DateTimeUnits::Year => Kind::Year(()),
                DateTimeUnits::Quarter => Kind::Quarter(()),
                DateTimeUnits::Week => Kind::Week(()),
                DateTimeUnits::Month => Kind::Month(()),
                DateTimeUnits::Hour => Kind::Hour(()),
                DateTimeUnits::Day => Kind::Day(()),
                DateTimeUnits::DayOfWeek => Kind::DayOfWeek(()),
                DateTimeUnits::DayOfYear => Kind::DayOfYear(()),
                DateTimeUnits::IsoDayOfWeek => Kind::IsoDayOfWeek(()),
                DateTimeUnits::IsoDayOfYear => Kind::IsoDayOfYear(()),
                DateTimeUnits::Minute => Kind::Minute(()),
                DateTimeUnits::Second => Kind::Second(()),
                DateTimeUnits::Milliseconds => Kind::Milliseconds(()),
                DateTimeUnits::Microseconds => Kind::Microseconds(()),
                DateTimeUnits::Timezone => Kind::Timezone(()),
                DateTimeUnits::TimezoneHour => Kind::TimezoneHour(()),
                DateTimeUnits::TimezoneMinute => Kind::TimezoneMinute(()),
            }),
        }
    }
}

impl TryFrom<ProtoDateTimeUnits> for DateTimeUnits {
    type Error = TryFromProtoError;

    fn try_from(value: ProtoDateTimeUnits) -> Result<Self, Self::Error> {
        use proto_date_time_units::Kind;
        let kind = value
            .kind
            .ok_or_else(|| TryFromProtoError::MissingField("ProtoDateTimeUnits.kind".into()))?;
        Ok(match kind {
            Kind::Epoch(_) => DateTimeUnits::Epoch,
            Kind::Millennium(_) => DateTimeUnits::Millennium,
            Kind::Century(_) => DateTimeUnits::Century,
            Kind::Decade(_) => DateTimeUnits::Decade,
            Kind::Year(_) => DateTimeUnits::Year,
            Kind::Quarter(_) => DateTimeUnits::Quarter,
            Kind::Week(_) => DateTimeUnits::Week,
            Kind::Month(_) => DateTimeUnits::Month,
            Kind::Hour(_) => DateTimeUnits::Hour,
            Kind::Day(_) => DateTimeUnits::Day,
            Kind::DayOfWeek(_) => DateTimeUnits::DayOfWeek,
            Kind::DayOfYear(_) => DateTimeUnits::DayOfYear,
            Kind::IsoDayOfWeek(_) => DateTimeUnits::IsoDayOfWeek,
            Kind::IsoDayOfYear(_) => DateTimeUnits::IsoDayOfYear,
            Kind::Minute(_) => DateTimeUnits::Minute,
            Kind::Second(_) => DateTimeUnits::Second,
            Kind::Milliseconds(_) => DateTimeUnits::Milliseconds,
            Kind::Microseconds(_) => DateTimeUnits::Microseconds,
            Kind::Timezone(_) => DateTimeUnits::Timezone,
            Kind::TimezoneHour(_) => DateTimeUnits::TimezoneHour,
            Kind::TimezoneMinute(_) => DateTimeUnits::TimezoneMinute,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
       #[test]
        fn datetimeunits_serialization_roundtrip(expect in any::<DateTimeUnits>() ) {
            let actual = protobuf_roundtrip::<_, ProtoDateTimeUnits>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
