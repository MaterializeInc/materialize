// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Description of how a dataflow follows time, independent of time.

use mz_proto::{RustType, TryFromProtoError};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::Timestamp;
use proptest::arbitrary::{any, Arbitrary};
use proptest::prelude::BoxedStrategy;
use proptest::strategy::{Just, Strategy, Union};
use serde::{Deserialize, Serialize};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_compute_types.time_dependence.rs"
));

/// Description of how a dataflow follows time.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub enum TimeDependence {
    /// Valid up to a some nested time, rounded according to the refresh schedule.
    RefreshSchedule(Option<RefreshSchedule>, Vec<Self>),
    /// Valid up to the wall-clock time.
    Wallclock,
}

impl TimeDependence {
    /// Normalizes by removing unnecessary nesting.
    pub fn normalize(&mut self) {
        use TimeDependence::*;
        match self {
            RefreshSchedule(None, existing) if existing.len() == 1 => {
                *self = existing.remove(0);
            }
            _ => {}
        }
    }

    /// Applies the indefiniteness to a wall clock time.
    pub fn apply(&self, wall_clock: Timestamp) -> Option<Timestamp> {
        match self {
            TimeDependence::RefreshSchedule(schedule, inner) => {
                let result = inner.iter().map(|inner| inner.apply(wall_clock)).min()??;
                if let Some(schedule) = schedule {
                    schedule.round_up_timestamp(result)
                } else {
                    Some(result)
                }
            }
            TimeDependence::Wallclock => Some(wall_clock),
        }
    }
}

impl RustType<ProtoTimeDependence> for TimeDependence {
    fn into_proto(&self) -> ProtoTimeDependence {
        use crate::time_dependence::proto_time_dependence::ProtoRefreshSchedule;
        ProtoTimeDependence {
            kind: Some(match self {
                TimeDependence::RefreshSchedule(schedule, inner) => {
                    proto_time_dependence::Kind::RefreshSchedule(ProtoRefreshSchedule {
                        refresh_schedule: schedule.as_ref().map(|s| s.into_proto()),
                        dependence: inner.into_proto(),
                    })
                }
                TimeDependence::Wallclock => proto_time_dependence::Kind::Wallclock(()),
            }),
        }
    }

    fn from_proto(proto: ProtoTimeDependence) -> Result<Self, TryFromProtoError> {
        use crate::time_dependence::proto_time_dependence::ProtoRefreshSchedule;
        let inner = match proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoTimeDependence::kind"))?
        {
            proto_time_dependence::Kind::RefreshSchedule(ProtoRefreshSchedule {
                refresh_schedule,
                dependence,
            }) => TimeDependence::RefreshSchedule(
                refresh_schedule
                    .map(RefreshSchedule::from_proto)
                    .transpose()?,
                dependence
                    .into_iter()
                    .map(TimeDependence::from_proto)
                    .collect::<Result<_, _>>()?,
            ),
            proto_time_dependence::Kind::Wallclock(()) => TimeDependence::Wallclock,
        };
        Ok(inner)
    }
}

impl Arbitrary for TimeDependence {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            any::<RefreshSchedule>()
                .prop_map(|s| {
                    TimeDependence::RefreshSchedule(Some(s), vec![TimeDependence::Wallclock])
                })
                .boxed(),
            Just(TimeDependence::Wallclock).boxed(),
        ])
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_time_dependence_normalize() {
        let mut i = TimeDependence::RefreshSchedule(None, vec![TimeDependence::Wallclock]);
        i.normalize();
        assert_eq!(i, TimeDependence::Wallclock);

        let mut i = TimeDependence::RefreshSchedule(
            Some(RefreshSchedule {
                everies: vec![],
                ats: vec![Timestamp::from(1000)],
            }),
            vec![TimeDependence::Wallclock],
        );
        i.normalize();
        assert_eq!(
            i,
            TimeDependence::RefreshSchedule(
                Some(RefreshSchedule {
                    everies: vec![],
                    ats: vec![Timestamp::from(1000)],
                }),
                vec![TimeDependence::Wallclock]
            )
        );

        i = TimeDependence::Wallclock;
        i.normalize();
        assert_eq!(i, TimeDependence::Wallclock);
    }
}
