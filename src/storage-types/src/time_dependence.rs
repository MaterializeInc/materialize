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
use proptest::strategy::Strategy;
use serde::{Deserialize, Serialize};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.time_dependence.rs"
));

/// Description of how a dataflow follows time.
///
/// The default value indicates the dataflow follows wall-clock without modifications.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct TimeDependence {
    /// Optional refresh schedule. None indicates no rounding.
    pub schedule: Option<RefreshSchedule>,
    /// Inner dependencies to evaluate first. Empty implies wall-clock dependence.
    pub dependence: Vec<Self>,
}

impl TimeDependence {
    /// Construct a new [`TimeDependence`] from an optional schedule and a collection of
    /// dependencies.
    pub fn new(schedule: Option<RefreshSchedule>, dependence: Vec<Self>) -> Self {
        Self {
            schedule,
            dependence,
        }
    }

    /// Normalizes by removing unnecessary nesting.
    pub fn normalize(this: Option<Self>) -> Option<Self> {
        match this {
            Some(TimeDependence {
                schedule: None,
                mut dependence,
            }) if dependence.len() == 1 => Some(dependence.remove(0)),
            other => other,
        }
    }

    /// Applies the indefiniteness to a wall clock time.
    pub fn apply(&self, wall_clock: Timestamp) -> Option<Timestamp> {
        let result = self
            .dependence
            .iter()
            .map(|inner| inner.apply(wall_clock))
            .min()
            .unwrap_or(Some(wall_clock))?;

        if let Some(schedule) = &self.schedule {
            schedule.round_up_timestamp(result)
        } else {
            Some(result)
        }
    }
}

impl RustType<ProtoTimeDependence> for TimeDependence {
    fn into_proto(&self) -> ProtoTimeDependence {
        ProtoTimeDependence {
            schedule: self.schedule.as_ref().map(|s| s.into_proto()),
            dependence: self.dependence.into_proto(),
        }
    }

    fn from_proto(proto: ProtoTimeDependence) -> Result<Self, TryFromProtoError> {
        Ok(TimeDependence {
            schedule: proto
                .schedule
                .map(RefreshSchedule::from_proto)
                .transpose()?,
            dependence: proto
                .dependence
                .into_iter()
                .map(TimeDependence::from_proto)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl Arbitrary for TimeDependence {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<Option<RefreshSchedule>>()
            .prop_map(|s| TimeDependence::new(s, vec![]))
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_time_dependence_normalize() {
        let i = TimeDependence::default();
        let i = TimeDependence::normalize(Some(i));
        assert_eq!(i, Some(TimeDependence::default()),);

        let i = TimeDependence::new(
            Some(RefreshSchedule {
                everies: vec![],
                ats: vec![Timestamp::from(1000)],
            }),
            vec![],
        );
        let i = TimeDependence::normalize(Some(i));
        assert_eq!(
            i,
            Some(TimeDependence {
                schedule: Some(RefreshSchedule {
                    everies: vec![],
                    ats: vec![Timestamp::from(1000)],
                }),
                dependence: vec![],
            })
        );
    }

    #[mz_ore::test]
    fn test_apply() {
        let schedule = |at| RefreshSchedule {
            everies: vec![],
            ats: vec![at],
        };
        // A default schedule follows wall-clock.
        assert_eq!(
            Some(100.into()),
            TimeDependence::default().apply(100.into())
        );

        // Nesting default schedules follows wall-clock.
        assert_eq!(
            Some(100.into()),
            TimeDependence::new(None, vec![TimeDependence::default()]).apply(100.into())
        );

        // Default refresh schedules refresh never, no wall-clock dependence.
        assert_eq!(
            None,
            TimeDependence::new(Some(RefreshSchedule::default()), vec![]).apply(100.into())
        );

        // Refresh schedule rounds up
        assert_eq!(
            Some(200.into()),
            TimeDependence::new(Some(schedule(200.into())), vec![]).apply(100.into())
        );

        // Smallest refresh wins.
        assert_eq!(
            Some(300.into()),
            TimeDependence::new(
                None,
                vec![
                    TimeDependence::new(Some(schedule(400.into())), vec![]),
                    TimeDependence::new(Some(schedule(300.into())), vec![])
                ]
            )
            .apply(100.into())
        );

        // Defined for all times, dependence rounds up.
        assert_eq!(
            None,
            TimeDependence::new(
                Some(schedule(200.into())),
                vec![
                    TimeDependence::new(Some(schedule(400.into())), vec![]),
                    TimeDependence::new(Some(schedule(300.into())), vec![])
                ]
            )
            .apply(100.into())
        );

        // Schedule rounds up minimum of dependence.
        assert_eq!(
            Some(350.into()),
            TimeDependence::new(
                Some(schedule(350.into())),
                vec![
                    TimeDependence::new(Some(schedule(400.into())), vec![]),
                    TimeDependence::new(Some(schedule(300.into())), vec![])
                ]
            )
            .apply(100.into())
        );

        // Any default dependence forces wall-clock dependence.
        assert_eq!(
            Some(100.into()),
            TimeDependence::new(
                None,
                vec![
                    TimeDependence::new(Some(schedule(400.into())), vec![]),
                    TimeDependence::new(None, vec![])
                ]
            )
            .apply(100.into())
        );
    }
}
