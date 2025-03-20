// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Description of how a dataflow follows wall-clock time, independent of a specific point in time.

use mz_proto::{RustType, TryFromProtoError};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::Timestamp;
use proptest::arbitrary::{any, Arbitrary};
use proptest::prelude::BoxedStrategy;
use proptest::strategy::Strategy;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::instances::StorageInstanceId;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.time_dependence.rs"
));

/// Description of how a dataflow follows time.
///
/// The default value indicates the dataflow follows wall-clock without modifications.
///
/// Note: This is different from `Timeline` or `TimelineContext`, which describe in what timeline
/// an object exists. `TimeDependence` explains how an object in an epoch-based timeline
/// relates to wall-clock time.
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

    /// Merge any number of optional dependencies into one, using the supplied refresh schedule.
    ///
    /// It applies the following rules under the assumption that, ignoring the refresh schedule, the
    /// frontier of an object ticks at the rate of the slowest immediate dependency. For objects
    /// depending on wall-clock time, this is firstly wall-clock time, followed by refresh schedule.
    /// At the moment, we cannot express other behavior. This means:
    /// * A merge of anything with wall-clock time results in wall-clock time.
    /// * A merge of anything but wall-clock time or a refresh schedule results in a dependence
    ///   on the deduplicated collection of dependencies.
    /// * Otherwise, a dataflow is indeterminate, which expresses that we either don't know how it
    ///   follows wall-clock time, or is a constant collection.
    ///
    /// If the result is a dependence without refresh schedule on a single nested dependence, the
    /// function instead returns the inner value.
    pub fn merge(
        mut dependencies: Vec<Option<Self>>,
        schedule: Option<&RefreshSchedule>,
    ) -> Option<Self> {
        // Any `Some` value dominates the `None` values.
        dependencies.retain(Option::is_some);
        dependencies.sort();
        dependencies.dedup();

        if dependencies
            .iter()
            .any(|dep| *dep == Some(TimeDependence::default()))
        {
            // Wall-clock dependency is dominant.
            Some(TimeDependence::new(schedule.cloned(), vec![]))
        } else if !dependencies.is_empty() {
            // No immediate wall-clock dependency, but some other dependency.
            if schedule.is_none() && dependencies.len() == 1 {
                // We don't have a refresh schedule, and one dependency, so just return that.
                dependencies.remove(0)
            } else {
                // Insert our refresh schedule.
                let dependencies = dependencies.into_iter().flatten().collect();
                Some(TimeDependence::new(schedule.cloned(), dependencies))
            }
        } else {
            // Not related to wall-clock time; or unknown relation.
            None
        }
    }

    /// Applies the indefiniteness to a wall clock time.
    pub fn apply(&self, wall_clock: Timestamp) -> Option<Timestamp> {
        // We take the minimum of our dependencies applied to the wall-clock time, or just
        // wall-clock itself if we don't have any dependence. The minimum corresponds to the time
        // that we can expect as the output time at the passed wall-clock time.
        let result = self
            .dependence
            .iter()
            .map(|inner| inner.apply(wall_clock))
            .min()
            .unwrap_or(Some(wall_clock))?;

        // Round the result according to our refresh schedule, if there is any.
        if let Some(schedule) = &self.schedule {
            schedule.round_up_timestamp(result)
        } else {
            Some(result)
        }
    }
}

/// Errors arising when reading time dependence information.
#[derive(Error, Debug)]
pub enum TimeDependenceError {
    /// The given instance does not exist.
    #[error("instance does not exist: {0}")]
    InstanceMissing(StorageInstanceId),
    /// One of the imported collections does not exist.
    #[error("collection does not exist: {0}")]
    CollectionMissing(mz_repr::GlobalId),
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
    fn test_time_dependence_merge() {
        let schedule = |at| {
            Some(RefreshSchedule {
                everies: vec![],
                ats: vec![at],
            })
        };

        assert_eq!(None, TimeDependence::merge(vec![], None));
        let default = Some(TimeDependence::default());
        assert_eq!(
            default.clone(),
            TimeDependence::merge(vec![default.clone()], None)
        );
        assert_eq!(
            Some(TimeDependence::new(schedule(10.into()), vec![])),
            TimeDependence::merge(vec![default.clone()], schedule(10.into()).as_ref())
        );

        let scheduled = Some(TimeDependence::new(schedule(10.into()), vec![]));
        assert_eq!(
            scheduled.clone(),
            TimeDependence::merge(vec![scheduled.clone()], None)
        );
        assert_eq!(
            Some(TimeDependence::new(
                schedule(10.into()),
                vec![scheduled.clone().unwrap()]
            )),
            TimeDependence::merge(vec![scheduled.clone()], schedule(10.into()).as_ref())
        );
        assert_eq!(
            Some(TimeDependence::new(schedule(10.into()), vec![])),
            TimeDependence::merge(
                vec![default.clone(), scheduled.clone()],
                schedule(10.into()).as_ref()
            )
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
