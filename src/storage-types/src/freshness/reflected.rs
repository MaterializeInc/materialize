// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The reflected time of a collection, the inverse of its time dependence.

use mz_repr::Timestamp;
use mz_repr::refresh_schedule::RefreshSchedule;

use crate::time_dependence::TimeDependence;

/// The latest wall-clock time whose data a collection with write frontier `frontier` contains
/// in full.
///
/// Returns `None` if `frontier` is 0, or if a refresh schedule in `dependence` has no refresh at
/// or before the bound it inverts, as before its first refresh.
pub fn reflected_time(dependence: &TimeDependence, frontier: Timestamp) -> Option<Timestamp> {
    // The largest output timestamp that is complete.
    reflected_at(dependence, frontier.step_back()?)
}

/// `reflected_time` for the inclusive bound `bound` on the complete output timestamps.
fn reflected_at(dependence: &TimeDependence, bound: Timestamp) -> Option<Timestamp> {
    // Updates land at the next refresh, so only inputs up to the last refresh at or before
    // `bound` are visible.
    let bound = match &dependence.schedule {
        Some(schedule) => last_refresh_at_or_before(schedule, bound)?,
        None => bound,
    };
    // NOTE: Inverting `apply` as a whole, as the largest t with `apply(t) < frontier`,
    // overstates for several children, because `apply` takes the minimum of the children's
    // frontiers. Taking the minimum of the children's reflected times inverts each path.
    dependence.dependence.iter().try_fold(bound, |min, child| {
        Some(min.min(reflected_at(child, bound)?))
    })
}

/// The last refresh of `schedule` at or before `timestamp`, if any.
///
/// NOTE: A `REFRESH EVERY` grid extends below its `aligned_to`, so a view whose frontier is
/// still its first refresh reflects the grid point one interval before it, a time before the
/// view existed. Until that first refresh completes, the view's staleness is overstated by
/// about one interval.
fn last_refresh_at_or_before(
    schedule: &RefreshSchedule,
    timestamp: Timestamp,
) -> Option<Timestamp> {
    let refresh = schedule.round_down_timestamp_m1(timestamp.checked_add(1)?)?;
    // NOTE: `round_down_timestamp_m1` saturates at timestamp 0 when the previous refresh would
    // be negative, so 0 only counts if it is an actual refresh.
    (schedule.round_up_timestamp(refresh) == Some(refresh)).then_some(refresh)
}
