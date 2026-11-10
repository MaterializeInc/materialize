// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::{soft_assert_or_log, soft_panic_or_log};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{Datum, Timestamp};
use timely::PartialOrder;
use timely::progress::Antichain;

/// Returns `(last_completed_refresh, next_refresh)` for a REFRESH materialized view.
///
/// `initial_as_of` is the committed first refresh, not the installation as-of.
/// `write_frontier` must be known. An empty frontier means completion, not an
/// absent observation. A finite frontier beyond the first refresh is reported
/// directly as the next refresh.
pub fn refresh_introspection(
    refresh_schedule: &RefreshSchedule,
    initial_as_of: &Antichain<Timestamp>,
    write_frontier: &Antichain<Timestamp>,
) -> (Datum<'static>, Datum<'static>) {
    if write_frontier.is_empty() {
        let last_completed_refresh = if let Some(last_refresh) = refresh_schedule.last_refresh() {
            last_refresh.into()
        } else {
            // For REFRESH EVERY, saturating roundup puts a refresh at MAX.
            Timestamp::MAX.into()
        };
        (last_completed_refresh, Datum::Null)
    } else if PartialOrder::less_equal(write_frontier, initial_as_of) {
        let initial_as_of = initial_as_of
            .as_option()
            .expect("initial_as_of can't be [], because then there would be no refreshes at all");
        let first_refresh = refresh_schedule
            .round_up_timestamp(*initial_as_of)
            .expect("sequencing makes sure that REFRESH MVs always have a first refresh");
        soft_assert_or_log!(
            first_refresh == *initial_as_of,
            "initial_as_of should be set to the first refresh"
        );
        (Datum::Null, first_refresh.into())
    } else {
        let write_frontier = write_frontier.as_option().expect("checked above");
        let last_completed_refresh = refresh_schedule
            .round_down_timestamp_m1(*write_frontier)
            .map_or_else(
                || {
                    soft_panic_or_log!(
                        "rounding down should have returned the first refresh or later"
                    );
                    Datum::Null
                },
                |last_completed_refresh| last_completed_refresh.into(),
            );
        (last_completed_refresh, (*write_frontier).into())
    }
}
