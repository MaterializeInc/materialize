// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the compute state's own helpers, as opposed to the peek sweep it drives.

use mz_dyncfg::ConfigUpdates;

use super::*;

#[mz_ore::test]
fn row_iteration_limit_observes_updates_and_disabled_rows() {
    let config = mz_dyncfgs::all_dyncfgs();
    let row_iteration_config = PeekRowIterationConfig::new(&config);
    let mut tracker = PeekRowIterationTracker::new(row_iteration_config.current_limit(), 0);

    tracker.track_next().unwrap();
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&PEEK_ROW_ITERATION_LIMIT, 3);
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, false);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&PEEK_ROW_ITERATION_LIMIT, 5);
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();
    assert_eq!(
        tracker.track_next(),
        Err(PeekError::RowIterationLimitExceeded { limit: 5 })
    );
}
