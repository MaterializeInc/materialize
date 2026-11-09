// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_timely_util::capture::progress_link;
use timely::progress::Antichain;

use super::{CommandProgress, MaintenanceProgress};

/// Bound on worker steps a pushed frontier may take to reach the probe.
const MAX_STEPS: usize = 100;

#[mz_ore::test]
fn applied_commands_advance_the_replayed_frontier() {
    let (pusher, source) = progress_link();
    timely::execute_directly(move |worker| {
        let progress = MaintenanceProgress::render(worker, source);
        let mut applied = CommandProgress::new(pusher);
        assert!(!progress.passed(0), "nothing is applied initially");

        applied.applied(4);
        applied.applied(9);
        for _ in 0..MAX_STEPS {
            if progress.passed(9) {
                break;
            }
            worker.step_or_park(None);
        }
        assert!(
            progress.passed(9),
            "the frontier passes the last applied command"
        );
        assert!(!progress.passed(10), "the frontier stops after it");
        assert_eq!(progress.frontier(), Antichain::from_elem(10));
    });
}
