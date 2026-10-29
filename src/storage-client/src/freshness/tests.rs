// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::Timestamp;
use mz_storage_types::freshness::histogram::BucketScheme;
use mz_storage_types::freshness::recorder::RecorderConfig;
use mz_storage_types::freshness::{ResolvedTree, Ticker};
use timely::progress::Antichain;

use super::{FreshnessRecording, Measure, SharedVisibilityLog};

#[mz_ore::test]
fn visibility_log_is_pruned_by_age() {
    let tree = ResolvedTree::Leaf(Ticker::Aligned { interval_ms: 1000 });
    let config = RecorderConfig {
        scheme: BucketScheme::new(4).expect("power of two"),
        interval_ms: 1,
        period_ms: 60_000,
        retention_ms: 30 * 24 * 60 * 60 * 1000,
    };
    let log = SharedVisibilityLog::default();
    assert_eq!(log.input_log(None).visible_at(1), None);
    // One advance per second for 15 minutes, observed 5 ms after each tick.
    for s in 1..=900u64 {
        let frontier = Antichain::from_elem(Timestamp::from(s * 1000 + 1));
        log.record(&tree, &frontier, s * 1000 + 5, &config);
    }
    let copy = log.input_log(None);
    // Ten minutes are kept, and older arrivals are answered by the last pruned entry.
    assert_eq!(copy.visible_at(300_000), Some(300_005));
    assert_eq!(copy.visible_at(299_000), Some(299_005));
    assert_eq!(copy.visible_at(1), Some(299_005));
    assert_eq!(copy.visible_at(900_000), Some(900_005));
    // A copy for a consumer answers what it has not recorded yet exactly.
    let copy = log.input_log(Some(600_000));
    assert_eq!(copy.visible_at(600_001), Some(601_005));
}

#[mz_ore::test]
fn closed_collection_stays_visible_from_its_close() {
    let tree = ResolvedTree::Leaf(Ticker::Aligned { interval_ms: 1000 });
    let config = RecorderConfig {
        scheme: BucketScheme::new(4).expect("power of two"),
        interval_ms: 1,
        period_ms: 60_000,
        retention_ms: 30 * 24 * 60 * 60 * 1000,
    };
    let log = SharedVisibilityLog::default();
    log.record(
        &tree,
        &Antichain::from_elem(Timestamp::from(1001)),
        1005,
        &config,
    );
    log.record(&tree, &Antichain::new(), 2005, &config);
    // Later observations, which prune by age, keep the close.
    log.record(&tree, &Antichain::new(), 10_000_000, &config);
    let copy = log.input_log(Some(5_000_000));
    assert_eq!(copy.visible_at(5_000_001), Some(2005));
    assert_eq!(copy.visible_at(u64::MAX), Some(2005));
}

#[mz_ore::test]
fn end_to_end_only_records_no_local_rows() {
    let tree = ResolvedTree::Leaf(Ticker::Aligned { interval_ms: 1000 });
    let config = RecorderConfig {
        scheme: BucketScheme::new(4).expect("power of two"),
        interval_ms: 1,
        period_ms: 60_000,
        retention_ms: 30 * 24 * 60 * 60 * 1000,
    };
    let frontier = |ms: u64| Antichain::from_elem(Timestamp::from(ms));
    let mut recording = FreshnessRecording::created(frontier(0), 0).end_to_end_only();
    let mut rows = recording.observe(&tree, &tree, &frontier(90_001), 90_005, &[], &config);
    assert_eq!(recording.recorded_through(), Some(90_000));
    rows.extend(recording.finish());
    assert!(
        rows.iter()
            .all(|(measure, _)| *measure == Measure::EndToEnd),
        "{rows:?}"
    );
    assert_eq!(rows.iter().map(|(_, row)| row.count).sum::<u64>(), 90_000);
}
