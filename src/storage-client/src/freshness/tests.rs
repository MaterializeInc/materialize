// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_cluster_client::WallclockLagFn;
use mz_ore::now::NowFn;
use mz_repr::Timestamp;
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use mz_storage_types::freshness::histogram::BucketScheme;
use mz_storage_types::freshness::recorder::RecorderConfig;
use mz_storage_types::freshness::{ResolvedTree, Ticker};
use timely::progress::Antichain;

use super::{AssignmentBoundCache, FreshnessRecording, Measure, SharedVisibilityLog, reported_lag};
use crate::controller::WallclockLag;

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

fn aligned(ms: u64) -> ResolvedTree {
    ResolvedTree::Leaf(Ticker::Aligned { interval_ms: ms })
}

fn refresh_every(ms: u64, input: ResolvedTree) -> ResolvedTree {
    ResolvedTree::Refresh {
        schedule: RefreshSchedule {
            everies: vec![RefreshEvery {
                interval: Duration::from_millis(ms),
                aligned_to: Timestamp::from(0u64),
            }],
            ats: vec![],
        },
        input: Box::new(input),
    }
}

fn refresh_at_only(ms: u64, input: ResolvedTree) -> ResolvedTree {
    ResolvedTree::Refresh {
        schedule: RefreshSchedule {
            everies: vec![],
            ats: vec![Timestamp::from(ms)],
        },
        input: Box::new(input),
    }
}

fn frontier(ms: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::from(ms))
}

/// The lag a controller reports for `tree`, with the reporting flag set to `reporting`.
struct Reporter(AssignmentBoundCache);

impl Reporter {
    fn new() -> Self {
        Reporter(AssignmentBoundCache::default())
    }

    fn lag(
        &mut self,
        reporting: bool,
        tree: Option<&ResolvedTree>,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        today: Duration,
    ) -> WallclockLag {
        let freshness = tree
            .filter(|_| reporting)
            .and_then(|tree| Some((tree, self.0.get(tree)?)));
        reported_lag(freshness, frontier, now_ms, today)
    }
}

/// The lag of `frontier` at `now_ms` as reported without the reporting flag.
fn ceiled(frontier: &Antichain<Timestamp>, now_ms: u64) -> Duration {
    let lag_fn = WallclockLagFn::new(NowFn::from(move || now_ms));
    frontier
        .as_option()
        .map_or(Duration::ZERO, |ts| lag_fn(*ts))
}

#[mz_ore::test]
fn reporting_off_reports_todays_whole_seconds() {
    let trees = [
        None,
        Some(aligned(1000)),
        Some(refresh_every(60_000, aligned(1000))),
        Some(ResolvedTree::Min(vec![aligned(7000), aligned(11000)])),
        Some(refresh_at_only(60_000, aligned(1000))),
    ];
    let frontiers = [
        Antichain::new(),
        frontier(0),
        frontier(5_001),
        frontier(120_000),
        frontier(1_000_000),
    ];
    let mut lag = Reporter::new();
    for tree in &trees {
        for frontier in &frontiers {
            for now_ms in [0, 5_005, 61_000, 119_999, 200_123, 1_000_000] {
                let today = ceiled(frontier, now_ms);
                let reported = lag.lag(false, tree.as_ref(), frontier, now_ms, today);
                let expected = u64::try_from(today.as_millis()).expect("fits");
                assert_eq!(
                    reported,
                    WallclockLag::Millis(expected),
                    "tree {tree:?}, frontier {frontier:?}, now {now_ms}"
                );
                assert_eq!(expected % 1000, 0, "whole seconds");
            }
        }
    }
}

#[mz_ore::test]
fn reporting_on_reports_max_of_bound_and_staleness() {
    let mut lag = Reporter::new();

    // A 1 s source that just ticked reports its bound.
    let source = aligned(1000);
    let d = source.assignment_bound().expect("bounded");
    assert!(d < 2_000, "1 s source bound {d}");
    let f = frontier(5_001);
    let reported = lag.lag(true, Some(&source), &f, 5_005, ceiled(&f, 5_005));
    assert_eq!(reported, WallclockLag::Millis(d));

    // The frontier of a view refreshed every minute is at its next refresh, so its ceiled lag
    // reads 0 while its bound is about a minute.
    let view = refresh_every(60_000, aligned(1000));
    let d = view.assignment_bound().expect("bounded");
    assert!(d >= 60_000, "refresh bound {d}");
    let f = frontier(120_000);
    assert_eq!(ceiled(&f, 61_000), Duration::ZERO);
    let reported = lag.lag(true, Some(&view), &f, 61_000, ceiled(&f, 61_000));
    assert_eq!(reported, WallclockLag::Millis(d));

    // A stalled view reports its staleness once that exceeds the bound.
    let reflected = view
        .last_reflected(Timestamp::from(120_000u64))
        .expect("reflected");
    let now_ms = 1_000_000;
    let reported = lag.lag(true, Some(&view), &f, now_ms, ceiled(&f, now_ms));
    assert_eq!(reported, WallclockLag::Millis(now_ms - reflected));
}

#[mz_ore::test]
fn reporting_on_falls_back_to_todays_value() {
    let mut lag = Reporter::new();
    let f = frontier(5_001);
    let today = ceiled(&f, 7_500);
    assert_eq!(today, Duration::from_secs(3));
    let expected = WallclockLag::Millis(3_000);

    assert_eq!(lag.lag(true, None, &f, 7_500, today), expected, "no tree");

    let at_only = refresh_at_only(60_000, aligned(1000));
    assert_eq!(at_only.assignment_bound(), None);
    assert_eq!(
        lag.lag(true, Some(&at_only), &f, 7_500, today),
        expected,
        "unbounded"
    );

    let view = refresh_every(60_000, aligned(1000));
    let f0 = frontier(0);
    assert_eq!(view.last_reflected(Timestamp::from(0u64)), None);
    assert_eq!(
        lag.lag(true, Some(&view), &f0, 7_500, ceiled(&f0, 7_500)),
        WallclockLag::Millis(8_000),
        "nothing reflected"
    );

    assert_eq!(
        lag.lag(true, Some(&view), &Antichain::new(), 7_500, Duration::ZERO),
        WallclockLag::Millis(0),
        "empty frontier"
    );
}

#[mz_ore::test]
fn bound_follows_changed_tickers() {
    let mut lag = Reporter::new();
    let f = frontier(10_001);
    let now_ms = 10_002;
    let today = ceiled(&f, now_ms);
    let fast = aligned(1000);
    let slow = aligned(5000);
    let fast_d = fast.assignment_bound().expect("bounded");
    let slow_d = slow.assignment_bound().expect("bounded");
    assert!(fast_d < slow_d);
    for (tree, d) in [(&fast, fast_d), (&slow, slow_d), (&fast, fast_d)] {
        assert_eq!(
            lag.lag(true, Some(tree), &f, now_ms, today),
            WallclockLag::Millis(d)
        );
    }
}
