// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use mz_cluster_client::{ReplicaId, WallclockLagFn};
use mz_controller_types::dyncfgs::WALLCLOCK_FRESHNESS_RESTART_POLICY;
use mz_dyncfg::ConfigUpdates;
use mz_ore::now::NowFn;
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::freshness::histogram::BucketScheme;
use mz_storage_types::freshness::recorder::{PeriodRow, RecorderConfig};
use mz_storage_types::freshness::{ResolvedTree, Ticker};
use timely::progress::Antichain;

use super::{
    AssignmentBoundCache, FreshnessRecording, FreshnessSeeds, Measure, RestartPolicy,
    SeedCollector, SharedVisibilityLog, pack_rows, recording_after_restart, reported_lag,
};
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

const T0: u64 = 1_800_000_000_000;

fn recorder_config() -> RecorderConfig {
    RecorderConfig {
        scheme: BucketScheme::new(4).expect("power of two"),
        interval_ms: 1,
        period_ms: 60_000,
        retention_ms: RETENTION_MS,
    }
}

const RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// A row of `measure` for each of `buckets` in period `(end - 60 s, end]`.
fn period_rows(measure: Measure, end: u64, buckets: &[i32]) -> Vec<(Measure, PeriodRow)> {
    buckets
        .iter()
        .map(|bucket| {
            let row = PeriodRow {
                period_start_ms: end - 60_000,
                period_end_ms: end,
                buckets_per_octave: 4,
                bucket: *bucket,
                count: 10,
            };
            (measure, row)
        })
        .collect()
}

fn collect_into(collector: &mut SeedCollector, rows: &[(Row, Diff)], diff: StorageDiff) {
    for (row, _) in rows {
        collector.add(&row.unpack(), diff);
    }
}

#[mz_ore::test]
fn seeds_are_the_maximum_period_end_per_key() {
    let (a, b) = (GlobalId::User(1), GlobalId::User(2));
    let r1 = ReplicaId::User(1);
    let labels = BTreeMap::new();
    let pack = |rows: Vec<Vec<(Measure, PeriodRow)>>, id, replica| {
        pack_rows(rows.into_iter().flatten().collect(), id, replica, &labels)
    };

    let mut global = SeedCollector::new(false);
    let rows_a = pack(
        vec![
            period_rows(Measure::EndToEnd, T0 + 120_000, &[3, 4]),
            period_rows(Measure::EndToEnd, T0 + 60_000, &[3]),
            period_rows(Measure::Local, T0 + 60_000, &[0]),
        ],
        a,
        None,
    );
    let rows_b = pack(
        vec![period_rows(Measure::Local, T0 + 240_000, &[1])],
        b,
        None,
    );
    collect_into(&mut global, &rows_a, 1);
    collect_into(&mut global, &rows_b, 1);
    // Rows that are not in the collection do not seed.
    let absent = pack(
        vec![period_rows(Measure::EndToEnd, T0 + 600_000, &[3])],
        b,
        None,
    );
    collect_into(&mut global, &absent, 0);
    collect_into(&mut global, &absent, -1);
    assert_eq!(
        global.finish(),
        BTreeMap::from([
            ((a, None, Measure::EndToEnd), T0 + 120_000),
            ((a, None, Measure::Local), T0 + 60_000),
            ((b, None, Measure::Local), T0 + 240_000),
        ]),
        "global"
    );

    let mut replica = SeedCollector::new(true);
    let rows = pack(
        vec![
            period_rows(Measure::EndToEnd, T0 + 180_000, &[3]),
            period_rows(Measure::Local, T0 + 180_000, &[0]),
            period_rows(Measure::Local, T0 + 120_000, &[0]),
        ],
        a,
        Some(r1),
    );
    collect_into(&mut replica, &rows, 1);
    assert_eq!(
        replica.finish(),
        BTreeMap::from([
            ((a, Some(r1), Measure::EndToEnd), T0 + 180_000),
            ((a, Some(r1), Measure::Local), T0 + 180_000),
        ]),
        "per replica"
    );
}

/// The summed count of the rows of each measure and period end.
fn period_counts(rows: Vec<(Measure, PeriodRow)>) -> BTreeMap<(Measure, u64), u64> {
    let mut counts = BTreeMap::new();
    for (measure, row) in rows {
        *counts.entry((measure, row.period_end_ms)).or_default() += row.count;
    }
    counts
}

#[mz_ore::test]
fn charged_recordings_start_at_their_seeds() {
    let tree = aligned(1000);
    let config = recorder_config();
    let (a, b) = (GlobalId::User(1), GlobalId::User(2));
    let r1 = ReplicaId::User(1);
    let seeds = FreshnessSeeds::default();
    let observe = |recording: &mut FreshnessRecording, reflected: u64| {
        let rows = recording.observe(
            &tree,
            &tree,
            &frontier(reflected + 1),
            reflected + 5,
            &[],
            &config,
        );
        period_counts(rows)
    };

    let mut global_a = FreshnessRecording::seeded(seeds.clone(), a, None, T0);
    assert_eq!(observe(&mut global_a, T0 + 120_000), BTreeMap::new());
    assert_eq!(
        global_a.recorded_through(),
        None,
        "waits for the global seeds"
    );

    seeds.publish(
        false,
        BTreeMap::from([
            ((a, None, Measure::EndToEnd), T0 + 60_000),
            ((a, None, Measure::Local), T0 + 60_000),
        ]),
    );
    assert_eq!(
        observe(&mut global_a, T0 + 180_000),
        BTreeMap::from([
            ((Measure::EndToEnd, T0 + 120_000), 60_000),
            ((Measure::EndToEnd, T0 + 180_000), 60_000),
            ((Measure::Local, T0 + 120_000), 60_000),
            ((Measure::Local, T0 + 180_000), 60_000),
        ]),
        "the arrivals since the seed count at the first observation"
    );

    // A later recording of `a` would count its arrivals twice if it reused the seed.
    for id in [a, b] {
        let mut recording = FreshnessRecording::seeded(seeds.clone(), id, None, T0);
        assert_eq!(
            observe(&mut recording, T0 + 180_000),
            BTreeMap::new(),
            "{id}"
        );
        assert_eq!(
            recording.recorded_through(),
            Some(T0 + 180_000),
            "{id} starts at its first observation"
        );
    }

    let mut replica_a = FreshnessRecording::seeded(seeds.clone(), a, Some(r1), T0);
    assert_eq!(observe(&mut replica_a, T0 + 120_000), BTreeMap::new());
    assert_eq!(
        replica_a.recorded_through(),
        None,
        "waits for the replica seeds"
    );
    seeds.publish(
        true,
        BTreeMap::from([((a, Some(r1), Measure::EndToEnd), T0 + 60_000)]),
    );
    assert_eq!(
        observe(&mut replica_a, T0 + 120_000),
        BTreeMap::from([((Measure::EndToEnd, T0 + 120_000), 60_000)]),
        "a measure without a seed starts at the first observation"
    );
}

#[mz_ore::test]
fn restart_policy_selects_the_recording() {
    let a = GlobalId::User(1);
    let tree = aligned(1000);
    let config = recorder_config();
    for (policy, expected, expected_count) in [
        ("hole", RestartPolicy::Hole, 0),
        ("charge", RestartPolicy::Charge, 120_000),
        ("bogus", RestartPolicy::Hole, 0),
    ] {
        let dyncfg = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&WALLCLOCK_FRESHNESS_RESTART_POLICY, policy);
        updates.apply(&dyncfg);
        assert_eq!(RestartPolicy::get(&dyncfg), expected, "{policy}");

        let seeds = FreshnessSeeds::default();
        seeds.publish(
            false,
            BTreeMap::from([
                ((a, None, Measure::EndToEnd), T0 + 60_000),
                ((a, None, Measure::Local), T0 + 60_000),
            ]),
        );
        let mut recording = recording_after_restart(&dyncfg, &seeds, a, None, T0);
        let rows = recording.observe(
            &tree,
            &tree,
            &frontier(T0 + 120_001),
            T0 + 120_005,
            &[],
            &config,
        );
        let counted: u64 = rows.iter().map(|(_, row)| row.count).sum();
        assert_eq!(counted, expected_count, "{policy}");
    }
}

#[mz_ore::test]
fn seeded_recording_debug_stays_small() {
    let seeds = FreshnessSeeds::default();
    let many = (0..10_000).map(|i| ((GlobalId::User(i), None, Measure::EndToEnd), T0));
    seeds.publish(false, many.collect());
    let recording = FreshnessRecording::seeded(seeds, GlobalId::User(0), None, T0);
    let debug = format!("{recording:?}");
    assert!(debug.contains("seeds: 10000"), "{debug}");
    assert!(debug.len() < 1024, "{} bytes: {debug}", debug.len());
}

/// The summed counts per measure.
fn measure_totals(rows: &[(Measure, PeriodRow)]) -> BTreeMap<Measure, u64> {
    let mut totals = BTreeMap::new();
    for (measure, row) in rows {
        *totals.entry(*measure).or_default() += row.count;
    }
    totals
}

#[mz_ore::test]
fn hole_starts_no_earlier_than_the_recording() {
    let tree = aligned(1000);
    let config = recorder_config();
    let seeds = FreshnessSeeds::default();
    let dyncfg = mz_dyncfgs::all_dyncfgs();
    let mut updates = ConfigUpdates::default();
    updates.add(&WALLCLOCK_FRESHNESS_RESTART_POLICY, "hole");
    updates.apply(&dyncfg);
    let mut recording = recording_after_restart(&dyncfg, &seeds, GlobalId::User(1), None, T0);
    // A first frontier near the epoch, as of a collection whose as-of is `[0]`, starts nothing
    // before the recording was built.
    let rows = recording.observe(&tree, &tree, &frontier(1001), T0, &[], &config);
    assert_eq!(rows, vec![]);
    assert_eq!(recording.recorded_through(), Some(T0));
    let rows = recording.observe(
        &tree,
        &tree,
        &frontier(T0 + 60_001),
        T0 + 60_005,
        &[],
        &config,
    );
    assert_eq!(
        period_counts(rows),
        BTreeMap::from([
            ((Measure::EndToEnd, T0 + 60_000), 60_000),
            ((Measure::Local, T0 + 60_000), 60_000),
        ])
    );
}

#[mz_ore::test]
fn charge_starts_no_earlier_than_the_retention() {
    const DAY_MS: u64 = 24 * 60 * 60 * 1000;
    let tree = aligned(1000);
    let config = recorder_config();
    let a = GlobalId::User(1);
    let seeds = FreshnessSeeds::default();
    // The end-to-end seed is older than the retention, as when the recording starts long after
    // the seeds were published. The local one is missing.
    seeds.publish(
        false,
        BTreeMap::from([((a, None, Measure::EndToEnd), T0 - 40 * DAY_MS)]),
    );
    let mut recording = FreshnessRecording::seeded(seeds, a, None, T0 - 1000);
    let rows = recording.observe(&tree, &tree, &frontier(T0 + 1), T0 + 5, &[], &config);
    // End to end catches up the retention in about a period per day. Local starts like `hole`,
    // at the later of the first frontier and the recording, which closes no period yet.
    let periods: BTreeMap<(u64, u64), u64> = rows
        .iter()
        .filter(|(measure, _)| *measure == Measure::EndToEnd)
        .map(|(_, row)| ((row.period_start_ms, row.period_end_ms), row.count))
        .fold(BTreeMap::new(), |mut periods, (key, count)| {
            *periods.entry(key).or_default() += count;
            periods
        });
    assert!(periods.len() <= 32, "{} periods", periods.len());
    let first_end = periods.keys().next().expect("rows").1;
    assert!(first_end > T0 + 5 - RETENTION_MS, "{first_end}");
    assert_eq!(
        measure_totals(&rows),
        BTreeMap::from([(Measure::EndToEnd, RETENTION_MS - 5)])
    );
    assert_eq!(recording.recorded_through(), Some(T0));
}

#[mz_ore::test]
fn end_to_end_only_recording_needs_no_local_seed() {
    let tree = aligned(1000);
    let config = recorder_config();
    let a = GlobalId::User(1);
    let seeds = FreshnessSeeds::default();
    seeds.publish(
        false,
        BTreeMap::from([((a, None, Measure::EndToEnd), T0 + 60_000)]),
    );
    let mut recording = FreshnessRecording::seeded(seeds, a, None, T0).end_to_end_only();
    let rows = recording.observe(
        &tree,
        &tree,
        &frontier(T0 + 120_001),
        T0 + 120_005,
        &[],
        &config,
    );
    assert_eq!(
        period_counts(rows),
        BTreeMap::from([((Measure::EndToEnd, T0 + 120_000), 60_000)])
    );
}
