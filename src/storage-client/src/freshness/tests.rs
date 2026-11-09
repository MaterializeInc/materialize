// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use mz_cluster_client::ReplicaId;
use mz_controller_types::dyncfgs::{
    ENABLE_WALLCLOCK_FRESHNESS_RECORDING, WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE,
    WALLCLOCK_FRESHNESS_PERIOD_INTERVAL,
};
use mz_dyncfg::{ConfigSet, ConfigUpdates};
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Timestamp};
use mz_storage_types::freshness::{BucketScheme, PeriodRow, RecorderConfig};
use mz_storage_types::time_dependence::TimeDependence;
use timely::progress::Antichain;

use super::{FreshnessState, MIN_PERIOD_INTERVAL, is_new_rejection, pack_rows, recorder_config};
use crate::healthcheck::{
    REPLICA_WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC, WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC,
};

fn ts(n: u64) -> Timestamp {
    Timestamp::from(n)
}

fn frontier(n: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(ts(n))
}

fn config() -> RecorderConfig {
    RecorderConfig {
        scheme: BucketScheme::new(4).expect("power of two"),
        period: Duration::from_secs(60),
    }
}

fn bucket(staleness_ms: u64) -> i32 {
    config().scheme.bucket(Duration::from_millis(staleness_ms))
}

/// The rows grouped by period, with the count per bucket.
fn periods(rows: &[PeriodRow]) -> BTreeMap<(u64, u64), BTreeMap<i32, u64>> {
    let mut periods: BTreeMap<_, BTreeMap<_, _>> = BTreeMap::new();
    for row in rows {
        let period = (u64::from(row.period_start), u64::from(row.period_end));
        let count = periods
            .entry(period)
            .or_default()
            .entry(row.bucket)
            .or_default();
        *count += row.count_ms;
    }
    periods
}

#[mz_ore::test]
fn flag_off_then_on_restarts_at_now() {
    let cfg = config();
    let mut state = FreshnessState::new(TimeDependence::default(), ts(0), &frontier(1), Some(&cfg));
    assert_eq!(
        state.observe(ts(1_000), &frontier(1_001), Some(&cfg)),
        vec![]
    );
    // Turning the flag off drops the open period without emitting it.
    assert_eq!(state.tick(ts(2_000), None), vec![]);
    assert_eq!(state.observe(ts(3_000), &frontier(1_001), None), vec![]);
    // Turning it on again starts at `now`, so no sample spans the off time.
    assert_eq!(state.tick(ts(10_000), Some(&cfg)), vec![]);
    let rows = state.finish(ts(12_000), Some(&cfg));
    let expected = BTreeMap::from([((10_000, 12_000), BTreeMap::from([(bucket(11_000), 2_000)]))]);
    assert_eq!(periods(&rows), expected);
}

#[mz_ore::test]
fn flag_on_after_construction_starts_at_now() {
    let cfg = config();
    let mut state = FreshnessState::new(TimeDependence::default(), ts(0), &frontier(1), None);
    assert_eq!(state.tick(ts(30_000), Some(&cfg)), vec![]);
    let rows = state.tick(ts(60_000), Some(&cfg));
    let expected = BTreeMap::from([((30_000, 60_000), BTreeMap::from([(bucket(60_000), 30_000)]))]);
    assert_eq!(periods(&rows), expected);
}

#[mz_ore::test]
fn reflected_none_maps_to_recorder_start() {
    let cfg = config();
    // A frontier at the minimum reflects nothing. The reporter counts staleness from the
    // start, a recorder created later from its own start.
    let mut state = FreshnessState::new(TimeDependence::default(), ts(0), &frontier(0), None);
    assert_eq!(state.tick(ts(10_000), Some(&cfg)), vec![]);
    assert_eq!(state.reported_lag(ts(13_000)), Duration::from_secs(13));
    let rows = state.finish(ts(13_000), Some(&cfg));
    let expected = BTreeMap::from([((10_000, 13_000), BTreeMap::from([(bucket(3_000), 3_000)]))]);
    assert_eq!(periods(&rows), expected);
}

#[mz_ore::test]
fn empty_frontier_finishes_once() {
    let cfg = config();
    let mut state = FreshnessState::new(TimeDependence::default(), ts(0), &frontier(1), Some(&cfg));
    let rows = state.observe(ts(5_000), &Antichain::new(), Some(&cfg));
    let expected = BTreeMap::from([((0, 5_000), BTreeMap::from([(bucket(5_000), 5_000)]))]);
    assert_eq!(periods(&rows), expected);
    assert_eq!(state.reported_lag(ts(6_000)), Duration::ZERO);
    assert_eq!(
        state.observe(ts(7_000), &frontier(7_001), Some(&cfg)),
        vec![]
    );
    assert_eq!(state.tick(ts(120_000), Some(&cfg)), vec![]);
    assert_eq!(state.finish(ts(130_000), Some(&cfg)), vec![]);
    assert_eq!(state.reported_lag(ts(140_000)), Duration::ZERO);
}

#[mz_ore::test]
fn finish_emits_partial_period_once() {
    let cfg = config();
    let mut state = FreshnessState::new(TimeDependence::default(), ts(0), &frontier(1), Some(&cfg));
    let rows = state.finish(ts(5_000), Some(&cfg));
    assert_eq!(periods(&rows).keys().collect::<Vec<_>>(), [&(0, 5_000)]);
    assert_eq!(state.finish(ts(6_000), Some(&cfg)), vec![]);
    assert_eq!(state.reported_lag(ts(6_000)), Duration::ZERO);
}

#[mz_ore::test]
fn refresh_view_reads_last_refresh() {
    let cfg = config();
    let every_minute = TimeDependence::new(
        Some(RefreshSchedule {
            everies: vec![RefreshEvery {
                interval: Duration::from_secs(60),
                aligned_to: ts(0),
            }],
            ats: vec![],
        }),
        vec![],
    );
    // The frontier passed the refresh at 60 s, so the view reflects inputs as of 60 s until
    // its frontier passes the next refresh, however far the frontier advances in between.
    let mut state = FreshnessState::new(every_minute, ts(60_000), &frontier(60_001), Some(&cfg));
    assert_eq!(
        state.observe(ts(90_000), &frontier(90_001), Some(&cfg)),
        vec![]
    );
    assert_eq!(state.reported_lag(ts(100_000)), Duration::from_secs(40));
    let rows = state.observe(ts(120_000), &frontier(120_001), Some(&cfg));
    let expected = BTreeMap::from([(
        (60_000, 120_000),
        BTreeMap::from([(bucket(30_000), 30_000), (bucket(60_000), 30_000)]),
    )]);
    assert_eq!(periods(&rows), expected);
    // The frontier passed the refresh at 120 s, and the next sample no longer reads the
    // staleness of the previous refresh.
    assert_eq!(state.reported_lag(ts(121_000)), Duration::from_secs(60));
    assert_eq!(
        state.observe(ts(122_000), &frontier(122_001), Some(&cfg)),
        vec![]
    );
    assert_eq!(state.reported_lag(ts(123_000)), Duration::from_secs(3));
}

#[mz_ore::test]
fn repeated_frontier_is_one_sample_per_period() {
    let cfg = config();
    let mut state = FreshnessState::new(TimeDependence::default(), ts(0), &frontier(1), Some(&cfg));
    let mut rows = Vec::new();
    // The frontier stalls at 1 for two periods, observed once per second.
    for now in (1_000..=120_000).step_by(1_000) {
        rows.extend(state.observe(ts(now), &frontier(1), Some(&cfg)));
        rows.extend(state.tick(ts(now), Some(&cfg)));
    }
    let expected = BTreeMap::from([
        ((0, 60_000), BTreeMap::from([(bucket(60_000), 60_000)])),
        (
            (60_000, 120_000),
            BTreeMap::from([(bucket(120_000), 60_000)]),
        ),
    ]);
    assert_eq!(periods(&rows), expected);
    assert_eq!(state.reported_lag(ts(120_000)), Duration::from_secs(120));
}

#[mz_ore::test]
fn reflected_none_maps_to_start() {
    let cfg = config();
    // A frontier at the minimum reflects nothing, so staleness counts from the start.
    let mut state = FreshnessState::new(
        TimeDependence::default(),
        ts(1_000),
        &frontier(0),
        Some(&cfg),
    );
    assert_eq!(state.reported_lag(ts(4_000)), Duration::from_secs(3));
    let rows = state.finish(ts(4_000), Some(&cfg));
    let expected = BTreeMap::from([((1_000, 4_000), BTreeMap::from([(bucket(3_000), 3_000)]))]);
    assert_eq!(periods(&rows), expected);
}

#[mz_ore::test]
fn created_state_charges_at_most_since_start() {
    let cfg = config();
    // A table created at 100 s starts with a frontier from long before, the txns upper at boot.
    let start = 100_000;
    let mut state = FreshnessState::new_created(
        TimeDependence::default(),
        ts(start),
        &frontier(1),
        Some(&cfg),
    );
    assert_eq!(state.reported_lag(ts(100_500)), Duration::from_millis(500));
    // Its first advance, and later ones that still reflect a time before its creation.
    let mut rows = state.observe(ts(100_800), &frontier(2), Some(&cfg));
    rows.extend(state.observe(ts(101_000), &frontier(100_901), Some(&cfg)));
    rows.extend(state.finish(ts(101_100), Some(&cfg)));
    let expected = BTreeMap::from([(
        (start, 101_100),
        BTreeMap::from([(bucket(800), 800), (bucket(1_000), 200), (bucket(200), 100)]),
    )]);
    assert_eq!(periods(&rows), expected);
    for row in &rows {
        assert!(
            row.bucket <= bucket(1_100),
            "charged more than since start: {row:?}"
        );
    }
}

#[mz_ore::test]
fn resume_charges_nothing_for_unobserved_time() {
    let cfg = config();
    let mut state = FreshnessState::new(TimeDependence::default(), ts(0), &frontier(1), None);
    assert_eq!(state.reported_lag(ts(1_000)), Duration::from_secs(1));
    // Nobody observed the frontier from 1 s to 50 s, when recording turns on.
    assert_eq!(
        state.resume(ts(50_000), &frontier(49_001), Some(&cfg)),
        vec![]
    );
    assert_eq!(state.reported_lag(ts(50_000)), Duration::from_secs(1));
    let rows = state.finish(ts(52_000), Some(&cfg));
    let expected = BTreeMap::from([((50_000, 52_000), BTreeMap::from([(bucket(3_000), 2_000)]))]);
    assert_eq!(periods(&rows), expected);
}

#[mz_ore::test]
fn resume_with_empty_frontier_completes() {
    let cfg = config();
    let mut state = FreshnessState::new(TimeDependence::default(), ts(0), &frontier(1), None);
    assert_eq!(
        state.resume(ts(50_000), &Antichain::new(), Some(&cfg)),
        vec![]
    );
    assert_eq!(state.reported_lag(ts(60_000)), Duration::ZERO);
    assert_eq!(
        state.observe(ts(61_000), &frontier(61_001), Some(&cfg)),
        vec![]
    );
    assert_eq!(state.finish(ts(62_000), Some(&cfg)), vec![]);
}

fn period_row(start: u64, bucket: i32, count_ms: u64) -> PeriodRow {
    PeriodRow {
        period_start: ts(start),
        period_end: ts(start + 60_000),
        buckets_per_octave: 4,
        bucket,
        count_ms,
    }
}

fn column_names(desc: &RelationDesc) -> Vec<&str> {
    desc.iter_names().map(|name| name.as_str()).collect()
}

fn millis(datum: Datum) -> i64 {
    match datum {
        Datum::TimestampTz(t) => t.timestamp_millis(),
        other => panic!("not a timestamp: {other:?}"),
    }
}

#[mz_ore::test]
fn pack_rows_layout() {
    let id = GlobalId::User(7);
    let labels = BTreeMap::from([("workload_class", "production".to_string())]);
    let rows = vec![
        period_row(60_000, 3, 1_500),
        period_row(120_000, -2, 58_500),
    ];

    let global = pack_rows(rows.clone(), id, None, &labels);
    assert_eq!(
        column_names(&WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC),
        [
            "period_start",
            "period_end",
            "object_id",
            "measure",
            "buckets_per_octave",
            "bucket",
            "labels"
        ]
    );
    assert_eq!(global.len(), 2);
    let (row, diff) = &global[0];
    let datums = row.unpack();
    assert_eq!(datums.len(), WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC.arity());
    assert_eq!(millis(datums[0]), 60_000);
    assert_eq!(millis(datums[1]), 120_000);
    assert_eq!(datums[2], Datum::String("u7"));
    assert_eq!(datums[3], Datum::String("end_to_end"));
    assert_eq!(datums[4], Datum::UInt32(4));
    assert_eq!(datums[5], Datum::Int32(3));
    let Datum::Map(map) = datums[6] else {
        panic!("labels are not a map: {:?}", datums[6]);
    };
    assert_eq!(
        map.iter().collect::<Vec<_>>(),
        [("workload_class", Datum::String("production"))]
    );
    assert_eq!(*diff, Diff::from(1_500));
    assert_eq!(global[1].1, Diff::from(58_500));
    assert_eq!(global[1].0.unpack()[5], Datum::Int32(-2));

    let replica = pack_rows(rows, id, Some(ReplicaId::User(2)), &labels);
    assert_eq!(
        column_names(&REPLICA_WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC),
        [
            "period_start",
            "period_end",
            "object_id",
            "replica_id",
            "measure",
            "buckets_per_octave",
            "bucket",
            "labels"
        ]
    );
    let (row, diff) = &replica[0];
    let datums = row.unpack();
    assert_eq!(
        datums.len(),
        REPLICA_WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC.arity()
    );
    assert_eq!(datums[2], Datum::String("u7"));
    assert_eq!(datums[3], Datum::String("u2"));
    assert_eq!(datums[4], Datum::String("end_to_end"));
    assert_eq!(datums[6], Datum::Int32(3));
    assert_eq!(*diff, Diff::from(1_500));
}

#[mz_ore::test]
fn recorder_config_reads_dyncfgs() {
    let dyncfg = ConfigSet::default()
        .add(&ENABLE_WALLCLOCK_FRESHNESS_RECORDING)
        .add(&WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE)
        .add(&WALLCLOCK_FRESHNESS_PERIOD_INTERVAL);
    assert_eq!(recorder_config(&dyncfg), None);
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_WALLCLOCK_FRESHNESS_RECORDING, true);
    updates.add(&WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE, 8);
    updates.add(
        &WALLCLOCK_FRESHNESS_PERIOD_INTERVAL,
        Duration::from_secs(10),
    );
    updates.apply(&dyncfg);
    let expected = RecorderConfig {
        scheme: BucketScheme::new(8).expect("power of two"),
        period: Duration::from_secs(10),
    };
    assert_eq!(recorder_config(&dyncfg), Some(expected));
}

#[mz_ore::test]
fn recorder_config_rejects_invalid_values() {
    let dyncfg = ConfigSet::default()
        .add(&ENABLE_WALLCLOCK_FRESHNESS_RECORDING)
        .add(&WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE)
        .add(&WALLCLOCK_FRESHNESS_PERIOD_INTERVAL);
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_WALLCLOCK_FRESHNESS_RECORDING, true);
    updates.add(&WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE, 3);
    updates.add(&WALLCLOCK_FRESHNESS_PERIOD_INTERVAL, Duration::ZERO);
    updates.apply(&dyncfg);
    let default = *WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE.default();
    let expected = RecorderConfig {
        scheme: BucketScheme::new(default).expect("power of two"),
        period: MIN_PERIOD_INTERVAL,
    };
    // Repeated reads of the same rejected values return the same fallback.
    assert_eq!(recorder_config(&dyncfg), Some(expected));
    assert_eq!(recorder_config(&dyncfg), Some(expected));
    let mut updates = ConfigUpdates::default();
    updates.add(
        &WALLCLOCK_FRESHNESS_PERIOD_INTERVAL,
        Duration::from_millis(999),
    );
    updates.apply(&dyncfg);
    assert_eq!(recorder_config(&dyncfg), Some(expected));
}

#[mz_ore::test]
fn rejections_are_new_once_per_value() {
    let last = AtomicU64::new(u64::MAX);
    assert!(is_new_rejection(&last, 3));
    assert!(!is_new_rejection(&last, 3));
    assert!(is_new_rejection(&last, 5));
    assert!(is_new_rejection(&last, 3));
}
