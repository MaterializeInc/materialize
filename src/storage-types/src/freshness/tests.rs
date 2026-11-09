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

use mz_ore::cast::CastLossy;
use mz_repr::Timestamp;
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use proptest::prelude::*;
use proptest::strategy::Union;

use super::{
    BucketScheme, PeriodRow, RecorderConfig, StalenessRecorder, StalenessReporter, reflected_time,
};
use crate::time_dependence::TimeDependence;

fn scheme(b: u32) -> BucketScheme {
    BucketScheme::new(b).expect("valid bucket count")
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

#[mz_ore::test]
fn bucket_scheme_validates_per_octave() {
    for b in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
        assert_eq!(scheme(b).per_octave(), b);
    }
    for b in [0, 3, 5, 6, 257, 512] {
        assert!(BucketScheme::new(b).is_none(), "{b}");
    }
}

#[mz_ore::test]
fn bucket_boundaries() {
    let s = scheme(4);
    assert_eq!(s.bucket(ms(1)), 0);
    assert_eq!(s.bucket(ms(2)), 4);
    assert_eq!(s.bucket(ms(1024)), 40);
    assert_eq!(s.bucket(ms(1025)), 41);
    for x in 1..10_000u64 {
        let k = s.bucket(ms(x));
        let x = f64::cast_lossy(x);
        assert!(s.upper_bound_ms(k - 1) < x, "{x} above bucket {k}");
        assert!(x <= s.upper_bound_ms(k), "{x} below bucket {k}");
    }
}

#[mz_ore::test]
fn bucket_exact_at_powers_of_two() {
    for b in [1, 2, 4, 8, 16, 64, 256] {
        let s = scheme(b);
        let bi = i32::try_from(b).unwrap();
        for e in 0..64i32 {
            let x = 1u64 << e;
            assert_eq!(s.bucket(ms(x)), e * bi, "b={b} 2^{e}");
            if e > 0 {
                // One past the bound is in the next bucket or, for a coarse scheme, a later one
                // of the same octave.
                assert!(s.bucket(ms(x + 1)) > e * bi, "b={b} 2^{e}+1");
                assert!(s.bucket(ms(x + 1)) <= (e + 1) * bi, "b={b} 2^{e}+1");
            }
            if e > 1 {
                // Just below 2^e, the top of the octave: the bucket is at most 2^e's.
                assert!(s.bucket(ms(x - 1)) <= e * bi, "b={b} 2^{e}-1");
                assert!(s.bucket(ms(x - 1)) > (e - 1) * bi, "b={b} 2^{e}-1");
            }
        }
        assert_eq!(s.bucket(ms(u64::MAX)), 64 * bi);
    }
}

#[mz_ore::test]
fn bucket_bounds_are_inclusive_above() {
    for b in [1, 2, 4, 8] {
        let s = scheme(b);
        assert_eq!(s.bucket(ms(0)), 0, "b={b}");
        assert_eq!(s.bucket(ms(1)), 0, "b={b}");
        assert_eq!(s.bucket(ms(2)), i32::try_from(b).unwrap(), "b={b}");
        for k in 1..=64 {
            let bound = s.upper_bound_ms(k);
            // Only bounds that are integers above 1 ms are checked, the others have no integer
            // staleness that sits exactly on the bound.
            if bound > 1.0 && bound == bound.floor() {
                assert_eq!(s.bucket(ms(u64::cast_lossy(bound))), k, "b={b} k={k}");
            }
        }
    }
}

#[mz_ore::test]
fn bucket_rounds_up_to_whole_milliseconds() {
    let s = scheme(4);
    // 1.5 ms is above the 1 ms bound of bucket 0, so it lands with 2 ms.
    assert_eq!(s.bucket(Duration::from_micros(1500)), s.bucket(ms(2)));
    assert_eq!(s.bucket(Duration::from_micros(1001)), s.bucket(ms(2)));
    assert_eq!(s.bucket(Duration::from_nanos(1)), 0);
    assert_eq!(s.bucket(Duration::MAX), s.bucket(ms(u64::MAX)));
}

fn ts(n: u64) -> Timestamp {
    Timestamp::from(n)
}

fn every(interval_ms: u64, aligned_to: u64) -> RefreshSchedule {
    RefreshSchedule {
        everies: vec![RefreshEvery {
            interval: ms(interval_ms),
            aligned_to: ts(aligned_to),
        }],
        ats: vec![],
    }
}

fn refreshed(schedule: RefreshSchedule) -> TimeDependence {
    TimeDependence::new(Some(schedule), vec![])
}

#[mz_ore::test]
fn reflected_wall_clock() {
    let wall_clock = TimeDependence::default();
    assert_eq!(reflected_time(&wall_clock, ts(1000)), Some(ts(999)));
    assert_eq!(reflected_time(&wall_clock, ts(1)), Some(ts(0)));
    assert_eq!(reflected_time(&wall_clock, ts(0)), None);
    let nested = TimeDependence::new(None, vec![TimeDependence::default()]);
    assert_eq!(reflected_time(&nested, ts(1000)), Some(ts(999)));
}

#[mz_ore::test]
fn reflected_refresh_every() {
    let minute = refreshed(every(60_000, 0));
    // At its next refresh, the view still holds the contents of the last one.
    assert_eq!(reflected_time(&minute, ts(120_000)), Some(ts(60_000)));
    assert_eq!(reflected_time(&minute, ts(120_001)), Some(ts(120_000)));
    assert_eq!(reflected_time(&minute, ts(60_001)), Some(ts(60_000)));
}

#[mz_ore::test]
fn reflected_minimum_of_children() {
    let join = TimeDependence::new(
        None,
        vec![refreshed(every(60_000, 0)), refreshed(every(7_000, 0))],
    );
    // The 60 s child last refreshed at 60 s, the 7 s child at 119 s.
    assert_eq!(reflected_time(&join, ts(120_000)), Some(ts(60_000)));
    // At 126 s the 7 s child is behind the 60 s child, which refreshed at 120 s.
    assert_eq!(reflected_time(&join, ts(126_000)), Some(ts(119_000)));
}

#[mz_ore::test]
fn reflected_before_first_refresh() {
    let at = RefreshSchedule {
        everies: vec![],
        ats: vec![ts(100)],
    };
    assert_eq!(reflected_time(&refreshed(at.clone()), ts(100)), None);
    assert_eq!(reflected_time(&refreshed(at), ts(101)), Some(ts(100)));
    // Rounding down saturates at 0, which is not a refresh of this schedule.
    let offset = refreshed(every(60_000, 30_000));
    assert_eq!(reflected_time(&offset, ts(10_000)), None);
    assert_eq!(reflected_time(&offset, ts(30_001)), Some(ts(30_000)));
    // A refreshed child before its first refresh leaves its parent without a reflected time.
    let join = TimeDependence::new(
        None,
        vec![TimeDependence::default(), refreshed(every(60_000, 30_000))],
    );
    assert_eq!(reflected_time(&join, ts(10_000)), None);
}

fn arb_dependence() -> impl Strategy<Value = TimeDependence> {
    let schedule = proptest::option::of(
        (1u64..=120, 0u64..=120_000).prop_map(|(secs, align)| every(secs * 1000, align)),
    );
    let leaf = schedule
        .clone()
        .prop_map(|s| TimeDependence::new(s, vec![]));
    // Depth up to 3, counting the root.
    leaf.prop_recursive(2, 16, 3, move |inner| {
        (schedule.clone(), proptest::collection::vec(inner, 1..=3))
            .prop_map(|(s, children)| TimeDependence::new(s, children))
    })
}

/// The schedules on every root-to-leaf path of `dependence`, innermost last.
fn paths(dependence: &TimeDependence) -> Vec<Vec<&RefreshSchedule>> {
    let mut out: Vec<Vec<&RefreshSchedule>> = if dependence.dependence.is_empty() {
        vec![vec![]]
    } else {
        dependence.dependence.iter().flat_map(paths).collect()
    };
    if let Some(schedule) = &dependence.schedule {
        for path in &mut out {
            path.insert(0, schedule);
        }
    }
    out
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn reflected_time_matches_oracle() {
    proptest!(|(dependence in arb_dependence(), frontier in 0u64..=600_000)| {
        // Brute force: the latest t whose rounding along every path, innermost schedule first,
        // lands below the frontier.
        let paths = paths(&dependence);
        let visible = |t: u64| {
            paths.iter().all(|path| {
                path.iter()
                    .rev()
                    .try_fold(ts(t), |t, schedule| schedule.round_up_timestamp(t))
                    .is_some_and(|t| u64::from(t) < frontier)
            })
        };
        let expected = (0..frontier).rev().find(|t| visible(*t)).map(ts);
        prop_assert_eq!(reflected_time(&dependence, ts(frontier)), expected);
    });
}

fn config(period_ms: u64, per_octave: u32) -> RecorderConfig {
    RecorderConfig {
        scheme: scheme(per_octave),
        period: ms(period_ms),
        retention: Duration::from_secs(30 * 24 * 60 * 60),
    }
}

/// The periods of `rows` in emission order, each with its buckets.
fn periods(rows: &[PeriodRow]) -> Vec<((u64, u64), BTreeMap<i32, u64>)> {
    let mut out: Vec<((u64, u64), BTreeMap<i32, u64>)> = Vec::new();
    for row in rows {
        let key = (u64::from(row.period_start), u64::from(row.period_end));
        if out.last().map(|(k, _)| *k) != Some(key) {
            out.push((key, BTreeMap::new()));
        }
        let (_, buckets) = out.last_mut().expect("pushed");
        let previous = buckets.insert(row.bucket, row.count_ms);
        assert!(previous.is_none(), "bucket {} twice in {key:?}", row.bucket);
    }
    out
}

fn total(buckets: &BTreeMap<i32, u64>) -> u64 {
    buckets.values().sum()
}

#[mz_ore::test]
fn steady_one_second_source() {
    let c = config(60_000, 4);
    let s = c.scheme;
    let mut recorder = StalenessRecorder::new(ts(0), ts(0));
    let mut rows = Vec::new();
    // Each tick is observed 50 ms after it.
    for k in 0..60u64 {
        let observed = recorder.observe(ts(1050 + 1000 * k), ts(1000 * (k + 1)), &c);
        if k < 59 {
            assert_eq!(observed, vec![], "period closed early at k={k}");
        }
        rows.extend(observed);
    }
    let periods = periods(&rows);
    assert_eq!(periods.len(), 1);
    let ((start, end), buckets) = &periods[0];
    assert_eq!((*start, *end), (0, 60_000));
    assert_eq!(total(buckets), 60_000);
    // The last sample is cut at 60000, where its staleness is 1000, not 1050.
    let range = s.bucket(ms(1000))..=s.bucket(ms(1050));
    assert!(buckets.keys().all(|k| range.contains(k)), "{buckets:?}");
    assert!(rows.iter().all(|r| r.buckets_per_octave == 4));
}

fn stalled_minutes(s: BucketScheme, ends: [u64; 3]) -> Vec<((u64, u64), BTreeMap<i32, u64>)> {
    ends.into_iter()
        .map(|end| {
            (
                (end - 60_000, end),
                BTreeMap::from([(s.bucket(ms(end)), 60_000)]),
            )
        })
        .collect()
}

#[mz_ore::test]
fn stall_charges_each_period_at_its_end() {
    let c = config(60_000, 4);
    let mut recorder = StalenessRecorder::new(ts(0), ts(0));
    let mut rows = Vec::new();
    for now in [60_000, 120_000, 180_000] {
        rows.extend(recorder.tick(ts(now), &c));
    }
    assert_eq!(
        periods(&rows),
        stalled_minutes(c.scheme, [60_000, 120_000, 180_000])
    );
}

#[mz_ore::test]
fn stall_without_ticks_charges_each_period_at_its_end() {
    let c = config(60_000, 4);
    let s = c.scheme;
    let mut recorder = StalenessRecorder::new(ts(0), ts(0));
    // One tick after three periods closes all three.
    let rows = recorder.tick(ts(190_000), &c);
    assert_eq!(
        periods(&rows),
        stalled_minutes(s, [60_000, 120_000, 180_000])
    );
    // The rest of the stall continues from the boundary with the same reflected time.
    let rows = recorder.finish(ts(190_000), &c);
    assert_eq!(
        periods(&rows),
        vec![(
            (180_000, 190_000),
            BTreeMap::from([(s.bucket(ms(190_000)), 10_000)])
        )]
    );
}

#[mz_ore::test]
fn frontier_ahead_of_clock_reads_zero() {
    let c = config(60_000, 4);
    let mut recorder = StalenessRecorder::new(ts(0), ts(500));
    let mut rows = Vec::new();
    for k in 1..=150u64 {
        let now = 400 * k;
        rows.extend(recorder.observe(ts(now), ts(now + 500), &c));
    }
    assert_eq!(
        periods(&rows),
        vec![((0, 60_000), BTreeMap::from([(0, 60_000)]))]
    );

    let mut reporter = StalenessReporter::new(ts(0), ts(500));
    reporter.observe(ts(400), ts(900));
    assert_eq!(reporter.read(ts(400)), Duration::ZERO);
}

#[mz_ore::test]
fn clock_steps_back_drops_sample() {
    let c = config(60_000, 4);
    let s = c.scheme;
    let mut recorder = StalenessRecorder::new(ts(0), ts(0));
    assert_eq!(recorder.observe(ts(1000), ts(0), &c), vec![]);
    // The step back is dropped, and its reflected time holds for the next sample.
    assert_eq!(recorder.observe(ts(900), ts(900), &c), vec![]);
    assert_eq!(recorder.observe(ts(2000), ts(1900), &c), vec![]);
    let rows = recorder.finish(ts(2000), &c);
    // [900, 1000) counts once, the next sample continues at 1000.
    assert_eq!(
        periods(&rows),
        vec![(
            (0, 2000),
            BTreeMap::from([(s.bucket(ms(1000)), 1000), (s.bucket(ms(1100)), 1000)])
        )]
    );
}

#[mz_ore::test]
fn period_change_cuts_open_period() {
    let minute = config(60_000, 4);
    let ten = config(10_000, 4);
    let mut recorder = StalenessRecorder::new(ts(0), ts(0));
    let mut rows = Vec::new();
    for now in (1000..=30_000).step_by(1000) {
        rows.extend(recorder.observe(ts(now), ts(now - 1000), &minute));
    }
    assert_eq!(rows, vec![]);
    for now in (31_000..=50_000).step_by(1000) {
        rows.extend(recorder.observe(ts(now), ts(now - 1000), &ten));
    }
    let bounds: Vec<_> = periods(&rows)
        .into_iter()
        .map(|(bounds, buckets)| {
            assert_eq!(total(&buckets), bounds.1 - bounds.0, "{bounds:?}");
            bounds
        })
        .collect();
    assert_eq!(
        bounds,
        vec![(0, 30_000), (30_000, 40_000), (40_000, 50_000)]
    );
}

#[mz_ore::test]
fn scheme_change_cuts_open_period() {
    let mut recorder = StalenessRecorder::new(ts(0), ts(0));
    let mut rows = recorder.observe(ts(30_000), ts(29_000), &config(60_000, 4));
    rows.extend(recorder.observe(ts(60_000), ts(59_000), &config(60_000, 8)));
    let summary: Vec<_> = rows
        .iter()
        .map(|r| {
            (
                u64::from(r.period_start),
                u64::from(r.period_end),
                r.buckets_per_octave,
                r.count_ms,
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![(0, 30_000, 4, 30_000), (30_000, 60_000, 8, 30_000)]
    );
}

#[mz_ore::test]
fn finish_emits_partial_period() {
    let c = config(60_000, 4);
    let mut recorder = StalenessRecorder::new(ts(0), ts(0));
    let mut rows = Vec::new();
    for now in (1000..30_000).step_by(1000) {
        rows.extend(recorder.observe(ts(now), ts(now - 1000), &c));
    }
    rows.extend(recorder.finish(ts(30_000), &c));
    let periods = periods(&rows);
    assert_eq!(periods.len(), 1);
    assert_eq!(periods[0].0, (0, 30_000));
    assert_eq!(total(&periods[0].1), 30_000);
    assert_eq!(recorder.finish(ts(40_000), &c), vec![]);
    assert_eq!(recorder.observe(ts(70_000), ts(69_000), &c), vec![]);
    assert_eq!(recorder.tick(ts(130_000), &c), vec![]);
}

#[mz_ore::test]
fn finish_without_counts_emits_nothing() {
    let c = config(60_000, 4);
    let mut recorder = StalenessRecorder::new(ts(5000), ts(0));
    assert_eq!(recorder.finish(ts(5000), &c), vec![]);
}

#[mz_ore::test]
fn retention_change_does_not_cut() {
    let mut recorder = StalenessRecorder::new(ts(0), ts(0));
    let c = config(60_000, 4);
    assert_eq!(recorder.observe(ts(30_000), ts(29_000), &c), vec![]);
    let shorter = RecorderConfig {
        retention: Duration::from_secs(60),
        ..c
    };
    let rows = recorder.observe(ts(60_000), ts(59_000), &shorter);
    let bounds: Vec<_> = periods(&rows).into_iter().map(|(b, _)| b).collect();
    assert_eq!(bounds, vec![(0, 60_000)]);
}

#[derive(Debug, Clone)]
enum Op {
    Observe { step: u64, lag: i64 },
    Tick { step: u64 },
}

/// An operation, and whether the period length flips before it.
fn arb_op() -> impl Strategy<Value = (Op, bool)> {
    // Mostly short steps, some across several periods.
    let step = Union::new_weighted(vec![(8, 0u64..3000), (1, 0u64..200_000)]);
    let op = Union::new(vec![
        (step.clone(), -1000i64..5000)
            .prop_map(|(step, lag)| Op::Observe { step, lag })
            .boxed(),
        step.prop_map(|step| Op::Tick { step }).boxed(),
    ]);
    (op, proptest::bool::weighted(0.05))
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn mass_is_conserved() {
    proptest!(|(
        start in 0u64..100_000,
        ops in proptest::collection::vec(arb_op(), 0..200),
        vary_period: bool,
        back in proptest::collection::vec(0u64..5000, 200),
    )| {
        let periods_ms = [60_000u64, 10_000];
        let mut period_idx = 0;
        let mut recorder = StalenessRecorder::new(ts(start), ts(start));
        let mut rows = Vec::new();
        let mut now = start;
        // The latest time a call closed a sample at, which the counts must reach.
        let mut high = start;
        for (i, (op, flip)) in ops.into_iter().enumerate() {
            if vary_period && flip {
                period_idx = 1 - period_idx;
            }
            let c = config(periods_ms[period_idx], 4);
            // Every fourth operation steps the clock back.
            let step_back = if i % 4 == 3 { back[i] } else { 0 };
            match op {
                Op::Observe { step, lag } => {
                    now = (now + step).saturating_sub(step_back);
                    let reflected = now.saturating_add_signed(-lag);
                    rows.extend(recorder.observe(ts(now), ts(reflected), &c));
                    high = high.max(now);
                }
                Op::Tick { step } => {
                    now = (now + step).saturating_sub(step_back);
                    rows.extend(recorder.tick(ts(now), &c));
                    // A tick closes the sample at the last period boundary only.
                    high = high.max(now - now % periods_ms[period_idx]);
                }
            }
        }
        rows.extend(recorder.finish(ts(now), &config(periods_ms[period_idx], 4)));
        high = high.max(now);

        let periods = periods(&rows);
        let mut expected_start = start;
        for (i, ((s, e), buckets)) in periods.iter().enumerate() {
            prop_assert_eq!(*s, expected_start, "periods tile wall-clock time");
            prop_assert!(s < e);
            prop_assert_eq!(total(buckets), e - s, "period ({}, {})", s, e);
            let interior = i != 0 && i + 1 != periods.len();
            if !vary_period && interior {
                prop_assert_eq!(e - s, 60_000, "full period ({}, {})", s, e);
                prop_assert_eq!(s % 60_000, 0);
            }
            expected_start = *e;
        }
        // A step back counts nothing twice, so the counts end where the latest sample closed.
        prop_assert_eq!(expected_start, high);
    });
}

#[mz_ore::test]
fn reporter_keeps_peak_between_reads() {
    let mut reporter = StalenessReporter::new(ts(0), ts(0));
    // Sample [0, 1200) against reflected time 0.
    reporter.observe(ts(1200), ts(1000));
    // Sample [1200, 2050) against reflected time 1000.
    reporter.observe(ts(2050), ts(2000));
    assert_eq!(reporter.read(ts(2050)), ms(1200));
    // The peak is reset, the last closed sample's worst case remains.
    assert_eq!(reporter.read(ts(2100)), ms(1050));
    // A stall grows live.
    assert_eq!(reporter.read(ts(3500)), ms(1500));
    // A step back closes no sample, but its reflected time holds from then on.
    reporter.observe(ts(1000), ts(1500));
    assert_eq!(reporter.read(ts(3500)), ms(2000));
}
