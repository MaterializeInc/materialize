// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_ore::cast::CastLossy;
use mz_repr::Timestamp;
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use proptest::prelude::*;

use super::{BucketScheme, reflected_time};
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
