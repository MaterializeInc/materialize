// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::time::Duration;

use itertools::Itertools;
use mz_ore::cast::CastLossy;
use mz_repr::Timestamp;
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use proptest::prelude::*;
use proptest::sample::select;
use proptest::strategy::Union;

use super::histogram::{BucketScheme, add_span};
use super::local::{LocalDelayRecorder, LocalInput, LocalRows, VisibilityLog};
use super::period::{DAY_MS, PeriodBook};
use super::recorder::{FreshnessRecorder, PeriodRow, RecorderConfig};
use super::*;
use timely::progress::Antichain;

fn aligned(ms: u64) -> ResolvedTree {
    ResolvedTree::Leaf(Ticker::Aligned { interval_ms: ms })
}

fn every(ms: u64) -> RefreshSchedule {
    RefreshSchedule {
        everies: vec![RefreshEvery {
            interval: Duration::from_millis(ms),
            aligned_to: Timestamp::from(0u64),
        }],
        ats: vec![],
    }
}

#[mz_ore::test]
fn last_reflected_join_7_11() {
    let tree = ResolvedTree::Min(vec![aligned(7000), aligned(11000)]);
    for (frontier, expected) in [
        (11001u64, 7000u64),
        (21001, 11000),
        (22001, 21000),
        (28001, 22000),
    ] {
        assert_eq!(
            tree.last_reflected(Timestamp::from(frontier)),
            Some(expected),
            "frontier {frontier}"
        );
    }
}

#[mz_ore::test]
fn last_reflected_refresh_before_first() {
    let tree = ResolvedTree::Refresh {
        schedule: every(60_000),
        input: Box::new(aligned(1000)),
    };
    // A frontier of 0 has no complete timestamp.
    assert_eq!(tree.last_reflected(Timestamp::from(0u64)), None);
    let schedule = RefreshSchedule {
        everies: vec![],
        ats: vec![Timestamp::from(60_000u64)],
    };
    let tree = ResolvedTree::Refresh {
        schedule,
        input: Box::new(aligned(1000)),
    };
    assert_eq!(tree.last_reflected(Timestamp::from(60_000u64)), None);
    assert_eq!(
        tree.last_reflected(Timestamp::from(60_001u64)),
        Some(60_000)
    );
}

#[mz_ore::test]
fn next_boundary_union_of_grids() {
    let tree = ResolvedTree::Min(vec![aligned(7000), aligned(11000)]);
    assert_eq!(tree.next_boundary(15000), 21000);
    assert_eq!(tree.next_boundary(22000), 22000);
}

#[mz_ore::test]
fn merge_dedups_and_unwraps() {
    let leaf = FreshnessTree::Leaf(FreshnessLeaf::Fixed(Ticker::Continuous));
    assert_eq!(FreshnessTree::merge(vec![], None), None);
    assert_eq!(
        FreshnessTree::merge(vec![leaf.clone(), leaf.clone()], None),
        Some(leaf.clone())
    );
    let sched = every(1000);
    assert_eq!(
        FreshnessTree::merge(vec![leaf.clone()], Some(&sched)),
        Some(FreshnessTree::Refresh {
            schedule: sched,
            input: Box::new(leaf)
        })
    );
}

#[mz_ore::test]
fn resolve_unknown_ingestion_is_none() {
    struct Empty;
    impl TickerLookup for Empty {
        fn ingestion_ticker(&self, _: GlobalId) -> Option<Ticker> {
            None
        }
    }
    let tree = FreshnessTree::Leaf(FreshnessLeaf::Ingestion(GlobalId::User(1)));
    assert!(tree.resolve(&Empty).is_none());
}

#[mz_ore::test]
fn continuous_and_unaligned_boundaries() {
    let tree = ResolvedTree::Min(vec![
        ResolvedTree::Leaf(Ticker::Unaligned { interval_ms: 7 }),
        ResolvedTree::Leaf(Ticker::Continuous),
    ]);
    assert_eq!(tree.next_boundary(15), 15);
    // The continuous leaf does not floor, the unaligned one floors to 14.
    assert_eq!(tree.last_reflected(Timestamp::from(16u64)), Some(14));
    let tree = ResolvedTree::Leaf(Ticker::Unaligned { interval_ms: 7 });
    assert_eq!(tree.next_boundary(15), 21);
    assert_eq!(tree.last_reflected(Timestamp::from(16u64)), Some(14));
}

fn arb_tree() -> impl Strategy<Value = ResolvedTree> {
    let ticker = Union::new([
        (1u64..=50)
            .prop_map(|interval_ms| Ticker::Aligned { interval_ms })
            .boxed(),
        (1u64..=50)
            .prop_map(|interval_ms| Ticker::Unaligned { interval_ms })
            .boxed(),
        Just(Ticker::Continuous).boxed(),
    ]);
    let schedule = (1u64..=60, 0u64..=30);
    (
        proptest::collection::vec(ticker.clone(), 1..=4),
        // A refresh input of the minimum, as a join that reads a REFRESH view.
        proptest::option::of((proptest::collection::vec(ticker, 1..=2), schedule.clone())),
        proptest::option::of(schedule),
    )
        .prop_map(|(leaves, inner, outer)| {
            let refreshed =
                |(interval, aligned_to): (u64, u64), input: ResolvedTree| ResolvedTree::Refresh {
                    schedule: RefreshSchedule {
                        everies: vec![RefreshEvery {
                            interval: Duration::from_millis(interval),
                            aligned_to: Timestamp::from(aligned_to),
                        }],
                        ats: vec![],
                    },
                    input: Box::new(input),
                };
            let mut inputs: Vec<_> = leaves.into_iter().map(ResolvedTree::Leaf).collect();
            if let Some((inner_leaves, schedule)) = inner {
                let inner_leaves = inner_leaves.into_iter().map(ResolvedTree::Leaf).collect();
                inputs.push(refreshed(schedule, ResolvedTree::Min(inner_leaves)));
            }
            let min = ResolvedTree::Min(inputs);
            match outer {
                None => min,
                Some(schedule) => refreshed(schedule, min),
            }
        })
}

/// `tree` with its unaligned leaves following the wall clock, which gives them no wait.
fn unaligned_as_continuous(tree: &ResolvedTree) -> ResolvedTree {
    match tree {
        ResolvedTree::Leaf(Ticker::Unaligned { .. }) => ResolvedTree::Leaf(Ticker::Continuous),
        ResolvedTree::Leaf(t) => ResolvedTree::Leaf(*t),
        ResolvedTree::Min(inputs) => {
            ResolvedTree::Min(inputs.iter().map(unaligned_as_continuous).collect())
        }
        ResolvedTree::Refresh { schedule, input } => ResolvedTree::Refresh {
            schedule: schedule.clone(),
            input: Box::new(unaligned_as_continuous(input)),
        },
    }
}

/// The first wall clock at which the on-time frontier of `tree` satisfies `passed`, which must
/// be monotone.
fn on_time_first(tree: &ResolvedTree, passed: impl Fn(u64) -> bool) -> u64 {
    let (mut lo, mut hi) = (0u64, 1u64 << 42);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if tree
            .expected_upper(mid)
            .is_some_and(|f| passed(u64::from(f)))
        {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

#[mz_ore::test]
fn grid_wait_matches_on_time_frontier() {
    proptest!(|(tree in arb_tree(), timestamp in 0u64..400)| {
        // Brute force: the first wall clock at which the on-time frontier is past `timestamp`.
        let model = unaligned_as_continuous(&tree);
        let passed = (timestamp..)
            .find(|w| model.expected_upper(*w).is_some_and(|f| u64::from(f) > timestamp))
            .unwrap();
        prop_assert_eq!(tree.grid_wait(timestamp), passed - timestamp);
    });
}

/// `tree` with its unaligned leaves aligned to the epoch.
fn unaligned_as_aligned(tree: &ResolvedTree) -> ResolvedTree {
    match tree {
        ResolvedTree::Leaf(Ticker::Unaligned { interval_ms }) => aligned(*interval_ms),
        ResolvedTree::Leaf(t) => ResolvedTree::Leaf(*t),
        ResolvedTree::Min(inputs) => {
            ResolvedTree::Min(inputs.iter().map(unaligned_as_aligned).collect())
        }
        ResolvedTree::Refresh { schedule, input } => ResolvedTree::Refresh {
            schedule: schedule.clone(),
            input: Box::new(unaligned_as_aligned(input)),
        },
    }
}

#[mz_ore::test]
fn latest_show_bounds_on_time_show() {
    proptest!(|(tree in arb_tree(), arrival in 1u64..400)| {
        // Without unaligned leaves, the latest show is the on-time show: when the frontier
        // passes the timestamp the arrival is assigned.
        let model = unaligned_as_aligned(&tree);
        let shown = model
            .assigned(arrival)
            .map(|t| t + model.grid_wait(t));
        prop_assert_eq!(model.latest_show(arrival), shown);
        // Treating a leaf as unaligned only moves its latest show later.
        if let (Some(latest), Some(shown)) = (tree.latest_show(arrival), shown) {
            prop_assert!(latest >= shown, "{latest} < {shown}");
        }
    });
}

#[mz_ore::test]
fn last_reflected_matches_oracle() {
    proptest!(|(tree in arb_tree(), frontier in 0u64..400)| {
        // Brute force: the largest arrival whose assigned timestamp is below the frontier.
        let mut expected = None;
        for t in 0..=frontier {
            if let Some(a) = tree.assigned(t) {
                if a < frontier {
                    expected = Some(t);
                }
            }
        }
        prop_assert_eq!(tree.last_reflected(Timestamp::from(frontier)), expected);
    });
}

fn unaligned(ms: u64) -> ResolvedTree {
    ResolvedTree::Leaf(Ticker::Unaligned { interval_ms: ms })
}

fn refresh(schedule: RefreshSchedule, input: ResolvedTree) -> ResolvedTree {
    ResolvedTree::Refresh {
        schedule,
        input: Box::new(input),
    }
}

#[mz_ore::test]
fn bound_join_closed_form() {
    let join = |is: &[u64]| ResolvedTree::Min(is.iter().map(|i| aligned(*i)).collect());
    assert_eq!(join(&[7000, 11000]).assignment_bound(), Some(17000));
    assert_eq!(join(&[1000, 7000]).assignment_bound(), Some(7000));
    assert_eq!(aligned(100).assignment_bound(), Some(100));
    // The pairwise maximum, from 10 + 15 - 5.
    assert_eq!(join(&[6000, 10000, 15000]).assignment_bound(), Some(20000));
    // Nested minima flatten.
    let nested = ResolvedTree::Min(vec![aligned(7000), ResolvedTree::Min(vec![aligned(11000)])]);
    assert_eq!(nested.assignment_bound(), Some(17000));
}

#[mz_ore::test]
fn bound_aligned_unaligned() {
    let tree = ResolvedTree::Min(vec![aligned(7000), unaligned(1000)]);
    assert_eq!(tree.assignment_bound(), Some(8000));
    let tree = ResolvedTree::Min(vec![unaligned(3000), unaligned(1000)]);
    assert_eq!(tree.assignment_bound(), Some(4000));
    assert_eq!(unaligned(1000).assignment_bound(), Some(1000));
}

#[mz_ore::test]
fn bound_continuous() {
    let continuous = ResolvedTree::Leaf(Ticker::Continuous);
    assert_eq!(continuous.assignment_bound(), Some(0));
    let tree = ResolvedTree::Min(vec![aligned(7000), continuous]);
    assert_eq!(tree.assignment_bound(), Some(7000));
}

#[mz_ore::test]
fn bound_refresh() {
    let tree = refresh(every(60_000), aligned(1000));
    assert_eq!(tree.assignment_bound(), Some(60_000));
    let tree = refresh(
        every(60_000),
        ResolvedTree::Min(vec![aligned(7000), aligned(11000)]),
    );
    assert_eq!(tree.assignment_bound(), Some(77_000));
    let month = 30 * 24 * 60 * 60 * 1000;
    let tree = refresh(every(month), aligned(1000));
    assert!(tree.assignment_bound() >= Some(month));
}

#[mz_ore::test]
fn bound_only_ats_is_unbounded() {
    let at = RefreshSchedule {
        everies: vec![],
        ats: vec![Timestamp::from(100u64)],
    };
    assert_eq!(refresh(at.clone(), aligned(10)).assignment_bound(), None);
    // The unbounded refresh may sit anywhere in the tree.
    let tree = ResolvedTree::Min(vec![aligned(7), refresh(at, aligned(10))]);
    assert_eq!(tree.assignment_bound(), None);
}

#[mz_ore::test]
fn bound_ignores_ats_next_to_everies() {
    let mut mixed = every(60_000);
    mixed.ats = vec![Timestamp::from(5u64), Timestamp::from(12_345u64)];
    let input = ResolvedTree::Min(vec![aligned(7000), aligned(11000)]);
    assert_eq!(
        refresh(mixed, input.clone()).assignment_bound(),
        refresh(every(60_000), input).assignment_bound()
    );
}

/// A hyperperiod with more than 10^6 change points takes the conservative bound, the period
/// plus the assignment and lag of the input.
#[mz_ore::test]
fn bound_refresh_conservative_fallback() {
    let tree = refresh(every(1_000_003), aligned(1));
    assert_eq!(tree.assignment_bound(), Some(1_000_003 + 1 + 1));
}

/// A minute refresh over a table reads about a minute, not two.
#[mz_ore::test]
fn bound_refresh_over_table() {
    let tree = refresh(every(60_000), unaligned(1000));
    assert_eq!(tree.assignment_bound(), Some(62_000));
}

/// The worst `visibility - arrival` over arrivals in a steady-state hyperperiod, found by
/// sweeping the wall clock and tracking the oldest arrival that is not yet reflected.
/// Returns the bound on the integer ms grid, which is one more than the largest latency.
fn oracle_bound(tree: &ResolvedTree, hyper: u64) -> u64 {
    let mut worst = 0;
    // The oldest arrival that is not yet reflected.
    let mut oldest = 0u64;
    for w in 0..5 * hyper {
        let Some(reflected) = tree.expected_upper(w).and_then(|e| tree.last_reflected(e)) else {
            continue;
        };
        if reflected >= oldest {
            if (hyper..2 * hyper).contains(&oldest) {
                worst = worst.max(w - oldest);
            }
            oldest = reflected + 1;
        }
    }
    worst + 1
}

fn lcm(a: u64, b: u64) -> u64 {
    fn gcd(a: u64, b: u64) -> u64 {
        if b == 0 { a } else { gcd(b, a % b) }
    }
    a / gcd(a, b) * b
}

#[mz_ore::test]
fn bound_matches_oracle() {
    let leaves = proptest::collection::vec(1u64..=30, 1..=3);
    proptest!(|(leaves in leaves, period in proptest::option::of(1u64..=60))| {
        let min = ResolvedTree::Min(leaves.iter().map(|i| aligned(*i)).collect());
        let (tree, hyper) = match period {
            None => (min, leaves.iter().fold(1, |a, b| lcm(a, *b))),
            Some(p) => (
                refresh(every(p), min),
                leaves.iter().fold(p, |a, b| lcm(a, *b)),
            ),
        };
        prop_assert_eq!(tree.assignment_bound(), Some(oracle_bound(&tree, hyper)));
    });
}

/// A tree whose unaligned leaves have an explicit phase, so the true delay can be simulated
/// without the frontier model that `assignment_bound` scans.
#[derive(Debug, Clone)]
enum Spec {
    Aligned(u64),
    /// Interval and phase, ticks at `phase + k * interval`.
    Unaligned(u64, u64),
    Continuous,
    Min(Vec<Spec>),
    /// Refresh period, aligned to 0.
    Refresh(u64, Box<Spec>),
}

impl Spec {
    fn tree(&self) -> ResolvedTree {
        match self {
            Spec::Aligned(i) => aligned(*i),
            Spec::Unaligned(i, _) => unaligned(*i),
            Spec::Continuous => ResolvedTree::Leaf(Ticker::Continuous),
            Spec::Min(inputs) => ResolvedTree::Min(inputs.iter().map(Spec::tree).collect()),
            Spec::Refresh(p, input) => refresh(every(*p), input.tree()),
        }
    }

    /// The least common multiple of all intervals and periods.
    fn hyper(&self) -> u64 {
        match self {
            Spec::Aligned(i) | Spec::Unaligned(i, _) => *i,
            Spec::Continuous => 1,
            Spec::Min(inputs) => inputs.iter().fold(1, |a, s| lcm(a, s.hyper())),
            Spec::Refresh(p, input) => lcm(*p, input.hyper()),
        }
    }

    /// The timestamp assigned to an arrival at `t`.
    fn assigned(&self, t: u64) -> u64 {
        match self {
            Spec::Aligned(i) => ceil_to(t, 0, *i),
            Spec::Unaligned(i, phase) => ceil_to(t, *phase, *i),
            Spec::Continuous => t,
            Spec::Min(inputs) => inputs.iter().map(|s| s.assigned(t)).max().unwrap(),
            Spec::Refresh(p, input) => ceil_to(input.assigned(t), 0, *p),
        }
    }

    /// The output frontier at wall clock `w`: a tick at `t` leaves a leaf at `t + 1`, a minimum
    /// takes the least input, and a refresh node advances to its next refresh as soon as its
    /// input has passed the previous one.
    fn frontier(&self, w: u64) -> u64 {
        match self {
            Spec::Aligned(i) => w - w % i + 1,
            // Only called at wall clocks past the phase.
            Spec::Unaligned(i, phase) => phase + (w - phase) / i * i + 1,
            Spec::Continuous => w + 1,
            Spec::Min(inputs) => inputs.iter().map(|s| s.frontier(w)).min().unwrap(),
            Spec::Refresh(p, input) => ceil_to(input.frontier(w), 0, *p),
        }
    }

    /// The wall clock at which a refresh node first runs, in the sense that its refresh at `x`
    /// has to wait for the wall clock to reach `x`, which a refresh input can run ahead of.
    fn gated_visible(&self, x: u64) -> u64 {
        match self {
            Spec::Aligned(i) => ceil_to(x, 0, *i),
            Spec::Unaligned(i, phase) => ceil_to(x, *phase, *i),
            Spec::Continuous => x,
            Spec::Min(inputs) => inputs.iter().map(|s| s.gated_visible(x)).max().unwrap(),
            Spec::Refresh(p, input) => input.gated_visible(ceil_to(x, 0, *p)),
        }
    }

    /// The largest delay of an arrival in a steady-state hyperperiod until the output frontier
    /// passes its timestamp, and the same under the stricter wall-clock gate.
    fn max_latency(&self) -> (u64, u64) {
        let hyper = self.hyper();
        let (mut frontier, mut gated) = (0, 0);
        for t in hyper..2 * hyper {
            let assigned = self.assigned(t);
            let w = (t..).find(|w| self.frontier(*w) > assigned).unwrap();
            frontier = frontier.max(w - t);
            gated = gated.max(self.gated_visible(assigned) - t);
        }
        (frontier, gated)
    }
}

/// The smallest `phase + k * interval` at or above `x`, for integer `k`.
fn ceil_to(x: u64, phase: u64, interval: u64) -> u64 {
    if x <= phase {
        phase - (phase - x) / interval * interval
    } else {
        phase + (x - phase).div_ceil(interval) * interval
    }
}

fn arb_spec() -> impl Strategy<Value = Spec> {
    let leaf = Union::new([
        (1u64..=8).prop_map(Spec::Aligned).boxed(),
        (1u64..=5)
            .prop_flat_map(|u| (Just(u), 0..u))
            .prop_map(|(u, phase)| Spec::Unaligned(u, phase))
            .boxed(),
        Just(Spec::Continuous).boxed(),
    ]);
    leaf.prop_recursive(2, 8, 3, |inner| {
        Union::new([
            proptest::collection::vec(inner.clone(), 2..=3)
                .prop_map(Spec::Min)
                .boxed(),
            (1u64..=10, inner)
                .prop_map(|(p, input)| Spec::Refresh(p, Box::new(input)))
                .boxed(),
        ])
    })
}

/// The bound never falls below the simulated delay of any phase, with and without refresh.
/// The scan models the output frontier. The conservative fallback also covers the stricter
/// reading in which a refresh waits for the wall clock.
#[mz_ore::test]
fn bound_covers_simulated_delay() {
    proptest!(ProptestConfig::with_cases(128), |(spec in arb_spec())| {
        let tree = spec.tree();
        let (frontier, gated) = spec.max_latency();
        let bound = tree.assignment_bound();
        prop_assert!(bound >= Some(frontier), "{bound:?} < {frontier}");
        let conservative = tree.assignment_bound_with_cap(0);
        prop_assert!(conservative >= Some(frontier.max(gated)), "{conservative:?} < {frontier} {gated}");
    });
}

/// The scan floors unaligned leaves to the epoch grid, which undershoots a phase off that grid
/// under a refresh, so such trees take the conservative bound.
#[mz_ore::test]
fn bound_refresh_over_unaligned_phase() {
    let spec = Spec::Min(vec![
        Spec::Refresh(5, Box::new(Spec::Unaligned(4, 1))),
        Spec::Aligned(8),
    ]);
    assert!(spec.tree().assignment_bound() >= Some(spec.max_latency().0));
}

/// Every phase of an unaligned leaf joined with an aligned one stays below the closed form.
#[mz_ore::test]
fn bound_covers_every_unaligned_phase() {
    for (i, u) in [(7u64, 3u64), (4, 6), (5, 5), (1, 4)] {
        let tree = ResolvedTree::Min(vec![aligned(i), unaligned(u)]);
        for phase in 0..u {
            let spec = Spec::Min(vec![Spec::Aligned(i), Spec::Unaligned(u, phase)]);
            assert!(
                tree.assignment_bound() >= Some(spec.max_latency().0),
                "i {i} u {u} phase {phase}"
            );
        }
    }
}

fn scheme(b: u32) -> BucketScheme {
    BucketScheme::new(b).expect("valid bucket count")
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
    assert_eq!(s.bucket(1), 0);
    assert_eq!(s.bucket(2), 4);
    assert_eq!(s.bucket(1024), 40);
    assert_eq!(s.bucket(1025), 41);
    for x in 1..10_000u64 {
        let k = s.bucket(x);
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
            assert_eq!(s.bucket(x), e * bi, "b={b} 2^{e}");
            if e > 0 {
                // One past the bound is in the next bucket or, for a coarse scheme, a later one
                // of the same octave.
                assert!(s.bucket(x + 1) > e * bi, "b={b} 2^{e}+1");
                assert!(s.bucket(x + 1) <= (e + 1) * bi, "b={b} 2^{e}+1");
            }
            if e > 1 {
                // Just below 2^e, the top of the octave: the bucket is at most 2^e's.
                assert!(s.bucket(x - 1) <= e * bi, "b={b} 2^{e}-1");
                assert!(s.bucket(x - 1) > (e - 1) * bi, "b={b} 2^{e}-1");
            }
        }
        assert_eq!(s.bucket(u64::MAX), 64 * bi);
    }
}

/// The number of multiples of `interval` in `(from, to]`.
fn multiples(interval: u64, from: u64, to: u64) -> u64 {
    to / interval - from / interval
}

/// Whether `s` is a window boundary of `tree`, computed from its tickers only.
fn is_boundary(tree: &ResolvedTree, s: u64) -> bool {
    let mut b = false;
    tree.for_each_ticker(&mut |t| {
        b |= match t {
            Ticker::Aligned { interval_ms } | Ticker::Unaligned { interval_ms } => {
                *interval_ms == 0 || s % interval_ms == 0
            }
            Ticker::Continuous => true,
        }
    });
    b
}

/// Reference for `add_span` that charges every arrival one by one: its window start is the
/// largest boundary below it, and its latency is `max(advance, to)` minus that start.
fn reference_span(
    tree: &ResolvedTree,
    scheme: &BucketScheme,
    interval: u64,
    from: u64,
    to: u64,
    advance: u64,
    counts: &mut BTreeMap<i32, u64>,
) {
    let a = advance.max(to);
    for t in (from + 1)..=to {
        if t % interval != 0 {
            continue;
        }
        let mut start = t - 1;
        while !is_boundary(tree, start) {
            start -= 1;
        }
        assert!(start >= from);
        *counts.entry(scheme.bucket(a - start)).or_default() += 1;
    }
}

/// A random tree with a span `(from, to]` between two of its window boundaries.
fn arb_span() -> impl Strategy<Value = (ResolvedTree, u64, u64)> {
    (arb_tree(), 0u64..300, 0u64..300).prop_map(|(tree, f, len)| {
        let from = tree.next_boundary(f);
        let to = tree.next_boundary(from + len);
        (tree, from, to)
    })
}

#[mz_ore::test]
fn span_conserves_counts() {
    proptest!(|(
        (tree, from, to) in arb_span(),
        extra in 0u64..500,
        interval in select(vec![1u64, 7, 10]),
        b in select(vec![1u32, 4, 16]),
    )| {
        let mut counts = BTreeMap::new();
        add_span(&tree, &scheme(b), interval, from, to, to + extra, &mut counts);
        prop_assert_eq!(counts.values().sum::<u64>(), multiples(interval, from, to));
        prop_assert!(counts.values().all(|c| *c > 0));
    });
}

#[mz_ore::test]
fn span_matches_window_walk() {
    proptest!(|(
        (tree, from, to) in arb_span(),
        extra in 0u64..500,
        interval in select(vec![1u64, 3, 7]),
        b in select(vec![1u32, 4, 16]),
    )| {
        let s = scheme(b);
        let mut actual = BTreeMap::new();
        add_span(&tree, &s, interval, from, to, to + extra, &mut actual);
        let mut expected = BTreeMap::new();
        reference_span(&tree, &s, interval, from, to, to + extra, &mut expected);
        prop_assert_eq!(actual, expected);
    });
}

#[mz_ore::test]
fn span_clock_skew() {
    proptest!(|((tree, from, to) in arb_span(), interval in 1u64..=5)| {
        let s = scheme(4);
        // The advance time precedes the span end, so the span end stands in for it, and every
        // arrival's latency is at least the length of its window.
        let mut skewed = BTreeMap::new();
        add_span(&tree, &s, interval, from, to, 0, &mut skewed);
        let mut clamped = BTreeMap::new();
        add_span(&tree, &s, interval, from, to, to, &mut clamped);
        prop_assert_eq!(&skewed, &clamped);
        let mut expected = BTreeMap::new();
        reference_span(&tree, &s, interval, from, to, 0, &mut expected);
        prop_assert_eq!(skewed, expected);
    });
}

#[mz_ore::test]
fn span_join_7_11_hyperperiod() {
    let tree = ResolvedTree::Min(vec![aligned(7000), aligned(11000)]);
    let s = scheme(256);
    let frontier = |w: u64| (w - w % 7000).min(w - w % 11000) + 1;
    let reflected = |w: u64| {
        tree.last_reflected(Timestamp::from(frontier(w)))
            .expect("visible")
    };
    // Steady state: one hyperperiod after the start.
    let (start, end) = (77_000u64, 154_000u64);
    let mut counts = BTreeMap::new();
    let mut prev = reflected(start);
    for w in (start + 1)..=end {
        if w % 7000 != 0 && w % 11000 != 0 {
            continue;
        }
        let new = reflected(w);
        add_span(&tree, &s, 1, prev, new, w, &mut counts);
        prev = new;
    }
    // Seconds of arrivals at each latency in seconds.
    let expected = [
        (7, 7),
        (11, 4),
        (12, 6),
        (13, 8),
        (14, 31),
        (15, 5),
        (16, 7),
        (17, 9),
    ];
    let mut by_bucket = BTreeMap::new();
    for (secs, count) in expected {
        by_bucket.insert(s.bucket(secs * 1000), count * 1000);
    }
    assert_eq!(
        by_bucket.len(),
        expected.len(),
        "scheme separates latencies"
    );
    assert_eq!(counts, by_bucket);
}

#[mz_ore::test]
fn span_long_stall_is_cheap() {
    let tree = ResolvedTree::Min(vec![aligned(1000), aligned(1001)]);
    let s = scheme(4);
    let day = 86_400_000u64;
    let (from, to) = (0, tree.next_boundary(day));
    let calls = std::cell::Cell::new(0u64);
    let mut counts = BTreeMap::new();
    histogram::add_span_with(
        &|z| {
            calls.set(calls.get() + 1);
            tree.next_boundary(z)
        },
        &s,
        1,
        from,
        to,
        to,
        &mut counts,
    );
    assert_eq!(counts.values().sum::<u64>(), to - from);
    // Windows are at least 1 ms long, so the buckets from latency 1 ms to the span length cover
    // every window, and each costs two boundary evaluations.
    let touched = u64::try_from(s.bucket(to - from) - s.bucket(1) + 1).unwrap();
    assert!(touched <= 4 * 28, "{touched} buckets");
    assert!(calls.get() <= 2 * touched, "{} calls", calls.get());
}

fn config(per_octave: u32) -> RecorderConfig {
    RecorderConfig {
        scheme: scheme(per_octave),
        interval_ms: 1,
        period_ms: 60_000,
        retention_ms: RETENTION_MS,
    }
}

const RETENTION_MS: u64 = 30 * DAY_MS;

fn at(frontier: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::from(frontier))
}

/// `logs` as inputs that all have `tree`.
fn inputs_of<'a>(tree: &'a ResolvedTree, logs: &[&'a VisibilityLog]) -> Vec<LocalInput<'a>> {
    logs.iter().map(|log| LocalInput { tree, log }).collect()
}

/// The summed counts of the rows of each period start.
fn period_totals(rows: &[PeriodRow]) -> BTreeMap<u64, u64> {
    let mut totals = BTreeMap::new();
    for row in rows {
        *totals.entry(row.period_start_ms).or_default() += row.count;
    }
    totals
}

#[mz_ore::test]
fn rows_are_final_and_conserved() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut recorder = FreshnessRecorder::new(0);
    let mut rows = Vec::new();
    let mut emitted_at = BTreeMap::new();
    for w in (1000..=300_000u64).step_by(1000) {
        let new = recorder.observe(&tree, &at(w + 1), w + 1, &cfg);
        for p in period_totals(&new).into_keys() {
            // Every period is emitted by exactly one call.
            assert!(
                emitted_at.insert(p, w).is_none(),
                "period {p} emitted twice"
            );
        }
        rows.extend(new);
    }
    let totals = period_totals(&rows);
    assert_eq!(totals.len(), 5);
    for (start, total) in totals {
        assert_eq!(total, 60_000, "period {start}");
        // A period is emitted once its last arrival is reflected.
        assert_eq!(emitted_at[&start], start + 60_000);
    }
}

#[mz_ore::test]
fn stall_charged_to_arrival_periods() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut recorder = FreshnessRecorder::new(0);
    let mut rows = Vec::new();
    for w in (1000..=100_000u64).step_by(1000) {
        rows.extend(recorder.observe(&tree, &at(w + 1), w + 1, &cfg));
    }
    // Only the first period closed before the stall.
    assert_eq!(
        period_totals(&rows).keys().copied().collect::<Vec<_>>(),
        [0]
    );
    // The frontier stays at 100_000 for 90 s and then recovers.
    let new = recorder.observe(&tree, &at(191_001), 191_001, &cfg);
    let totals = period_totals(&new);
    assert_eq!(
        totals,
        BTreeMap::from([(60_000, 60_000), (120_000, 60_000)])
    );
    let s = cfg.scheme;
    let extremes = |start: u64| {
        let buckets: Vec<i32> = new
            .iter()
            .filter(|r| r.period_start_ms == start)
            .map(|r| r.bucket)
            .collect();
        (*buckets.first().unwrap(), *buckets.last().unwrap())
    };
    // The stalled windows start at 100_000 and are visible at 191_000, so latencies fall from
    // 91 s. Period (60 s, 120 s] holds 20 s of them, ending with the window starting at 119_000.
    assert_eq!(extremes(60_000).1, s.bucket(91_000));
    let stalled: u64 = new
        .iter()
        .filter(|r| r.period_start_ms == 60_000 && r.bucket >= s.bucket(72_000))
        .map(|r| r.count)
        .sum();
    assert_eq!(stalled, 20_000);
    assert_eq!(extremes(120_000), (s.bucket(12_000), s.bucket(71_000)));
}

#[mz_ore::test]
fn regress_does_not_double_count() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut recorder = FreshnessRecorder::new(0);
    let mut rows = Vec::new();
    for (frontier, now) in [(100_001, 100_001), (50_001, 100_500), (120_001, 121_000)] {
        rows.extend(recorder.observe(&tree, &at(frontier), now, &cfg));
    }
    rows.extend(recorder.observe(&tree, &Antichain::new(), 130_000, &cfg));
    let total: u64 = rows.iter().map(|r| r.count).sum();
    assert_eq!(total, 120_000);
}

#[mz_ore::test]
fn alter_interval_does_not_double_count() {
    let cfg = config(4);
    let mut recorder = FreshnessRecorder::new(0);
    let mut rows = Vec::new();
    rows.extend(recorder.observe(&aligned(1000), &at(30_001), 30_001, &cfg));
    // The ingestion ticker changes from 1 s to 11 s.
    rows.extend(recorder.observe(&aligned(11_000), &at(33_001), 34_000, &cfg));
    rows.extend(recorder.observe(&aligned(11_000), &at(121_001), 122_000, &cfg));
    rows.extend(recorder.observe(&aligned(11_000), &Antichain::new(), 123_000, &cfg));
    let total: u64 = rows.iter().map(|r| r.count).sum();
    assert_eq!(total, 121_000);
}

#[mz_ore::test]
fn nothing_reflected_records_nothing() {
    let schedule = RefreshSchedule {
        everies: vec![],
        ats: vec![Timestamp::from(60_000u64)],
    };
    let tree = refresh(schedule, aligned(1000));
    let cfg = config(4);
    let mut recorder = FreshnessRecorder::new(0);
    assert!(
        recorder
            .observe(&tree, &at(30_000), 30_000, &cfg)
            .is_empty()
    );
    // The recorder is not finished by that and records the first refresh once it happens.
    let rows = recorder.observe(&tree, &at(60_001), 61_000, &cfg);
    assert_eq!(period_totals(&rows), BTreeMap::from([(0, 60_000)]));
    assert!(
        recorder
            .observe(&tree, &Antichain::new(), 62_000, &cfg)
            .is_empty()
    );
}

#[mz_ore::test]
fn empty_frontier_finishes() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut recorder = FreshnessRecorder::new(0);
    assert!(
        recorder
            .observe(&tree, &at(30_001), 30_001, &cfg)
            .is_empty()
    );
    let rows = recorder.observe(&tree, &Antichain::new(), 31_000, &cfg);
    // The open period is emitted up to the last arrival.
    assert_eq!(period_totals(&rows), BTreeMap::from([(0, 30_000)]));
    assert!(
        recorder
            .observe(&tree, &at(200_001), 200_001, &cfg)
            .is_empty()
    );
    assert!(
        recorder
            .observe(&tree, &Antichain::new(), 201_000, &cfg)
            .is_empty()
    );
}

#[mz_ore::test]
fn scheme_change_applies_next_period() {
    let tree = aligned(1000);
    let mut recorder = FreshnessRecorder::new(0);
    assert!(
        recorder
            .observe(&tree, &at(30_001), 30_001, &config(4))
            .is_empty()
    );
    let rows = recorder.observe(&tree, &at(130_001), 130_001, &config(1));
    let bpo = |start: u64| -> Vec<u32> {
        let mut v: Vec<u32> = rows
            .iter()
            .filter(|r| r.period_start_ms == start)
            .map(|r| r.buckets_per_octave)
            .collect();
        v.dedup();
        v
    };
    // Period (0, 60 s] was opened under the old scheme and keeps it.
    assert_eq!(bpo(0), [4]);
    assert_eq!(bpo(60_000), [1]);
    assert_eq!(period_totals(&rows).get(&120_000), None);
    // The rows of a period are sorted by bucket.
    let buckets: Vec<i32> = rows
        .iter()
        .filter(|r| r.period_start_ms == 0)
        .map(|r| r.bucket)
        .collect();
    assert!(buckets.windows(2).all(|w| w[0] < w[1]));
}

#[mz_ore::test]
fn period_split_matches_arrival_walk() {
    let tree = ResolvedTree::Min(vec![aligned(7000), aligned(11000)]);
    let cfg = RecorderConfig {
        period_ms: 10_000,
        ..config(4)
    };
    let mut recorder = FreshnessRecorder::new(0);
    let mut rows = Vec::new();
    let mut expected: BTreeMap<(u64, i32), u64> = BTreeMap::new();
    let mut high_water = 0;
    // Spans that cut windows at period boundaries, including a stall and a clock-skewed call.
    for (frontier, now) in [
        (11_001, 11_500),
        (21_001, 40_000),
        (22_001, 22_000),
        (60_001, 61_000),
        (150_001, 150_100),
    ] {
        rows.extend(recorder.observe(&tree, &at(frontier), now, &cfg));
        let lr = tree.last_reflected(Timestamp::from(frontier)).unwrap();
        let advance = now.max(lr);
        for x in (high_water + 1)..=lr {
            let start = tree.prev_boundary(x - 1);
            let period = (x - 1) - (x - 1) % 10_000;
            *expected
                .entry((period, cfg.scheme.bucket(advance - start)))
                .or_default() += 1;
        }
        high_water = high_water.max(lr);
    }
    rows.extend(recorder.observe(&tree, &Antichain::new(), 160_000, &cfg));
    let actual: BTreeMap<(u64, i32), u64> = rows
        .iter()
        .map(|r| ((r.period_start_ms, r.bucket), r.count))
        .collect();
    assert_eq!(actual, expected);
}

#[mz_ore::test]
fn period_length_change_keeps_arrivals_in_their_period() {
    let tree = aligned(1000);
    let mut recorder = FreshnessRecorder::new(0);
    let mut rows = Vec::new();
    let mut covered = 0;
    // Open (0, 10 s], then switch to 60 s periods, whose grid starts at 0 as well. The later
    // steps change the length again, to shorter and longer ones.
    for (frontier, period_ms) in [
        (5_001, 10_000),
        (40_001, 60_000),
        (45_001, 20_000),
        (130_001, 60_000),
        (200_001, 7_000),
    ] {
        let cfg = RecorderConfig {
            period_ms,
            ..config(4)
        };
        rows.extend(recorder.observe(&tree, &at(frontier), frontier, &cfg));
        covered = frontier - 1;
    }
    rows.extend(recorder.observe(&tree, &Antichain::new(), 201_000, &config(4)));
    let mut per_period: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for r in &rows {
        *per_period
            .entry((r.period_start_ms, r.period_end_ms))
            .or_default() += r.count;
    }
    // Every period holds at most its own length, so no arrival sits outside its period.
    for ((start, end), count) in &per_period {
        assert!(
            *count <= end - start,
            "period ({start}, {end}] holds {count}"
        );
    }
    let total: u64 = per_period.values().sum();
    assert_eq!(total, covered);
    // The old period is cut where the change happened, and the new one starts there.
    assert_eq!(per_period[&(0, 5_000)], 5_000);
    assert_eq!(per_period[&(5_000, 40_000)], 35_000);
    // The periods tile the arrivals without overlap.
    let mut end = 0;
    for (start, period_end) in per_period.keys() {
        assert_eq!(*start, end, "{per_period:?}");
        end = *period_end;
    }
}

fn bucket_counts(rows: &[PeriodRow]) -> BTreeMap<i32, u64> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row.bucket).or_default() += row.count;
    }
    counts
}

#[mz_ore::test]
fn visibility_log_visible_at() {
    let mut log = VisibilityLog::default();
    assert_eq!(log.visible_at(1), None);
    log.record(1001, Some(1000), 1010);
    log.record(2001, Some(2000), 2020);
    // A regressing or repeated value is ignored.
    log.record(2001, Some(2000), 2500);
    log.record(1501, Some(1500), 2600);
    // The earliest observation with `reflected >= arrival`.
    assert_eq!(log.visible_at(1), Some(1010));
    assert_eq!(log.visible_at(1000), Some(1010));
    assert_eq!(log.visible_at(1001), Some(2020));
    assert_eq!(log.visible_at(2000), Some(2020));
    assert_eq!(log.visible_at(2001), None);
    // The earliest observation with `frontier > timestamp`.
    assert_eq!(log.passed_at(0), Some(1010));
    assert_eq!(log.passed_at(1000), Some(1010));
    assert_eq!(log.passed_at(1001), Some(2020));
    assert_eq!(log.passed_at(2001), None);
    // Pruned entries answer with the observation of the last pruned entry, and the rest stay
    // correct.
    log.prune_through(1000);
    assert_eq!(log.visible_at(1000), Some(1010));
    assert_eq!(log.visible_at(1001), Some(2020));
    assert_eq!(log.passed_at(1000), Some(1010));
    assert_eq!(log.passed_at(1001), Some(2020));
    log.prune_through(5000);
    assert_eq!(log.visible_at(1), Some(2020));
    assert_eq!(log.visible_at(2000), Some(2020));
    assert_eq!(log.visible_at(6000), None);
    assert_eq!(log.passed_at(2000), Some(2020));
    assert_eq!(log.passed_at(2001), None);
}

#[mz_ore::test]
fn visibility_log_records_frontier_without_reflected_arrivals() {
    let mut log = VisibilityLog::default();
    // A frontier that reflects nothing, as a refresh view's before its first refresh, still
    // answers timestamps.
    log.record(10_000, None, 50);
    log.record(10_000, Some(9_000), 60);
    log.record(20_000, Some(9_000), 70);
    assert_eq!(log.visible_at(1), Some(60));
    assert_eq!(log.visible_at(9_001), None);
    assert_eq!(log.passed_at(9_999), Some(50));
    assert_eq!(log.passed_at(10_000), Some(70));
}

#[mz_ore::test]
fn local_chain_attributes_hops() {
    let tree = aligned(1000);
    let cfg = config(4);
    let (mut log_a, mut log_b) = (VisibilityLog::default(), VisibilityLog::default());
    let mut local_a = LocalDelayRecorder::new(0);
    let mut local_b = LocalDelayRecorder::new(0);
    let mut local_c = LocalDelayRecorder::new(0);
    let mut end_to_end_c = FreshnessRecorder::new(0);
    let (mut rows_a, mut rows_b, mut rows_c, mut rows_e2e) = (vec![], vec![], vec![], vec![]);
    for w in (1000..=200_000u64).step_by(1000) {
        // A is a 1 s source, B shows each tick 3 s later, and C shows it as B does.
        log_a.record(w + 1, Some(w), w + 1);
        rows_a.extend(
            local_a
                .observe(&tree, &tree, &at(w + 1), w + 1, &[], &cfg)
                .local,
        );
        log_b.record(w + 1, Some(w), w + 3001);
        let b = local_b.observe(
            &tree,
            &tree,
            &at(w + 1),
            w + 3001,
            &inputs_of(&tree, &[&log_a]),
            &cfg,
        );
        rows_b.extend(b.local);
        let c = local_c.observe(
            &tree,
            &tree,
            &at(w + 1),
            w + 3001,
            &inputs_of(&tree, &[&log_b]),
            &cfg,
        );
        rows_c.extend(c.local);
        rows_e2e.extend(end_to_end_c.observe(&tree, &at(w + 1), w + 3001, &cfg));
    }
    let empty = Antichain::new();
    rows_a.extend(
        local_a
            .observe(&tree, &tree, &empty, 300_000, &[], &cfg)
            .local,
    );
    rows_b.extend(
        local_b
            .observe(
                &tree,
                &tree,
                &empty,
                300_000,
                &inputs_of(&tree, &[&log_a]),
                &cfg,
            )
            .local,
    );
    rows_c.extend(
        local_c
            .observe(
                &tree,
                &tree,
                &empty,
                300_000,
                &inputs_of(&tree, &[&log_b]),
                &cfg,
            )
            .local,
    );
    rows_e2e.extend(end_to_end_c.observe(&tree, &empty, 300_000, &cfg));

    let s = scheme(4);
    // A window of 1 s ending at w becomes visible at w + 1, so the oldest arrival waited 1001 ms.
    assert_eq!(
        bucket_counts(&rows_a),
        BTreeMap::from([(s.bucket(1001), 200_000)])
    );
    assert_eq!(
        bucket_counts(&rows_b),
        BTreeMap::from([(s.bucket(3000), 200_000)])
    );
    // C adds nothing on top of B, which is the bucket of 1 ms.
    assert_eq!(
        bucket_counts(&rows_c),
        BTreeMap::from([(s.bucket(1), 200_000)])
    );
    // End to end includes the hops of B and C.
    assert_eq!(
        bucket_counts(&rows_e2e),
        BTreeMap::from([(s.bucket(4001), 200_000)])
    );
}

#[mz_ore::test]
fn local_join_splits_alignment_wait() {
    let (l7, l11) = (aligned(7000), aligned(11_000));
    let tree = ResolvedTree::Min(vec![l7.clone(), l11.clone()]);
    let cfg = config(4);
    for swap in [false, true] {
        // The window (0, 7000] needs timestamp 11000. The 11 s input shows it at 11010, which
        // makes it the slowest to show it, but the 7 s input stays below 11000 until 14010.
        let (mut fast, mut slow) = (VisibilityLog::default(), VisibilityLog::default());
        fast.record(7001, Some(7000), 7010);
        fast.record(14_001, Some(14_000), 14_010);
        slow.record(11_001, Some(11_000), 11_010);
        let (fast, slow) = (
            LocalInput {
                tree: &l7,
                log: &fast,
            },
            LocalInput {
                tree: &l11,
                log: &slow,
            },
        );
        let inputs = if swap { [slow, fast] } else { [fast, slow] };
        let mut recorder = LocalDelayRecorder::new(0);
        let mut rows = recorder.observe(&tree, &tree, &at(11_001), 14_030, &inputs, &cfg);
        let finished = recorder.observe(&tree, &tree, &Antichain::new(), 14_030, &inputs, &cfg);
        rows.local.extend(finished.local);
        rows.skew.extend(finished.skew);
        let s = scheme(4);
        assert_eq!(
            bucket_counts(&rows.local),
            BTreeMap::from([(s.bucket(20), 7000)])
        );
        assert_eq!(
            bucket_counts(&rows.skew),
            BTreeMap::from([(s.bucket(3000), 7000)])
        );
    }
}

#[mz_ore::test]
fn local_missing_input_entry_understates() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut observed = VisibilityLog::default();
    observed.record(1001, Some(1000), 1100);
    observed.record(2001, Some(2000), 2200);
    let missing = VisibilityLog::default();
    let mut pruned = VisibilityLog::default();
    pruned.record(1001, Some(1000), 1100);
    pruned.record(2001, Some(2000), 2200);
    pruned.prune_through(1000);
    let s = scheme(4);
    let run = |log: &VisibilityLog| {
        let mut recorder = LocalDelayRecorder::new(0);
        let mut rows = Vec::new();
        for (frontier, now) in [(1001, 1500), (2001, 2500)] {
            rows.extend(
                recorder
                    .observe(
                        &tree,
                        &tree,
                        &at(frontier),
                        now,
                        &inputs_of(&tree, &[log]),
                        &cfg,
                    )
                    .local,
            );
        }
        rows.extend(
            recorder
                .observe(
                    &tree,
                    &tree,
                    &Antichain::new(),
                    3000,
                    &inputs_of(&tree, &[log]),
                    &cfg,
                )
                .local,
        );
        bucket_counts(&rows)
    };
    // With entries the delays are 400 ms and 300 ms.
    assert_eq!(
        run(&observed),
        BTreeMap::from([(s.bucket(400), 1000), (s.bucket(300), 1000)])
    );
    // Without any, the input counts as ready when the object advanced, a delay of 0.
    assert_eq!(run(&missing), BTreeMap::from([(s.bucket(1), 2000)]));
    // The pruned log answers the first window with its last pruned entry.
    assert_eq!(run(&pruned), run(&observed));
}

#[mz_ore::test]
fn local_input_entry_inside_window_is_not_split() {
    // The object has windows ending at 7000, 14000 and 21000. The input's entry at 15000 falls
    // inside the last one, which is only visible in the input with the entry at 21000.
    let tree = aligned(7000);
    let mut log = VisibilityLog::default();
    log.record(15_001, Some(15_000), 21_100);
    log.record(21_001, Some(21_000), 21_500);
    let mut recorder = LocalDelayRecorder::new(0);
    let cfg = config(4);
    let mut rows = recorder
        .observe(
            &tree,
            &tree,
            &at(21_001),
            22_000,
            &inputs_of(&tree, &[&log]),
            &cfg,
        )
        .local;
    rows.extend(
        recorder
            .observe(
                &tree,
                &tree,
                &Antichain::new(),
                22_000,
                &inputs_of(&tree, &[&log]),
                &cfg,
            )
            .local,
    );
    let s = scheme(4);
    // The first two windows are ready at 21100, the last at 21500.
    assert_eq!(
        bucket_counts(&rows),
        BTreeMap::from([(s.bucket(900), 14_000), (s.bucket(500), 7_000)])
    );
}

#[mz_ore::test]
fn local_negative_delay_clamps() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut log = VisibilityLog::default();
    log.record(1001, Some(1000), 5000);
    let mut recorder = LocalDelayRecorder::new(0);
    let mut rows = recorder
        .observe(
            &tree,
            &tree,
            &at(1001),
            1500,
            &inputs_of(&tree, &[&log]),
            &cfg,
        )
        .local;
    rows.extend(
        recorder
            .observe(
                &tree,
                &tree,
                &Antichain::new(),
                1500,
                &inputs_of(&tree, &[&log]),
                &cfg,
            )
            .local,
    );
    assert_eq!(
        bucket_counts(&rows),
        BTreeMap::from([(scheme(4).bucket(1), 1000)])
    );
}

/// A 1 s source feeds `refreshed`, a REFRESH EVERY 10 s object that shows each refresh 2 s after
/// it is due. A plain object `plain` reads `refreshed` and advances with it. Returns the local
/// rows of `refreshed` and of `plain`.
fn refresh_chain() -> (Vec<PeriodRow>, Vec<PeriodRow>) {
    let tree = refresh(every(10_000), aligned(1000));
    let inner = tree.below_refresh();
    let cfg = config(4);
    let mut log_a = VisibilityLog::default();
    let mut log_refreshed = VisibilityLog::default();
    let mut local_refreshed = LocalDelayRecorder::new(0);
    let mut local_plain = LocalDelayRecorder::new(0);
    let (mut rows_refreshed, mut rows_plain) = (vec![], vec![]);
    let mut next_a = 1000;
    for k in 1..=20u64 {
        let refresh_ts = 10_000 * k;
        let now = refresh_ts + 2000;
        // The source ticks every second up to the refresh and a bit beyond.
        while next_a <= now {
            log_a.record(next_a + 1, Some(next_a), next_a + 1);
            next_a += 1000;
        }
        let frontier = at(refresh_ts + 1);
        assert_eq!(
            tree.last_reflected(Timestamp::from(refresh_ts + 1)),
            Some(refresh_ts)
        );
        // The refresh view's own refresh is not part of what it needs from its input.
        let refreshed = local_refreshed.observe(
            &tree,
            inner,
            &frontier,
            now,
            &inputs_of(inner, &[&log_a]),
            &cfg,
        );
        rows_refreshed.extend(refreshed.local);
        log_refreshed.record(refresh_ts + 1, Some(refresh_ts), now);
        let plain = local_plain.observe(
            &tree,
            &tree,
            &frontier,
            now,
            &inputs_of(&tree, &[&log_refreshed]),
            &cfg,
        );
        rows_plain.extend(plain.local);
    }
    let empty = Antichain::new();
    let refreshed = local_refreshed.observe(
        &tree,
        inner,
        &empty,
        300_000,
        &inputs_of(inner, &[&log_a]),
        &cfg,
    );
    rows_refreshed.extend(refreshed.local);
    let plain = local_plain.observe(
        &tree,
        &tree,
        &empty,
        300_000,
        &inputs_of(&tree, &[&log_refreshed]),
        &cfg,
    );
    rows_plain.extend(plain.local);
    (rows_refreshed, rows_plain)
}

#[mz_ore::test]
fn local_refresh_object_carries_wait() {
    // The object is the refresh view over a plain input. An arrival ending at w is visible in
    // the input at w + 1 and in the object at the refresh time + 2 s, so the wait lands on it.
    let (rows, _) = refresh_chain();
    let s = scheme(4);
    assert_eq!(bucket_counts(&rows).values().sum::<u64>(), 200_000);
    let (lowest, highest) = (
        *bucket_counts(&rows).keys().next().unwrap(),
        *bucket_counts(&rows).keys().last().unwrap(),
    );
    // The newest window of a refresh waits 1999 ms, the oldest 10 s + 999 ms.
    assert_eq!(lowest, s.bucket(1999));
    assert_eq!(highest, s.bucket(10_999));
}

#[mz_ore::test]
fn local_refresh_input_does_not_leak_wait() {
    // A plain object over the refresh view sees the data when the view shows it, so it adds 0.
    let (_, rows) = refresh_chain();
    assert_eq!(
        bucket_counts(&rows),
        BTreeMap::from([(scheme(4).bucket(1), 200_000)])
    );
}

fn arb_local_tree() -> impl Strategy<Value = ResolvedTree> {
    proptest::sample::select(vec![
        aligned(1000),
        ResolvedTree::Min(vec![aligned(7000), aligned(11000)]),
        ResolvedTree::Leaf(Ticker::Continuous),
        ResolvedTree::Min(vec![unaligned(300), aligned(1000)]),
        refresh(every(5000), aligned(1000)),
        ResolvedTree::Min(vec![refresh(every(7000), aligned(1000)), aligned(3000)]),
    ])
}

fn arb_log() -> impl Strategy<Value = VisibilityLog> {
    proptest::collection::vec((1u64..30_000, 0u64..5_000, 0u64..40_000), 0..12).prop_map(|steps| {
        let (mut reflected, mut observed) = (0, 0);
        let mut log = VisibilityLog::default();
        for (dr, ahead, dt) in steps {
            reflected += dr;
            observed += dt;
            log.record(reflected + 1 + ahead, Some(reflected), observed);
        }
        log
    })
}

#[mz_ore::test]
fn local_counts_match_end_to_end() {
    let strategy = (
        arb_local_tree(),
        proptest::collection::vec((1u64..30_000, 0u64..40_000), 1..10),
        proptest::collection::vec(arb_log(), 0..3),
        proptest::sample::select(vec![1u64, 3]),
    );
    proptest!(ProptestConfig::with_cases(64), |((tree, steps, logs, interval_ms) in strategy)| {
        let cfg = RecorderConfig { interval_ms, period_ms: 10_000, ..config(4) };
        let needed = tree.below_refresh();
        let logs: Vec<&VisibilityLog> = logs.iter().collect();
        let inputs = inputs_of(needed, &logs);
        let mut end_to_end = FreshnessRecorder::new(0);
        let mut local = LocalDelayRecorder::new(0);
        let (mut rows_e2e, mut rows_local, mut rows_skew) = (vec![], vec![], vec![]);
        let (mut frontier, mut now) = (1, 0);
        let mut observe = |frontier: &Antichain<Timestamp>, now: u64| {
            rows_e2e.extend(end_to_end.observe(&tree, frontier, now, &cfg));
            let rows = local.observe(&tree, needed, frontier, now, &inputs, &cfg);
            rows_local.extend(rows.local);
            rows_skew.extend(rows.skew);
        };
        for (df, dt) in steps {
            frontier += df;
            now += dt;
            observe(&at(frontier), now);
        }
        observe(&Antichain::new(), now);
        prop_assert_eq!(period_totals(&rows_local), period_totals(&rows_e2e));
        if inputs.len() >= 2 {
            prop_assert_eq!(period_totals(&rows_skew), period_totals(&rows_e2e));
        } else {
            prop_assert!(rows_skew.is_empty());
        }
        if inputs.is_empty() {
            // Without inputs, local delay is the end-to-end latency.
            prop_assert_eq!(rows_local, rows_e2e);
        }
    });
}

#[mz_ore::test]
fn debug_output_stays_small() {
    let tree = ResolvedTree::Leaf(Ticker::Continuous);
    let mut log = VisibilityLog::default();
    for t in 1..10_000 {
        log.record(t + 1, Some(t), t);
    }
    let mut end_to_end = FreshnessRecorder::new(0);
    let mut local = LocalDelayRecorder::new(0);
    for t in 1..1_000 {
        let cfg = config(4);
        assert!(end_to_end.observe(&tree, &at(t + 1), t, &cfg).is_empty());
        assert_eq!(
            local.observe(
                &tree,
                &tree,
                &at(t + 1),
                t,
                &inputs_of(&tree, &[&log]),
                &cfg
            ),
            LocalRows::default()
        );
    }
    let end_to_end = format!("{end_to_end:?}");
    assert!(end_to_end.contains("open_periods: 1"), "{end_to_end}");
    let log = format!("{log:?}");
    assert!(log.contains("len: 9999"), "{log}");
    for debug in [end_to_end, format!("{local:?}"), log] {
        assert!(debug.len() < 1024, "{} bytes: {debug}", debug.len());
    }
}

/// Advances `book` and returns the closed periods with the arrivals added to each, and the
/// number of `open` and `add` calls.
fn advance_counted(
    book: &mut PeriodBook<u64>,
    to_ms: u64,
    now_ms: u64,
    cfg: &RecorderConfig,
) -> (Vec<(u64, u64, u64)>, usize, usize) {
    let (opens, adds) = (Cell::new(0), Cell::new(0));
    let closed = book.advance(
        to_ms,
        now_ms,
        cfg,
        || {
            opens.set(opens.get() + 1);
            0
        },
        |sum, from, to| {
            adds.set(adds.get() + 1);
            *sum += to - from;
        },
    );
    let closed = closed
        .into_iter()
        .map(|p| (p.start_ms, p.end_ms, p.state))
        .collect();
    (closed, opens.get(), adds.get())
}

/// A wall-clock time in 2026, a multiple of neither the period nor a day.
const NOW_MS: u64 = 1_793_262_731_123;

#[mz_ore::test]
fn period_book_catch_up_cuts_day_pieces() {
    let cfg = config(4);
    let stalled = |stall_days: u64| {
        let drop_ms = NOW_MS - stall_days * DAY_MS;
        let mut book = PeriodBook::new(drop_ms);
        let (closed, opens, adds) = advance_counted(&mut book, NOW_MS, NOW_MS, &cfg);
        (drop_ms, closed, opens, adds)
    };
    let (drop_ms, closed, opens, adds) = stalled(29);
    // Pieces are contiguous and cover the stall up to the last period boundary, which leaves
    // the rest in an open period.
    let covered_to = NOW_MS - NOW_MS % cfg.period_ms;
    let mut pos = drop_ms;
    for (start, end, sum) in &closed {
        assert!(*start <= pos && pos < *end, "gap before ({start}, {end}]");
        assert_eq!(*sum, end - pos, "({start}, {end}]");
        pos = *end;
    }
    assert_eq!(pos, covered_to);
    // The first period ends the minute of the drop, and the pieces in between end at days.
    let drop_period_end = drop_ms.div_ceil(cfg.period_ms) * cfg.period_ms;
    assert_eq!(closed[0].1, drop_period_end);
    for (start, end, _) in &closed[1..closed.len() - 1] {
        assert_eq!(end % DAY_MS, 0, "({start}, {end}]");
        assert!(end - start <= DAY_MS);
    }
    assert!(
        (30..=32).contains(&closed.len()),
        "{} periods",
        closed.len()
    );
    assert_eq!(opens, closed.len() + 1);
    assert_eq!(adds, opens);
    // A stall longer than the retention does no more work.
    let (_, longer, longer_opens, _) = stalled(90);
    assert!(longer.len() <= closed.len() + 2, "{} periods", longer.len());
    assert!(longer_opens <= opens + 2);
}

#[mz_ore::test]
fn period_book_short_span_keeps_periods() {
    let cfg = config(4);
    let start = NOW_MS - 3_600_000;
    let mut book = PeriodBook::new(start);
    let (closed, _, _) = advance_counted(&mut book, NOW_MS, NOW_MS, &cfg);
    // One hour is not a catch-up yet, so every closed period after the first, which starts at
    // the recording start, has the configured length.
    assert_eq!(closed[0].0, start);
    assert_eq!(closed[0].1 % cfg.period_ms, 0);
    assert!(closed[1..].iter().all(|(s, e, _)| e - s == cfg.period_ms));
    assert_eq!(closed.len(), 60);
}

#[mz_ore::test]
fn period_book_skips_arrivals_before_retention() {
    let cfg = config(4);
    let mut book = PeriodBook::new(0);
    let (closed, _, _) = advance_counted(&mut book, NOW_MS, NOW_MS, &cfg);
    let floor = NOW_MS - RETENTION_MS;
    assert!(closed.iter().all(|(_, end, _)| *end > floor));
    let total: u64 = closed.iter().map(|(_, _, sum)| sum).sum();
    assert_eq!(total, NOW_MS - NOW_MS % cfg.period_ms - floor);
    assert!(closed.len() <= 32, "{} periods", closed.len());
    // A frontier entirely older than the retention records nothing.
    let mut book = PeriodBook::new(0);
    let (closed, opens, _) = advance_counted(&mut book, floor - 1, NOW_MS, &cfg);
    assert!(closed.is_empty());
    assert_eq!(opens, 0);
}

#[mz_ore::test]
fn period_book_caps_frontier_ahead_of_wall_clock() {
    let cfg = config(4);
    let mut book = PeriodBook::new(NOW_MS - 30_000);
    let far = NOW_MS + 75 * 365 * DAY_MS;
    let (closed, opens, _) = advance_counted(&mut book, far, NOW_MS, &cfg);
    let cap = NOW_MS + cfg.period_ms;
    assert!(closed.iter().all(|(_, end, _)| *end <= cap));
    assert!(opens <= 2, "{opens} periods opened");
    // At the same wall-clock time, the cap is reached already.
    let (closed, opens, _) = advance_counted(&mut book, far, NOW_MS, &cfg);
    assert!(closed.is_empty() && opens == 0);
    // The next observation continues from the cap.
    let later = NOW_MS + 10 * cfg.period_ms;
    let (closed, opens, _) = advance_counted(&mut book, far, later, &cfg);
    assert!(
        closed
            .iter()
            .all(|(_, end, _)| *end <= later + cfg.period_ms)
    );
    assert!(opens <= 11, "{opens} periods opened");
}

#[mz_ore::test]
fn replica_outage_catch_up_is_exact_and_bounded() {
    let tree = aligned(1000);
    let cfg = config(4);
    let start = NOW_MS - NOW_MS % 1000;
    let mut recorder = FreshnessRecorder::new(start);
    let mut rows = Vec::new();
    let mut drop_ms = start;
    // Steady state for ten minutes, then the replica goes away.
    for w in (start + 1000..=start + 600_500).step_by(1000) {
        rows.extend(recorder.observe(&tree, &at(w + 1), w + 1, &cfg));
        drop_ms = w;
    }
    let last_end = rows.iter().map(|r| r.period_end_ms).max().unwrap();
    assert!(last_end <= drop_ms && drop_ms - last_end < cfg.period_ms);
    // A new replica catches up thirty days later.
    let now = drop_ms + 30 * DAY_MS;
    let caught_up = recorder.observe(&tree, &at(now + 1), now, &cfg);
    let mut per_period: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for r in &caught_up {
        *per_period
            .entry((r.period_start_ms, r.period_end_ms))
            .or_default() += r.count;
    }
    // Every piece holds exactly its arrivals, one per ms, from the end of the last row on.
    let mut pos = last_end;
    for ((start, end), count) in &per_period {
        assert!(*start <= pos && pos < *end, "gap before ({start}, {end}]");
        assert_eq!(*count, end - pos, "({start}, {end}]");
        pos = *end;
    }
    assert_eq!(pos, now - now % cfg.period_ms);
    assert!((30..=33).contains(&per_period.len()), "{per_period:?}");
    // About 100 rows: the pieces times the buckets they touch.
    assert!(caught_up.len() <= 200, "{} rows", caught_up.len());
    // The catch-up is latency: the oldest arrivals waited thirty days.
    let oldest = caught_up.iter().map(|r| r.bucket).max().unwrap();
    assert_eq!(oldest, cfg.scheme.bucket(30 * DAY_MS));
}

#[mz_ore::test]
fn finish_emits_open_periods() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut recorder = FreshnessRecorder::new(0);
    let mut local = LocalDelayRecorder::new(0);
    assert!(
        recorder
            .observe(&tree, &at(30_001), 30_001, &cfg)
            .is_empty()
    );
    assert_eq!(
        local.observe(&tree, &tree, &at(30_001), 30_001, &[], &cfg),
        LocalRows::default()
    );
    let rows = recorder.finish();
    assert_eq!(period_totals(&rows), BTreeMap::from([(0, 30_000)]));
    assert_eq!(
        local.finish(),
        LocalRows {
            local: rows,
            skew: vec![]
        }
    );
    assert!(
        recorder
            .observe(&tree, &at(90_001), 90_001, &cfg)
            .is_empty()
    );
    assert!(recorder.finish().is_empty());
}

#[mz_ore::test]
fn local_long_input_outage_uses_last_pruned_observation() {
    let tree = aligned(1000);
    let cfg = config(4);
    let start = NOW_MS - NOW_MS % 1000 - 2 * 3_600_000;
    let end = start + 2 * 3_600_000;
    // The input shows each window 100 ms after it ends, for two hours, while the object is stuck
    // at `start`. The input's log keeps the entries of the last ten minutes.
    let mut log = VisibilityLog::default();
    for w in (start + 1000..=end).step_by(1000) {
        log.record(w + 1, Some(w), w + 100);
    }
    let now = end + 100;
    let cutoff = now - 600_000;
    log.prune_through(cutoff - 1100);
    let mut recorder = LocalDelayRecorder::new(start);
    let mut rows = recorder
        .observe(
            &tree,
            &tree,
            &at(end + 1),
            now,
            &inputs_of(&tree, &[&log]),
            &cfg,
        )
        .local;
    rows.extend(recorder.finish().local);
    let counts = bucket_counts(&rows);
    let total: u64 = counts.values().sum();
    assert_eq!(total, end - start);
    // The pruned arrivals were visible in the input at the latest when its last pruned entry
    // was observed, just before the cutoff, not when the object advanced.
    let last_pruned_observed = cutoff - 1000;
    let pruned: u64 = counts
        .range(cfg.scheme.bucket(now - last_pruned_observed)..)
        .map(|(_, c)| c)
        .sum();
    assert!(pruned >= end - start - 600_000, "{counts:?}");
    assert!(counts.get(&0).copied().unwrap_or(0) <= 1000, "{counts:?}");
}

#[mz_ore::test]
fn period_book_cuts_open_periods_at_a_length_change() {
    let long = RecorderConfig {
        period_ms: 60_000,
        ..config(4)
    };
    let short = RecorderConfig {
        period_ms: 1_000,
        ..config(4)
    };
    let mut book = PeriodBook::new(0);
    let (closed, _, _) = advance_counted(&mut book, 30_000, 30_001, &long);
    assert_eq!(closed, vec![]);
    // The open (0, 60 s] period is cut at the change and handed out with its 30 s of arrivals,
    // before the periods of the new length.
    let (closed, _, _) = advance_counted(&mut book, 31_000, 31_001, &short);
    assert_eq!(closed, vec![(0, 30_000, 30_000), (30_000, 31_000, 1_000)]);
    let (mut later, _, _) = advance_counted(&mut book, 95_000, 95_001, &short);
    later.extend(
        book.finish()
            .into_iter()
            .map(|p| (p.start_ms, p.end_ms, p.state)),
    );
    assert!(
        later.iter().all(|(start, _, _)| *start >= 30_000),
        "{later:?}"
    );
    assert!(later.windows(2).all(|w| w[0].1 <= w[1].1), "{later:?}");
    let total: u64 = later.iter().map(|(_, _, count)| count).sum();
    assert_eq!(total, 95_000 - 31_000);
}

#[mz_ore::test]
fn period_book_cuts_short_periods_before_long_ones() {
    let short = RecorderConfig {
        period_ms: 1_000,
        ..config(4)
    };
    let long = RecorderConfig {
        period_ms: 60_000,
        ..config(4)
    };
    let mut book = PeriodBook::new(0);
    let (closed, _, _) = advance_counted(&mut book, 30_500, 30_501, &short);
    assert_eq!(closed.len(), 30);
    // The open (30 s, 31 s] period is cut at 30.5 s, and the first long period starts there
    // instead of at 0, which would overlap the short ones handed out.
    let (closed, _, _) = advance_counted(&mut book, 70_000, 70_001, &long);
    assert_eq!(
        closed,
        vec![(30_000, 30_500, 500), (30_500, 60_000, 29_500)]
    );
    let rest: Vec<_> = book
        .finish()
        .into_iter()
        .map(|p| (p.start_ms, p.end_ms, p.state))
        .collect();
    assert_eq!(rest, vec![(60_000, 120_000, 10_000)]);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn period_book_tiles_in_order_across_length_changes() {
    const HOUR_MS: u64 = 60 * 60 * 1000;
    let step = (
        // Mostly short advances, some long enough to be accounted as a catch-up.
        Union::new([(1u64..200_000).boxed(), (HOUR_MS..30 * HOUR_MS).boxed()]),
        select(vec![1_000u64, 7_000, 10_000, 60_000, 300_000]),
        // The wall clock behind or ahead of the frontier, which caps the high-water mark.
        -400_000i64..400_000,
        select(vec![RETENTION_MS, 2 * HOUR_MS, 10 * 60 * 1000]),
    );
    let strategy = (0u64..10 * DAY_MS, proptest::collection::vec(step, 1..20));
    proptest!(|((start, steps) in strategy)| {
        let mut book = PeriodBook::new(start);
        let mut handed_out = Vec::new();
        // The arrival spans `(from, to]` the book must count.
        let mut counted: Vec<(u64, u64)> = Vec::new();
        let (mut to, mut high_water) = (start, start);
        for (advance, period_ms, now_delta, retention_ms) in steps {
            to += advance;
            let now = to.saturating_add_signed(now_delta);
            let cfg = RecorderConfig { period_ms, retention_ms, ..config(4) };
            let cap = now + period_ms;
            let reached = to.min(cap - cap % period_ms);
            if reached > high_water {
                let from = high_water.max(now.saturating_sub(retention_ms));
                if from < reached {
                    counted.push((from, reached));
                }
                high_water = reached;
            }
            let (closed, _, _) = advance_counted(&mut book, to, now, &cfg);
            handed_out.extend(closed);
        }
        handed_out.extend(book.finish().into_iter().map(|p| (p.start_ms, p.end_ms, p.state)));
        // In order, without overlap, starting at the recording start, and every counted
        // arrival in the period it falls into.
        let mut end = start;
        for (period_start, period_end, count) in &handed_out {
            prop_assert!(*period_start >= end, "{:?}", handed_out);
            prop_assert!(*period_end > *period_start, "{:?}", handed_out);
            let expected: u64 = counted
                .iter()
                .map(|(from, to)| {
                    to.min(period_end).saturating_sub(*from.max(period_start))
                })
                .sum();
            prop_assert_eq!(*count, expected, "({}, {}] {:?}", period_start, period_end, counted);
            end = *period_end;
        }
        let total: u64 = handed_out.iter().map(|(_, _, count)| count).sum();
        let expected: u64 = counted.iter().map(|(from, to)| to - from).sum();
        prop_assert_eq!(total, expected);
    });
}

#[mz_ore::test]
fn period_book_starts_at_an_off_grid_start() {
    // A restart seeded at the end of a cut period, off the grid of 60 s periods.
    let mut book = PeriodBook::new(30_500);
    let (closed, _, _) = advance_counted(&mut book, 130_000, 130_001, &config(4));
    assert_eq!(
        closed,
        vec![(30_500, 60_000, 29_500), (60_000, 120_000, 60_000)]
    );
}

#[mz_ore::test]
fn visibility_log_retention_keeps_the_last_entry() {
    let mut log = VisibilityLog::default();
    log.record(1001, Some(1000), 1010);
    log.record(2001, Some(2000), 2020);
    log.record(3001, Some(3000), 3030);
    log.prune_observed_before(2500);
    // Pruned arrivals are answered by the last pruned entry.
    assert_eq!(log.visible_at(1000), Some(2020));
    assert_eq!(log.visible_at(1001), Some(2020));
    assert_eq!(log.visible_at(2001), Some(3030));
    // Everything is older than the bound, but the last entry stays.
    log.prune_observed_before(10_000);
    assert_eq!(log.visible_at(2001), Some(3030));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn visibility_log_after_answers_later_arrivals() {
    let strategy = (arb_log(), 0u64..300_000, 0u64..300_000);
    proptest!(|((log, after, arrival) in strategy)| {
        let copy = log.after(after);
        if arrival > after {
            prop_assert_eq!(copy.visible_at(arrival), log.visible_at(arrival));
        }
        // A timestamp is never answered earlier than by the full log.
        if let (Some(copied), Some(full)) = (copy.passed_at(arrival), log.passed_at(arrival)) {
            prop_assert!(copied >= full, "{copied} < {full}");
        }
    });
}

#[mz_ore::test]
fn local_recorder_reports_high_water() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut local = LocalDelayRecorder::new(500);
    assert_eq!(local.high_water_ms(), 500);
    local.observe(&tree, &tree, &at(3001), 3005, &[], &cfg);
    assert_eq!(local.high_water_ms(), 3000);
}

/// A frontier history of one collection, `(observed_ms, frontier)` ascending in both.
type History = Vec<(u64, u64)>;

/// Appends an observation. An observation before the last one is moved to it, and one that does
/// not advance the frontier is dropped.
fn push_advance(history: &mut History, observed_ms: u64, frontier: u64) {
    match history.last_mut() {
        Some((_, f)) if frontier <= *f => {}
        Some((o, f)) if observed_ms <= *o => *f = frontier,
        _ => history.push((observed_ms, frontier)),
    }
}

/// A leaf that ticks every `interval` ms through `end`, observed `delay` ms after each tick and
/// `ahead` ms early against the wall clock. Observations that fall into `stall` wait for its end.
fn leaf_history(
    interval: u64,
    delay: u64,
    end: u64,
    stall: Option<(u64, u64)>,
    ahead: u64,
) -> History {
    let mut history = Vec::new();
    let mut tick = interval;
    while tick <= end {
        let mut observed = tick + delay;
        if let Some((from, to)) = stall
            && (from..to).contains(&observed)
        {
            observed = to + delay;
        }
        push_advance(&mut history, observed.saturating_sub(ahead), tick + 1);
        tick += interval;
    }
    history
}

/// The frontier of `history` at wall clock `w`, 0 before its first observation.
fn frontier_at(history: &History, w: u64) -> u64 {
    match history.partition_point(|(o, _)| *o <= w) {
        0 => 0,
        idx => history[idx - 1].1,
    }
}

/// An object whose frontier is the least of its inputs' frontiers, observed `delay` ms after the
/// input observation that advanced it.
fn derived_history(inputs: &[&History], delay: u64) -> History {
    let mut times: Vec<u64> = inputs
        .iter()
        .flat_map(|h| h.iter().map(|(o, _)| *o))
        .collect();
    times.sort();
    times.dedup();
    let mut history = Vec::new();
    for t in times {
        let frontier = inputs.iter().map(|h| frontier_at(h, t)).min().unwrap();
        if frontier > 0 {
            push_advance(&mut history, t + delay, frontier);
        }
    }
    history
}

/// `history` with the observations in `[from, to)` held back until `to`.
fn held(history: &History, (from, to): (u64, u64)) -> History {
    let mut out = Vec::new();
    for (o, f) in history {
        let o = if (from..to).contains(o) { to } else { *o };
        push_advance(&mut out, o, *f);
    }
    out
}

fn log_of(tree: &ResolvedTree, history: &History) -> VisibilityLog {
    let mut log = VisibilityLog::default();
    for (o, f) in history {
        log.record(*f, tree.last_reflected(Timestamp::from(*f)), *o);
    }
    log
}

#[derive(Debug, Default)]
struct Recorded {
    end_to_end: Vec<PeriodRow>,
    local: Vec<PeriodRow>,
    skew: Vec<PeriodRow>,
    /// The last recorded arrival.
    high_water_ms: u64,
}

/// Records an object with `tree` and `needed` from `start_ms` on over its `history`, with inputs
/// whose logs hold all their observations up front.
fn record_history(
    tree: &ResolvedTree,
    needed: &ResolvedTree,
    history: &History,
    inputs: &[(&ResolvedTree, &History)],
    start_ms: u64,
) -> Recorded {
    let cfg = config(4);
    let logs: Vec<VisibilityLog> = inputs.iter().map(|(t, h)| log_of(t, h)).collect();
    let logs: Vec<LocalInput<'_>> = inputs
        .iter()
        .zip_eq(logs.iter())
        .map(|((tree, _), log)| LocalInput { tree, log })
        .collect();
    let mut end_to_end = FreshnessRecorder::new(start_ms);
    let mut local = LocalDelayRecorder::new(start_ms);
    let mut recorded = Recorded::default();
    for (o, f) in history {
        recorded
            .end_to_end
            .extend(end_to_end.observe(tree, &at(*f), *o, &cfg));
        let rows = local.observe(tree, needed, &at(*f), *o, &logs, &cfg);
        recorded.local.extend(rows.local);
        recorded.skew.extend(rows.skew);
    }
    recorded.high_water_ms = local.high_water_ms();
    recorded.end_to_end.extend(end_to_end.finish());
    let rows = local.finish();
    recorded.local.extend(rows.local);
    recorded.skew.extend(rows.skew);
    recorded
}

/// The split of one arrival's end-to-end charge, from complete histories.
#[derive(Debug)]
struct Truth {
    arrival: u64,
    window_start: u64,
    /// The first observation of the object that reflects the arrival.
    observed_ms: u64,
    /// Its time as the end-to-end latency charges it.
    advance_ms: u64,
    arrival_ready_ms: u64,
    timestamp_ready_ms: u64,
    /// The skew with every input on time, the cap of skew.
    cap_ms: u64,
}

impl Truth {
    fn new(
        tree: &ResolvedTree,
        needed: &ResolvedTree,
        history: &History,
        inputs: &[(&ResolvedTree, &History)],
        cap: &mut dyn FnMut(u64, u64) -> u64,
        arrival: u64,
    ) -> Self {
        let reflects = |tree: &ResolvedTree, f: u64| {
            tree.last_reflected(Timestamp::from(f))
                .is_some_and(|r| r >= arrival)
        };
        let (observed_ms, frontier) =
            history[history.partition_point(|(_, f)| !reflects(tree, *f))];
        let advance_ms = observed_ms.max(tree.last_reflected(Timestamp::from(frontier)).unwrap());
        let timestamp = needed.assigned(arrival).unwrap();
        let (mut arrival_ready_ms, mut timestamp_ready_ms) = (0, 0);
        for (input, h) in inputs {
            let shown = h.partition_point(|(_, f)| !reflects(input, *f));
            arrival_ready_ms = arrival_ready_ms.max(h.get(shown).map_or(observed_ms, |(o, _)| *o));
            let passed = h.partition_point(|(_, f)| *f <= timestamp);
            timestamp_ready_ms =
                timestamp_ready_ms.max(h.get(passed).map_or(observed_ms, |(o, _)| *o));
        }
        // An input observed after the object showed the arrival cannot have delayed it beyond
        // that observation, and one that showed it before its window started was ready no
        // earlier than the window start.
        let window_start = tree.prev_boundary(arrival - 1);
        let arrival_ready_ms = arrival_ready_ms.min(observed_ms).max(window_start);
        let timestamp_ready_ms = timestamp_ready_ms.min(observed_ms).max(arrival_ready_ms);
        Truth {
            arrival,
            window_start,
            observed_ms,
            advance_ms,
            arrival_ready_ms,
            timestamp_ready_ms,
            cap_ms: cap(arrival, timestamp),
        }
    }

    /// The truths of the arrivals in `(from, to]`, with the cap from each input's on-time
    /// frontier, searched in `expected_upper` of its tree.
    fn all(
        tree: &ResolvedTree,
        needed: &ResolvedTree,
        history: &History,
        inputs: &[(&ResolvedTree, &History)],
        span: (u64, u64),
    ) -> Vec<Self> {
        let models: Vec<ResolvedTree> = inputs
            .iter()
            .map(|(t, _)| unaligned_as_continuous(t))
            .collect();
        // The shows of an aligned input are equal within a window of the object, so they are
        // searched once per window.
        let per_window = models
            .iter()
            .all(|m| !m.tickers().contains(&Ticker::Continuous));
        let mut shown_cache: Option<(u64, u64)> = None;
        let mut cap = |arrival: u64, timestamp: u64| {
            let passed = models
                .iter()
                .map(|m| on_time_first(m, |f| f > timestamp))
                .max()
                .unwrap();
            let window = tree.prev_boundary(arrival - 1);
            let shown = match shown_cache {
                Some((w, shown)) if per_window && w == window => shown,
                _ => {
                    let shown = models
                        .iter()
                        .map(|m| {
                            on_time_first(m, |f| {
                                m.last_reflected(Timestamp::from(f))
                                    .is_some_and(|r| r >= arrival)
                            })
                        })
                        .max()
                        .unwrap();
                    shown_cache = Some((window, shown));
                    shown
                }
            };
            passed.saturating_sub(shown)
        };
        Truth::all_with(tree, needed, history, inputs, &mut cap, span)
    }

    /// The truths of the arrivals in `(from, to]`, with the cap `cap(arrival, timestamp)`.
    fn all_with(
        tree: &ResolvedTree,
        needed: &ResolvedTree,
        history: &History,
        inputs: &[(&ResolvedTree, &History)],
        cap: &mut dyn FnMut(u64, u64) -> u64,
        (from, to): (u64, u64),
    ) -> Vec<Self> {
        (from + 1..=to)
            .map(|a| Truth::new(tree, needed, history, inputs, cap, a))
            .collect()
    }

    fn end_to_end_ms(&self) -> u64 {
        self.advance_ms - self.window_start
    }

    /// The rest of the end-to-end charge. Panics if skew and local delay exceed it.
    fn upstream_ms(&self) -> u64 {
        let split = self.skew_ms() + self.local_ms();
        self.end_to_end_ms()
            .checked_sub(split)
            .unwrap_or_else(|| panic!("skew and local exceed end to end: {self:?}"))
    }

    /// The time from the window start until the slowest input showed the arrival, a lower
    /// bound of upstream.
    fn arrival_upstream_ms(&self) -> u64 {
        self.arrival_ready_ms - self.window_start
    }

    /// The wait for the inputs' frontiers after they all showed the arrival.
    fn raw_skew_ms(&self) -> u64 {
        self.timestamp_ready_ms - self.arrival_ready_ms
    }

    fn skew_ms(&self) -> u64 {
        self.raw_skew_ms().min(self.cap_ms)
    }

    fn local_ms(&self) -> u64 {
        let ready = self.timestamp_ready_ms.max(self.arrival_ready_ms);
        self.observed_ms.saturating_sub(ready)
    }
}

/// The histogram of `latencies`, with 0 in the bucket of 1 ms as the recorders count it.
fn histogram(latencies: impl Iterator<Item = u64>) -> BTreeMap<i32, u64> {
    let s = scheme(4);
    let mut counts = BTreeMap::new();
    for latency in latencies {
        *counts.entry(s.bucket(latency.max(1))).or_default() += 1;
    }
    counts
}

/// Whether `low` counts as many latencies as `high` and at most as many at or above every
/// bucket, which holds if every latency of `low` is at most its counterpart in `high`.
fn dominated_by(low: &BTreeMap<i32, u64>, high: &BTreeMap<i32, u64>) -> bool {
    let at_or_above = |m: &BTreeMap<i32, u64>, b: i32| m.range(b..).map(|(_, c)| *c).sum::<u64>();
    low.keys()
        .chain(high.keys())
        .chain([&i32::MIN])
        .all(|b| at_or_above(low, *b) <= at_or_above(high, *b))
        && at_or_above(low, i32::MIN) == at_or_above(high, i32::MIN)
}

/// A tree that ticks on time. Independent of the recorder's model, it knows the phase of its
/// unaligned leaves.
#[derive(Debug, Clone)]
enum OnTimeInput {
    /// Ticks every `interval_ms` at `phase_ms`, aligned to the epoch without a phase.
    Leaf {
        interval_ms: u64,
        phase_ms: Option<u64>,
    },
    Min(Vec<OnTimeInput>),
    /// Refreshes every `refresh_ms`.
    Refresh(u64, Box<OnTimeInput>),
}

impl OnTimeInput {
    /// A join of leaves `(interval_ms, phase_ms)`, optionally under a refresh every
    /// `refresh_ms`.
    fn join(leaves: &[(u64, Option<u64>)], refresh_ms: Option<u64>) -> Self {
        let mut leaves: Vec<_> = leaves
            .iter()
            .map(|(interval_ms, phase_ms)| OnTimeInput::Leaf {
                interval_ms: *interval_ms,
                phase_ms: *phase_ms,
            })
            .collect();
        let inner = match leaves.len() {
            1 => leaves.pop().unwrap(),
            _ => OnTimeInput::Min(leaves),
        };
        match refresh_ms {
            None => inner,
            Some(p) => OnTimeInput::Refresh(p, Box::new(inner)),
        }
    }

    fn tree(&self) -> ResolvedTree {
        match self {
            OnTimeInput::Leaf {
                interval_ms,
                phase_ms: None,
            } => aligned(*interval_ms),
            OnTimeInput::Leaf { interval_ms, .. } => unaligned(*interval_ms),
            OnTimeInput::Min(inputs) => {
                ResolvedTree::Min(inputs.iter().map(OnTimeInput::tree).collect())
            }
            OnTimeInput::Refresh(p, input) => refresh(every(*p), input.tree()),
        }
    }

    /// The frontier at wall clock `w`. A leaf's tick at `t` leaves it at `t + 1`, and a refresh
    /// advances to its next refresh once its input passed the previous one.
    fn frontier(&self, w: u64) -> u64 {
        match self {
            OnTimeInput::Leaf {
                interval_ms,
                phase_ms,
            } => {
                let phase = phase_ms.unwrap_or(0);
                if w >= phase {
                    phase + (w - phase) / interval_ms * interval_ms + 1
                } else {
                    1
                }
            }
            OnTimeInput::Min(inputs) => inputs.iter().map(|i| i.frontier(w)).min().unwrap(),
            OnTimeInput::Refresh(p, input) => ceil_to(input.frontier(w), 0, *p),
        }
    }

    /// The timestamp `arrival` gets: the latest of the leaves' next ticks, rounded up to the
    /// next refresh of every refresh above them.
    fn assigned(&self, arrival: u64) -> u64 {
        match self {
            OnTimeInput::Leaf {
                interval_ms,
                phase_ms,
            } => ceil_to(arrival, phase_ms.unwrap_or(0), *interval_ms),
            OnTimeInput::Min(inputs) => inputs.iter().map(|i| i.assigned(arrival)).max().unwrap(),
            OnTimeInput::Refresh(p, input) => ceil_to(input.assigned(arrival), 0, *p),
        }
    }

    /// The first wall clock at which the frontier is past `timestamp`.
    fn passes(&self, timestamp: u64) -> u64 {
        let (mut lo, mut hi) = (0, timestamp.saturating_add(1 << 30));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.frontier(mid) > timestamp {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        lo
    }

    /// The first wall clock at which the frontier reflects `arrival`.
    fn shows(&self, arrival: u64) -> u64 {
        self.passes(self.assigned(arrival))
    }

    /// Adds the tick times of the leaves up to `end` to `ticks`.
    fn ticks(&self, end: u64, ticks: &mut Vec<u64>) {
        match self {
            OnTimeInput::Leaf {
                interval_ms,
                phase_ms,
            } => {
                let mut t = phase_ms.unwrap_or(0);
                while t <= end {
                    ticks.push(t);
                    t += interval_ms;
                }
            }
            OnTimeInput::Min(inputs) => inputs.iter().for_each(|i| i.ticks(end, ticks)),
            OnTimeInput::Refresh(_, input) => input.ticks(end, ticks),
        }
    }

    /// The frontier history up to `end`, each advance observed `delay` ms after it, held in
    /// `stall` until its end.
    fn history(&self, end: u64, delay: u64, stall: Option<(u64, u64)>) -> History {
        let mut ticks = Vec::new();
        self.ticks(end, &mut ticks);
        ticks.sort();
        ticks.dedup();
        let mut history = Vec::new();
        for t in ticks {
            let mut observed = t + delay;
            if let Some((from, to)) = stall
                && (from..to).contains(&observed)
            {
                observed = to + delay;
            }
            push_advance(&mut history, observed, self.frontier(t));
        }
        history
    }
}

/// The skew of `arrival` with every input on time, from the last input showing it until every
/// input passes `timestamp`, the timestamp the object needs.
fn on_time_cap(inputs: &[OnTimeInput], arrival: u64, timestamp: u64) -> u64 {
    let passed = inputs.iter().map(|i| i.passes(timestamp)).max().unwrap();
    let shown = inputs.iter().map(|i| i.shows(arrival)).max().unwrap();
    passed.saturating_sub(shown)
}

/// Checks that `rows` have, per period of length `period_ms`, the same periods and counts as
/// the `latency` of `truths`, and never more mass at or above any bucket.
fn never_exceeds(
    rows: &[PeriodRow],
    truths: &[Truth],
    period_ms: u64,
    latency: fn(&Truth) -> u64,
) -> Result<(), String> {
    let mut recorded: BTreeMap<u64, BTreeMap<i32, u64>> = BTreeMap::new();
    for row in rows {
        *recorded
            .entry(row.period_start_ms)
            .or_default()
            .entry(row.bucket)
            .or_default() += row.count;
    }
    let mut latencies: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for truth in truths {
        let start = (truth.arrival - 1) / period_ms * period_ms;
        latencies.entry(start).or_default().push(latency(truth));
    }
    let expected: BTreeMap<u64, BTreeMap<i32, u64>> = latencies
        .into_iter()
        .map(|(p, l)| (p, histogram(l.into_iter())))
        .collect();
    if recorded.keys().ne(expected.keys()) {
        return Err(format!("periods {recorded:?} != {expected:?}"));
    }
    for (p, counts) in &recorded {
        if !dominated_by(counts, &expected[p]) {
            return Err(format!("{p}: {counts:?} > {:?}", expected[p]));
        }
    }
    Ok(())
}

/// A join of a 7 s and an 11 s source, each observed 10 ms after its tick, which the join shows
/// 20 ms after the input observation that advances it. Returns the trees and histories.
fn join_7_11(
    stall_11: Option<(u64, u64)>,
    end: u64,
) -> (ResolvedTree, [(ResolvedTree, History); 2]) {
    let (l7, l11) = (aligned(7000), aligned(11_000));
    let join = ResolvedTree::Min(vec![l7.clone(), l11.clone()]);
    let h7 = leaf_history(7000, 10, end, None, 0);
    let h11 = leaf_history(11_000, 10, end, stall_11, 0);
    (join, [(l7, h7), (l11, h11)])
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_join_charges_alignment_to_skew() {
    let (join, inputs) = join_7_11(None, 400_000);
    let inputs: Vec<_> = inputs.iter().map(|(t, h)| (t, h)).collect();
    let history = derived_history(&[inputs[0].1, inputs[1].1], 20);
    let recorded = record_history(&join, &join, &history, &inputs, 120_000);
    let span = (120_000, recorded.high_water_ms);
    let truths = Truth::all(&join, &join, &history, &inputs, span);
    let s = scheme(4);
    // The join adds its 20 ms to every arrival.
    assert_eq!(
        bucket_counts(&recorded.local),
        BTreeMap::from([(s.bucket(20), span.1 - span.0)])
    );
    // The wait for the inputs' frontiers to align is skew, up to 11 s - gcd(7 s, 11 s).
    assert_eq!(
        bucket_counts(&recorded.skew),
        histogram(truths.iter().map(Truth::skew_ms))
    );
    assert_eq!(truths.iter().map(Truth::skew_ms).max(), Some(10_000));
    assert!(bucket_counts(&recorded.skew).len() >= 4, "{recorded:?}");
    assert_eq!(
        bucket_counts(&recorded.end_to_end),
        histogram(truths.iter().map(Truth::end_to_end_ms))
    );
    for truth in &truths {
        // With every input on time the cap never binds, and upstream is the time until the
        // slowest input showed the arrival.
        assert_eq!(truth.raw_skew_ms(), truth.skew_ms(), "{truth:?}");
        assert_eq!(
            truth.upstream_ms(),
            truth.arrival_upstream_ms(),
            "{truth:?}"
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_join_input_stall_is_upstream() {
    // The 11 s input's observations stall for 5 minutes.
    let (join, inputs) = join_7_11(Some((130_000, 430_000)), 600_000);
    let inputs: Vec<_> = inputs.iter().map(|(t, h)| (t, h)).collect();
    let history = derived_history(&[inputs[0].1, inputs[1].1], 20);
    let recorded = record_history(&join, &join, &history, &inputs, 120_000);
    let span = (120_000, recorded.high_water_ms);
    let truths = Truth::all(&join, &join, &history, &inputs, span);
    let s = scheme(4);
    assert_eq!(
        bucket_counts(&recorded.local),
        BTreeMap::from([(s.bucket(20), span.1 - span.0)])
    );
    assert_eq!(
        bucket_counts(&recorded.skew),
        histogram(truths.iter().map(Truth::skew_ms))
    );
    // An arrival that the 11 s input showed before its stall, but whose needed timestamp lies
    // past that input's last tick before the stall, waits for that input's frontier through
    // the stall. The cap keeps that wait in upstream.
    assert!(truths.iter().map(Truth::raw_skew_ms).max().unwrap() >= 290_000);
    for truth in &truths {
        assert!(truth.skew_ms() <= truth.cap_ms, "{truth:?}");
        assert!(truth.cap_ms <= 10_000, "{truth:?}");
    }
    let skew_max = bucket_counts(&recorded.skew).into_keys().last().unwrap();
    assert!(skew_max <= s.bucket(10_000), "{skew_max}");
    // The arrivals during the stall wait for the input to show them, its own latency, and
    // little else.
    assert!(truths.iter().map(Truth::upstream_ms).max().unwrap() >= 290_000);
    for truth in truths
        .iter()
        .filter(|t| (125_000..425_000).contains(&t.arrival))
    {
        assert!(
            truth.upstream_ms() + 10_020 >= truth.end_to_end_ms(),
            "{truth:?}"
        );
    }
    let e2e_max = bucket_counts(&recorded.end_to_end)
        .into_keys()
        .last()
        .unwrap();
    assert!(e2e_max >= s.bucket(290_000), "{e2e_max}");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_object_stall_is_local() {
    // The join stalls for 10 minutes while its inputs keep ticking.
    let (join, inputs) = join_7_11(None, 900_000);
    let inputs: Vec<_> = inputs.iter().map(|(t, h)| (t, h)).collect();
    let history = held(
        &derived_history(&[inputs[0].1, inputs[1].1], 20),
        (200_000, 800_000),
    );
    let recorded = record_history(&join, &join, &history, &inputs, 120_000);
    let span = (120_000, recorded.high_water_ms);
    let truths = Truth::all(&join, &join, &history, &inputs, span);
    let s = scheme(4);
    assert_eq!(
        bucket_counts(&recorded.local),
        histogram(truths.iter().map(Truth::local_ms))
    );
    assert!(truths.iter().map(Truth::local_ms).max().unwrap() >= 590_000);
    let skew_max = bucket_counts(&recorded.skew).into_keys().last().unwrap();
    assert!(skew_max <= s.bucket(10_000), "{skew_max}");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_input_that_is_a_join_stays_exact() {
    let (join, inputs) = join_7_11(None, 400_000);
    let h_join = derived_history(&[&inputs[0].1, &inputs[1].1], 20);
    let s = scheme(4);

    // An index on the join has one input and no skew.
    let index = derived_history(&[&h_join], 5);
    let inputs = [(&join, &h_join)];
    let recorded = record_history(&join, &join, &index, &inputs, 120_000);
    assert_eq!(
        bucket_counts(&recorded.local),
        BTreeMap::from([(s.bucket(5), recorded.high_water_ms - 120_000)])
    );
    assert_eq!(recorded.skew, vec![]);

    // A join of the join with a 3 s source needs timestamps under all three leaves, which the
    // inner join's frontier passes later than it shows the arrival.
    let l3 = aligned(3000);
    let h3 = leaf_history(3000, 10, 400_000, None, 0);
    let outer = ResolvedTree::Min(vec![join.clone(), l3.clone()]);
    let h_outer = derived_history(&[&h_join, &h3], 5);
    let inputs = [(&join, &h_join), (&l3, &h3)];
    let recorded = record_history(&outer, &outer, &h_outer, &inputs, 120_000);
    let span = (120_000, recorded.high_water_ms);
    let truths = Truth::all(&outer, &outer, &h_outer, &inputs, span);
    for truth in &truths {
        assert!(
            truth.timestamp_ready_ms >= truth.arrival_ready_ms,
            "{truth:?}"
        );
        assert_eq!(
            truth.upstream_ms() + truth.skew_ms() + truth.local_ms(),
            truth.end_to_end_ms(),
            "{truth:?}"
        );
    }
    assert_eq!(
        bucket_counts(&recorded.local),
        BTreeMap::from([(s.bucket(5), span.1 - span.0)])
    );
    assert_eq!(
        bucket_counts(&recorded.skew),
        histogram(truths.iter().map(Truth::skew_ms))
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_frontier_ahead_of_wall_clock_is_not_local() {
    // A 1 s source whose frontier runs 3 s ahead of the controller's clock, and an index that
    // shows it 5 ms later.
    let leaf = aligned(1000);
    let source = leaf_history(1000, 0, 400_000, None, 3000);
    let index = derived_history(&[&source], 5);
    let inputs = [(&leaf, &source)];
    let recorded = record_history(&leaf, &leaf, &index, &inputs, 120_000);
    let truths = Truth::all(
        &leaf,
        &leaf,
        &index,
        &inputs,
        (120_000, recorded.high_water_ms),
    );
    // The index shows each arrival 5 ms after its source, which is before the arrival's window
    // starts, so it adds nothing to the end-to-end charge.
    let local = bucket_counts(&recorded.local);
    assert_eq!(local, histogram(truths.iter().map(Truth::local_ms)));
    assert!(local.keys().all(|b| *b <= scheme(4).bucket(5)), "{local:?}");
    assert_eq!(recorded.skew, vec![]);
}

#[mz_ore::test]
fn split_single_input_records_no_skew() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut log = VisibilityLog::default();
    let mut recorder = LocalDelayRecorder::new(0);
    let mut rows = LocalRows::default();
    for w in (1000..=120_000u64).step_by(1000) {
        log.record(w + 1, Some(w), w + 10);
        let new = recorder.observe(
            &tree,
            &tree,
            &at(w + 1),
            w + 20,
            &inputs_of(&tree, &[&log]),
            &cfg,
        );
        rows.local.extend(new.local);
        rows.skew.extend(new.skew);
    }
    assert_eq!(period_totals(&rows.local).values().sum::<u64>(), 120_000);
    assert_eq!(rows.skew, vec![]);
}

/// Inputs that are a leaf or a join of two leaves, optionally under a refresh and stalled, and
/// an object that joins them. Each is observed with a random delay so that an input's
/// observation can come after the object's, and with a random lead of its frontier over the
/// wall clock, which makes the period book defer arrivals.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_never_exceeds_per_arrival_oracle() {
    const END: u64 = 2_000;
    // A leaf ticks at multiples of its interval, or, if unaligned, at a random phase.
    let leaf = (
        select(vec![1u64, 2, 3, 5, 7, 11, 13, 20]),
        proptest::option::of(0u64..1000),
    )
        .prop_map(|(interval_ms, phase)| OnTimeInput::Leaf {
            interval_ms,
            phase_ms: phase.map(|p| p % interval_ms),
        });
    // Joins and refreshes nest in any order, so an input can be a join that reads a refresh
    // view or another join, and the object joins such inputs.
    let shape = leaf.prop_recursive(2, 6, 3, |inner| {
        Union::new([
            proptest::collection::vec(inner.clone(), 2..=3)
                .prop_map(OnTimeInput::Min)
                .boxed(),
            (select(vec![20u64, 30, 50]), inner)
                .prop_map(|(p, input)| OnTimeInput::Refresh(p, Box::new(input)))
                .boxed(),
        ])
    });
    // Leads above about 550 ms with leaves of a few ms defer more observations than the
    // recorder keeps, which makes it merge them.
    let lead = select(vec![0u64, 100, 400, 600, 900, 1500]);
    let input = (
        shape,
        proptest::option::of((1u64..END, 0u64..300)),
        lead.clone(),
    );
    let strategy = (
        proptest::collection::vec(input, 1..=3),
        lead,
        any::<u64>(),
        0u64..30,
    );
    proptest!(ProptestConfig::with_cases(64), |((specs, lead, seed, max_delay) in strategy)| {
        let mut state = seed | 1;
        let mut delay = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % (max_delay + 1)
        };
        let cfg = RecorderConfig { period_ms: 500, ..config(4) };
        let models: Vec<OnTimeInput> = specs.iter().map(|(model, _, _)| model.clone()).collect();
        let trees: Vec<ResolvedTree> = models.iter().map(OnTimeInput::tree).collect();
        let tree = ResolvedTree::Min(trees.clone());
        // The true frontier, held during the input's stall.
        let frontier = |k: usize, t: u64| -> u64 {
            match specs[k].1 {
                Some((from, len)) if (from..from + len).contains(&t) => {
                    models[k].frontier(from - 1)
                }
                _ => models[k].frontier(t),
            }
        };
        let mut cap = |arrival: u64, timestamp: u64| on_time_cap(&models, arrival, timestamp);
        let mut histories: Vec<History> = vec![Vec::new(); specs.len()];
        let mut history = Vec::new();
        for t in 0..END {
            for (k, h) in histories.iter_mut().enumerate() {
                let observed = (t + delay()).saturating_sub(specs[k].2);
                push_advance(h, observed, frontier(k, t));
            }
            let f = (0..specs.len()).map(|k| frontier(k, t)).min().unwrap();
            push_advance(&mut history, (t + delay()).saturating_sub(lead), f);
        }

        // Observations in wall-clock order, inputs first at equal times.
        let mut events: Vec<(u64, usize, u64)> = histories
            .iter()
            .enumerate()
            .flat_map(|(k, h)| h.iter().map(move |(o, f)| (*o, k, *f)))
            .chain(history.iter().map(|(o, f)| (*o, usize::MAX, *f)))
            .collect();
        events.sort();
        let mut logs = vec![VisibilityLog::default(); specs.len()];
        let mut end_to_end = FreshnessRecorder::new(0);
        let mut local = LocalDelayRecorder::new(0);
        let mut recorded = Recorded::default();
        for (o, k, f) in events {
            if k < specs.len() {
                logs[k].record(f, trees[k].last_reflected(Timestamp::from(f)), o);
                continue;
            }
            let inputs: Vec<LocalInput<'_>> = trees
                .iter()
                .zip_eq(logs.iter())
                .map(|(tree, log)| LocalInput { tree, log })
                .collect();
            recorded.end_to_end.extend(end_to_end.observe(&tree, &at(f), o, &cfg));
            let rows = local.observe(&tree, &tree, &at(f), o, &inputs, &cfg);
            recorded.local.extend(rows.local);
            recorded.skew.extend(rows.skew);
        }
        let high_water_ms = local.high_water_ms();
        recorded.end_to_end.extend(end_to_end.finish());
        let rows = local.finish();
        recorded.local.extend(rows.local);
        recorded.skew.extend(rows.skew);

        let inputs: Vec<_> = trees.iter().zip_eq(histories.iter()).collect();
        let truths =
            Truth::all_with(&tree, &tree, &history, &inputs, &mut cap, (0, high_water_ms));
        for truth in &truths {
            // `upstream_ms` panics if skew and local exceed the end-to-end charge. Upstream
            // takes the time until the slowest input showed the arrival, and any excess skew.
            let upstream = truth.upstream_ms();
            prop_assert!(truth.arrival_upstream_ms() <= upstream, "{truth:?}");
            if truth.advance_ms == truth.observed_ms && truth.raw_skew_ms() == truth.skew_ms() {
                prop_assert_eq!(truth.arrival_upstream_ms(), upstream, "{:?}", truth);
            }
        }
        prop_assert_eq!(
            period_totals(&recorded.local),
            period_totals(&recorded.end_to_end)
        );
        if let Err(e) = never_exceeds(&recorded.local, &truths, cfg.period_ms, Truth::local_ms) {
            return Err(TestCaseError::fail(format!("local {e}")));
        }
        if specs.len() >= 2 {
            prop_assert_eq!(
                period_totals(&recorded.skew),
                period_totals(&recorded.end_to_end)
            );
            if let Err(e) = never_exceeds(&recorded.skew, &truths, cfg.period_ms, Truth::skew_ms) {
                return Err(TestCaseError::fail(format!("skew {e}")));
            }
        } else {
            prop_assert!(recorded.skew.is_empty());
        }
    });
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_merges_held_back_observations() {
    // A join of a 10 ms and a 15 ms source whose frontiers run 5 s ahead of the controller's
    // clock, shown by the join 5 ms after the input that advances it. With 1 s periods, the
    // period book defers about 5 s of join observations, far more than the recorder keeps.
    let (fast, slow) = (aligned(10), aligned(15));
    let join = ResolvedTree::Min(vec![fast.clone(), slow.clone()]);
    let end = 60_000;
    let h_fast = leaf_history(10, 0, end, None, 5000);
    let h_slow = leaf_history(15, 0, end, None, 5000);
    let history = derived_history(&[&h_fast, &h_slow], 5);
    let inputs = [(&fast, &h_fast), (&slow, &h_slow)];
    let cfg = RecorderConfig {
        period_ms: 1000,
        ..config(4)
    };
    let (log_fast, log_slow) = (log_of(&fast, &h_fast), log_of(&slow, &h_slow));
    let logs = [
        LocalInput {
            tree: &fast,
            log: &log_fast,
        },
        LocalInput {
            tree: &slow,
            log: &log_slow,
        },
    ];
    let start_ms = 10_000;
    let mut local = LocalDelayRecorder::new(start_ms);
    let mut rows = LocalRows::default();
    let mut longest = 0;
    for (o, f) in &history {
        let new = local.observe(&join, &join, &at(*f), *o, &logs, &cfg);
        rows.local.extend(new.local);
        rows.skew.extend(new.skew);
        longest = longest.max(local.shown_len());
    }
    let high_water_ms = local.high_water_ms();
    let new = local.finish();
    rows.local.extend(new.local);
    rows.skew.extend(new.skew);
    // The join advances every 5 ms, so about 1000 observations wait at a time, and the list
    // stays at its bound only by merging.
    assert_eq!(longest, super::local::MAX_SHOWN);
    // The oracle charges every arrival at its first observation, as an unmerged list would.
    let truths = Truth::all(&join, &join, &history, &inputs, (start_ms, high_water_ms));
    never_exceeds(&rows.local, &truths, cfg.period_ms, Truth::local_ms).unwrap();
    never_exceeds(&rows.skew, &truths, cfg.period_ms, Truth::skew_ms).unwrap();
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_refresh_join_input_stall_is_not_skew() {
    // A join of a 1 s source with a view that refreshes every 10 minutes over another 1 s
    // source and shows each refresh 2 s after it is due. The joined source stalls for 5 minutes
    // around the refresh at 20 minutes.
    const PERIOD: u64 = 600_000;
    let end = 3 * PERIOD + 60_000;
    let source = aligned(1000);
    let view = refresh(every(PERIOD), aligned(1000));
    let join = ResolvedTree::Min(vec![source.clone(), view.clone()]);
    let stall = (2 * PERIOD - 150_000, 2 * PERIOD + 150_000);
    let h_source = leaf_history(1000, 10, end, Some(stall), 0);
    let mut h_view = Vec::new();
    for k in 1..=end / PERIOD {
        // After the refresh at `k * PERIOD`, the view's frontier is the next refresh.
        push_advance(&mut h_view, k * PERIOD + 2000, (k + 1) * PERIOD);
    }
    let history = derived_history(&[&h_source, &h_view], 20);
    let inputs = [(&source, &h_source), (&view, &h_view)];
    let start_ms = PERIOD;
    let recorded = record_history(&join, &join, &history, &inputs, start_ms);
    let span = (start_ms, recorded.high_water_ms);
    assert!(span.1 >= 2 * PERIOD, "{span:?}");
    let truths = Truth::all(&join, &join, &history, &inputs, span);
    let s = scheme(4);
    // Every arrival needs a refresh time, which both inputs' grids reach without waiting.
    assert!(truths.iter().all(|t| t.cap_ms == 0));
    // Without the cap, the period before the stall would wait out the stall in skew.
    let waited = truths.iter().filter(|t| t.raw_skew_ms() >= 140_000).count();
    assert!(waited >= 400_000, "{waited}");
    assert_eq!(
        bucket_counts(&recorded.skew),
        BTreeMap::from([(s.bucket(1), span.1 - span.0)])
    );
    assert_eq!(
        bucket_counts(&recorded.local),
        BTreeMap::from([(s.bucket(20), span.1 - span.0)])
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_cap_measures_from_inner_join_showing() {
    // A join of an inner join of a 7 s and an 11 s source with a 3 s source that stalls for 5
    // minutes. The inner join shows an arrival only when its own inputs align, after the 3 s
    // source passed the needed timestamp, so on time the outer join has no skew there, and
    // the stall must not appear as skew.
    let (inner, inner_inputs) = join_7_11(None, 600_000);
    let h_inner = derived_history(&[&inner_inputs[0].1, &inner_inputs[1].1], 20);
    let l3 = aligned(3000);
    let h3 = leaf_history(3000, 10, 600_000, Some((130_000, 430_000)), 0);
    let outer = ResolvedTree::Min(vec![inner.clone(), l3.clone()]);
    let history = derived_history(&[&h_inner, &h3], 5);
    let inputs = [(&inner, &h_inner), (&l3, &h3)];
    let recorded = record_history(&outer, &outer, &history, &inputs, 120_000);
    let span = (120_000, recorded.high_water_ms);
    let truths = Truth::all(&outer, &outer, &history, &inputs, span);
    let period_ms = config(4).period_ms;
    never_exceeds(&recorded.skew, &truths, period_ms, Truth::skew_ms).unwrap();
    never_exceeds(&recorded.local, &truths, period_ms, Truth::local_ms).unwrap();
    // The stall makes arrivals wait after both inputs showed them, but none of that is skew
    // where the on-time skew is 0.
    assert!(truths.iter().map(Truth::raw_skew_ms).max().unwrap() >= 290_000);
    let on_time_aligned = truths.iter().filter(|t| t.cap_ms == 0).count();
    assert!(on_time_aligned > 0);
    let s = scheme(4);
    let zero_skew = bucket_counts(&recorded.skew)
        .get(&s.bucket(1))
        .copied()
        .unwrap_or(0);
    assert!(
        zero_skew >= u64::try_from(on_time_aligned).unwrap(),
        "{zero_skew} < {on_time_aligned}"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_unaligned_input_adds_no_skew_cap() {
    // A table that ticks every second at a phase off the epoch grid, joined with a 300 ms
    // source that stalls for a minute. The table's phase is unknown to the recorder, so only
    // the source's grid can cause skew.
    let table = OnTimeInput::join(&[(1000, Some(437))], None);
    let source = OnTimeInput::join(&[(300, None)], None);
    let (t_table, t_source) = (table.tree(), source.tree());
    let join = ResolvedTree::Min(vec![t_table.clone(), t_source.clone()]);
    let end = 400_000;
    let h_table = table.history(end, 10, None);
    let h_source = source.history(end, 10, Some((200_000, 260_000)));
    let history = derived_history(&[&h_table, &h_source], 20);
    let inputs = [(&t_table, &h_table), (&t_source, &h_source)];
    let recorded = record_history(&join, &join, &history, &inputs, 120_000);
    let span = (120_000, recorded.high_water_ms);
    let models = [table, source];
    let mut cap = |arrival: u64, timestamp: u64| on_time_cap(&models, arrival, timestamp);
    let truths = Truth::all_with(&join, &join, &history, &inputs, &mut cap, span);
    let period_ms = config(4).period_ms;
    never_exceeds(&recorded.skew, &truths, period_ms, Truth::skew_ms).unwrap();
    let skew_max = bucket_counts(&recorded.skew).into_keys().last().unwrap();
    assert!(skew_max <= scheme(4).bucket(299), "{skew_max}");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_cap_bounds_nested_unaligned_leaf() {
    // A join of `x`, a join of a 3 s table at an unknown phase with a 7 s source, with a 2 s
    // source that stalls for 5 minutes. `x` shows an arrival only once its table passes the
    // 7 s timestamp, which takes up to 3 s, so the cap must allow for the latest such tick.
    let x = OnTimeInput::join(&[(3000, Some(1234)), (7000, None)], None);
    let y = OnTimeInput::join(&[(2000, None)], None);
    let (t_x, t_y) = (x.tree(), y.tree());
    let outer = ResolvedTree::Min(vec![t_x.clone(), t_y.clone()]);
    let end = 600_000;
    let h_x = x.history(end, 10, None);
    let h_y = y.history(end, 10, Some((130_000, 430_000)));
    let history = derived_history(&[&h_x, &h_y], 20);
    let inputs = [(&t_x, &h_x), (&t_y, &h_y)];
    let recorded = record_history(&outer, &outer, &history, &inputs, 120_000);
    let span = (120_000, recorded.high_water_ms);
    let models = [x, y];
    let mut cap = |arrival: u64, timestamp: u64| on_time_cap(&models, arrival, timestamp);
    let truths = Truth::all_with(&outer, &outer, &history, &inputs, &mut cap, span);
    let period_ms = config(4).period_ms;
    never_exceeds(&recorded.skew, &truths, period_ms, Truth::skew_ms).unwrap();
    never_exceeds(&recorded.local, &truths, period_ms, Truth::local_ms).unwrap();
    assert!(truths.iter().map(Truth::raw_skew_ms).max().unwrap() >= 290_000);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn split_skew_past_the_step_budget_understates() {
    // Both inputs of a 7 s x 11 s join answer more than four hours of arrivals with single
    // entries. The 11 s input showed them at 20 000 s but passed their needed timestamps only
    // 4.8 s later, so one group spans all of them, with far more needed timestamps than the
    // step budget.
    let (l7, l11) = (aligned(7000), aligned(11_000));
    let join = ResolvedTree::Min(vec![l7.clone(), l11.clone()]);
    let span_end = 15_000_000;
    let (mut log7, mut log11) = (VisibilityLog::default(), VisibilityLog::default());
    log7.record(u64::MAX - 1, Some(u64::MAX - 2), 20_000_000);
    log11.record(1, Some(u64::MAX - 2), 20_000_000);
    log11.record(u64::MAX - 1, Some(u64::MAX - 2), 20_004_800);
    let inputs = [
        LocalInput {
            tree: &l7,
            log: &log7,
        },
        LocalInput {
            tree: &l11,
            log: &log11,
        },
    ];
    let cfg = config(4);
    let now = 30_000_000;
    let mut local = LocalDelayRecorder::new(0);
    let mut end_to_end = FreshnessRecorder::new(0);
    let mut rows = local.observe(&join, &join, &at(span_end + 1), now, &inputs, &cfg);
    // The last arrival the join reflects, on both grids.
    let reflected = local.high_water_ms();
    let finished = local.finish();
    rows.local.extend(finished.local);
    rows.skew.extend(finished.skew);
    let mut e2e = end_to_end.observe(&join, &at(span_end + 1), now, &cfg);
    e2e.extend(end_to_end.finish());
    let s = scheme(4);
    // Local delay and end-to-end latency do not depend on the budget.
    assert_eq!(
        bucket_counts(&rows.local),
        BTreeMap::from([(s.bucket(now - 20_004_800), reflected)])
    );
    assert_eq!(period_totals(&rows.skew), period_totals(&e2e));
    // A step covers the arrivals of one needed timestamp, at most 11 s of them, so the steps
    // within the budget cover at most that much, and the rest records 0.
    let budgeted = u64::try_from(super::local::MAX_GRID_STEPS).unwrap() * 11_000;
    let zero = bucket_counts(&rows.skew)
        .get(&s.bucket(1))
        .copied()
        .unwrap_or(0);
    assert!(zero >= reflected - budgeted, "{zero}");
    // Within the budget, skew is the on-time skew where it is below the 4.8 s wait.
    assert!(
        bucket_counts(&rows.skew).len() > 1,
        "{:?}",
        bucket_counts(&rows.skew)
    );
    let skew_max = bucket_counts(&rows.skew).into_keys().last().unwrap();
    assert!(skew_max <= s.bucket(4800), "{skew_max}");
}
