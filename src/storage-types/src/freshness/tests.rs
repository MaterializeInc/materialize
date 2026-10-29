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

use mz_ore::cast::CastLossy;
use mz_repr::Timestamp;
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use proptest::prelude::*;
use proptest::sample::select;
use proptest::strategy::Union;

use super::histogram::{BucketScheme, add_span};
use super::local::{LocalDelayRecorder, VisibilityLog};
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

/// The timestamp at which an arrival at `t` becomes visible.
fn assigned(tree: &ResolvedTree, t: u64) -> Option<u64> {
    match tree {
        ResolvedTree::Leaf(Ticker::Continuous) => Some(t),
        ResolvedTree::Leaf(Ticker::Aligned { interval_ms } | Ticker::Unaligned { interval_ms }) => {
            Some(t.div_ceil(*interval_ms) * interval_ms)
        }
        ResolvedTree::Min(inputs) => inputs
            .iter()
            .map(|i| assigned(i, t))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .max(),
        ResolvedTree::Refresh { schedule, input } => {
            let a = assigned(input, t)?;
            schedule
                .round_up_timestamp(Timestamp::from(a))
                .map(u64::from)
        }
    }
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
    (
        proptest::collection::vec(ticker, 1..=4),
        proptest::option::of((1u64..=60, 0u64..=30)),
    )
        .prop_map(|(leaves, refresh)| {
            let min = ResolvedTree::Min(leaves.into_iter().map(ResolvedTree::Leaf).collect());
            match refresh {
                None => min,
                Some((interval, aligned_to)) => ResolvedTree::Refresh {
                    schedule: RefreshSchedule {
                        everies: vec![RefreshEvery {
                            interval: Duration::from_millis(interval),
                            aligned_to: Timestamp::from(aligned_to),
                        }],
                        ats: vec![],
                    },
                    input: Box::new(min),
                },
            }
        })
}

#[mz_ore::test]
fn last_reflected_matches_oracle() {
    proptest!(|(tree in arb_tree(), frontier in 0u64..400)| {
        // Brute force: the largest arrival whose assigned timestamp is below the frontier.
        let mut expected = None;
        for t in 0..=frontier {
            if let Some(a) = assigned(&tree, t) {
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
    // Open (0, 10 s], then switch to 60 s periods, so the new period starts at 0 as well. The
    // later steps change the length again, including to one that nests the open periods.
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
    // The old period only holds the arrivals before the change, and the new (0, 60 s] period,
    // which shares its start, takes the rest.
    assert_eq!(per_period[&(0, 10_000)], 5_000);
    assert_eq!(per_period[&(0, 60_000)], 50_000);
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
    log.record(1000, 1010);
    log.record(2000, 2020);
    // A regressing or repeated value is ignored.
    log.record(2000, 2500);
    log.record(1500, 2600);
    // The earliest observation with `reflected >= arrival`.
    assert_eq!(log.visible_at(1), Some(1010));
    assert_eq!(log.visible_at(1000), Some(1010));
    assert_eq!(log.visible_at(1001), Some(2020));
    assert_eq!(log.visible_at(2000), Some(2020));
    assert_eq!(log.visible_at(2001), None);
    // Pruned entries answer with the observation of the last pruned entry, and the rest stay
    // correct.
    log.prune_through(1000);
    assert_eq!(log.visible_at(1000), Some(1010));
    assert_eq!(log.visible_at(1001), Some(2020));
    log.prune_through(5000);
    assert_eq!(log.visible_at(1), Some(2020));
    assert_eq!(log.visible_at(2000), Some(2020));
    assert_eq!(log.visible_at(6000), None);
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
        log_a.record(w, w + 1);
        rows_a.extend(local_a.observe(&tree, &at(w + 1), w + 1, &[], &cfg));
        log_b.record(w, w + 3001);
        rows_b.extend(local_b.observe(&tree, &at(w + 1), w + 3001, &[&log_a], &cfg));
        rows_c.extend(local_c.observe(&tree, &at(w + 1), w + 3001, &[&log_b], &cfg));
        rows_e2e.extend(end_to_end_c.observe(&tree, &at(w + 1), w + 3001, &cfg));
    }
    let empty = Antichain::new();
    rows_a.extend(local_a.observe(&tree, &empty, 300_000, &[], &cfg));
    rows_b.extend(local_b.observe(&tree, &empty, 300_000, &[&log_a], &cfg));
    rows_c.extend(local_c.observe(&tree, &empty, 300_000, &[&log_b], &cfg));
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
fn local_join_uses_slowest_input() {
    let tree = ResolvedTree::Min(vec![aligned(7000), aligned(11000)]);
    let cfg = config(4);
    for swap in [false, true] {
        let (mut fast, mut slow) = (VisibilityLog::default(), VisibilityLog::default());
        fast.record(7000, 7100);
        slow.record(7000, 7500);
        let inputs = if swap { [&slow, &fast] } else { [&fast, &slow] };
        let mut recorder = LocalDelayRecorder::new(0);
        let mut rows = recorder.observe(&tree, &at(11_001), 8000, &inputs, &cfg);
        rows.extend(recorder.observe(&tree, &Antichain::new(), 8000, &inputs, &cfg));
        // The slow input, observed at 7500, decides.
        assert_eq!(
            bucket_counts(&rows),
            BTreeMap::from([(scheme(4).bucket(500), 7000)])
        );
    }
}

#[mz_ore::test]
fn local_missing_input_entry_understates() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut observed = VisibilityLog::default();
    observed.record(1000, 1100);
    observed.record(2000, 2200);
    let missing = VisibilityLog::default();
    let mut pruned = VisibilityLog::default();
    pruned.record(1000, 1100);
    pruned.record(2000, 2200);
    pruned.prune_through(1000);
    let s = scheme(4);
    let run = |log: &VisibilityLog| {
        let mut recorder = LocalDelayRecorder::new(0);
        let mut rows = Vec::new();
        for (frontier, now) in [(1001, 1500), (2001, 2500)] {
            rows.extend(recorder.observe(&tree, &at(frontier), now, &[log], &cfg));
        }
        rows.extend(recorder.observe(&tree, &Antichain::new(), 3000, &[log], &cfg));
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
    log.record(15_000, 21_100);
    log.record(21_000, 21_500);
    let mut recorder = LocalDelayRecorder::new(0);
    let mut rows = recorder.observe(&tree, &at(21_001), 22_000, &[&log], &config(4));
    rows.extend(recorder.observe(&tree, &Antichain::new(), 22_000, &[&log], &config(4)));
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
    log.record(1000, 5000);
    let mut recorder = LocalDelayRecorder::new(0);
    let mut rows = recorder.observe(&tree, &at(1001), 1500, &[&log], &cfg);
    rows.extend(recorder.observe(&tree, &Antichain::new(), 1500, &[&log], &cfg));
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
            log_a.record(next_a, next_a + 1);
            next_a += 1000;
        }
        let frontier = at(refresh_ts + 1);
        assert_eq!(
            tree.last_reflected(Timestamp::from(refresh_ts + 1)),
            Some(refresh_ts)
        );
        rows_refreshed.extend(local_refreshed.observe(&tree, &frontier, now, &[&log_a], &cfg));
        log_refreshed.record(refresh_ts, now);
        rows_plain.extend(local_plain.observe(&tree, &frontier, now, &[&log_refreshed], &cfg));
    }
    let empty = Antichain::new();
    rows_refreshed.extend(local_refreshed.observe(&tree, &empty, 300_000, &[&log_a], &cfg));
    rows_plain.extend(local_plain.observe(&tree, &empty, 300_000, &[&log_refreshed], &cfg));
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
    proptest::collection::vec((1u64..30_000, 0u64..40_000), 0..12).prop_map(|steps| {
        let (mut reflected, mut observed) = (0, 0);
        let mut log = VisibilityLog::default();
        for (dr, dt) in steps {
            reflected += dr;
            observed += dt;
            log.record(reflected, observed);
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
        let inputs: Vec<&VisibilityLog> = logs.iter().collect();
        let mut end_to_end = FreshnessRecorder::new(0);
        let mut local = LocalDelayRecorder::new(0);
        let (mut rows_e2e, mut rows_local) = (vec![], vec![]);
        let (mut frontier, mut now) = (1, 0);
        for (df, dt) in steps {
            frontier += df;
            now += dt;
            rows_e2e.extend(end_to_end.observe(&tree, &at(frontier), now, &cfg));
            rows_local.extend(local.observe(&tree, &at(frontier), now, &inputs, &cfg));
        }
        rows_e2e.extend(end_to_end.observe(&tree, &Antichain::new(), now, &cfg));
        rows_local.extend(local.observe(&tree, &Antichain::new(), now, &inputs, &cfg));
        prop_assert_eq!(period_totals(&rows_local), period_totals(&rows_e2e));
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
        log.record(t, t);
    }
    let mut end_to_end = FreshnessRecorder::new(0);
    let mut local = LocalDelayRecorder::new(0);
    // Each period length opens its own period `(0, length]`, which stays open below 1000.
    for t in 1..1_000 {
        let cfg = RecorderConfig {
            period_ms: 1_000 * t,
            ..config(4)
        };
        assert!(end_to_end.observe(&tree, &at(t + 1), t, &cfg).is_empty());
        assert!(
            local
                .observe(&tree, &at(t + 1), t, &[&log], &cfg)
                .is_empty()
        );
    }
    let end_to_end = format!("{end_to_end:?}");
    assert!(end_to_end.contains("open_periods: 999"), "{end_to_end}");
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
    let mut book = PeriodBook::new(NOW_MS - 3_600_000);
    let (closed, _, _) = advance_counted(&mut book, NOW_MS, NOW_MS, &cfg);
    // One hour is not a catch-up yet, so every closed period has the configured length.
    assert!(closed.iter().all(|(s, e, _)| e - s == cfg.period_ms));
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
    assert!(
        local
            .observe(&tree, &at(30_001), 30_001, &[], &cfg)
            .is_empty()
    );
    let rows = recorder.finish();
    assert_eq!(period_totals(&rows), BTreeMap::from([(0, 30_000)]));
    assert_eq!(local.finish(), rows);
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
        log.record(w, w + 100);
    }
    let now = end + 100;
    let cutoff = now - 600_000;
    log.prune_through(cutoff - 1100);
    let mut recorder = LocalDelayRecorder::new(start);
    let mut rows = recorder.observe(&tree, &at(end + 1), now, &[&log], &cfg);
    rows.extend(recorder.finish());
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
