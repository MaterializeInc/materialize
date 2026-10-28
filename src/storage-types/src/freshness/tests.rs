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

use super::histogram::{BucketScheme, add_span};
use super::local::{FrontierLog, LocalDelayRecorder};
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
    let ticker = prop_oneof![
        (1u64..=50).prop_map(|interval_ms| Ticker::Aligned { interval_ms }),
        (1u64..=50).prop_map(|interval_ms| Ticker::Unaligned { interval_ms }),
        Just(Ticker::Continuous),
    ];
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
    let leaf = prop_oneof![
        (1u64..=8).prop_map(Spec::Aligned),
        (1u64..=5)
            .prop_flat_map(|u| (Just(u), 0..u))
            .prop_map(|(u, phase)| Spec::Unaligned(u, phase)),
        Just(Spec::Continuous),
    ];
    leaf.prop_recursive(2, 8, 3, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 2..=3).prop_map(Spec::Min),
            (1u64..=10, inner).prop_map(|(p, input)| Spec::Refresh(p, Box::new(input))),
        ]
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
        interval in prop_oneof![Just(1u64), Just(7u64), Just(10u64)],
        b in prop_oneof![Just(1u32), Just(4u32), Just(16u32)],
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
        interval in prop_oneof![Just(1u64), Just(3u64), Just(7u64)],
        b in prop_oneof![Just(1u32), Just(4u32), Just(16u32)],
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
    }
}

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

fn ts(t: u64) -> Timestamp {
    Timestamp::from(t)
}

fn bucket_counts(rows: &[PeriodRow]) -> BTreeMap<i32, u64> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row.bucket).or_default() += row.count;
    }
    counts
}

#[mz_ore::test]
fn frontier_log_passed_at() {
    let mut log = FrontierLog::default();
    assert_eq!(log.passed_at(ts(0)), None);
    log.record(ts(1001), 1010);
    log.record(ts(2001), 2020);
    // A regressing or repeated frontier is ignored.
    log.record(ts(2001), 2500);
    log.record(ts(1500), 2600);
    // The earliest observation strictly above the timestamp.
    assert_eq!(log.passed_at(ts(0)), Some(1010));
    assert_eq!(log.passed_at(ts(1000)), Some(1010));
    assert_eq!(log.passed_at(ts(1001)), Some(2020));
    assert_eq!(log.passed_at(ts(2000)), Some(2020));
    assert_eq!(log.passed_at(ts(2001)), None);
    // Pruned entries answer nothing, and the rest stay correct.
    log.prune_through(ts(1000));
    assert_eq!(log.passed_at(ts(1000)), None);
    assert_eq!(log.passed_at(ts(1001)), Some(2020));
    log.prune_through(ts(5000));
    assert_eq!(log.passed_at(ts(2000)), None);
    assert_eq!(log.passed_at(ts(6000)), None);
}

#[mz_ore::test]
fn local_chain_attributes_hops() {
    let tree = aligned(1000);
    let cfg = config(4);
    let (mut log_a, mut log_b) = (FrontierLog::default(), FrontierLog::default());
    let mut local_a = LocalDelayRecorder::new(0);
    let mut local_b = LocalDelayRecorder::new(0);
    let mut local_c = LocalDelayRecorder::new(0);
    let mut end_to_end_c = FreshnessRecorder::new(0);
    let (mut rows_a, mut rows_b, mut rows_c, mut rows_e2e) = (vec![], vec![], vec![], vec![]);
    for w in (1000..=200_000u64).step_by(1000) {
        // A is a 1 s source, B shows each tick 3 s later, and C shows it as B does.
        log_a.record(ts(w + 1), w + 1);
        rows_a.extend(local_a.observe(&tree, &at(w + 1), w + 1, &[], &cfg));
        log_b.record(ts(w + 1), w + 3001);
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
        let (mut fast, mut slow) = (FrontierLog::default(), FrontierLog::default());
        fast.record(ts(11_001), 7100);
        slow.record(ts(11_001), 7500);
        let inputs = if swap { [&slow, &fast] } else { [&fast, &slow] };
        let mut recorder = LocalDelayRecorder::new(0);
        let mut rows = recorder.observe(&tree, &at(11_001), 8000, &inputs, &cfg);
        rows.extend(recorder.observe(&tree, &Antichain::new(), 8000, &inputs, &cfg));
        // The first window ends at 7000 and carries the timestamp 11000, so both logs apply
        // and the slow one, observed at 7500, decides.
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
    let mut observed = FrontierLog::default();
    observed.record(ts(1001), 1100);
    observed.record(ts(2001), 2200);
    let missing = FrontierLog::default();
    let mut pruned = FrontierLog::default();
    pruned.record(ts(1001), 1100);
    pruned.record(ts(2001), 2200);
    pruned.prune_through(ts(1000));
    let s = scheme(4);
    let run = |log: &FrontierLog| {
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
    // Without any, the input counts as ready at the object's previous advance, which is the
    // start time 0 and then 1500.
    assert_eq!(
        run(&missing),
        BTreeMap::from([(s.bucket(1500), 1000), (s.bucket(1000), 1000)])
    );
    // The pruned log answers the second window and not the first.
    assert_eq!(
        run(&pruned),
        BTreeMap::from([(s.bucket(1500), 1000), (s.bucket(300), 1000)])
    );
}

#[mz_ore::test]
fn local_negative_delay_clamps() {
    let tree = aligned(1000);
    let cfg = config(4);
    let mut log = FrontierLog::default();
    log.record(ts(1001), 5000);
    let mut recorder = LocalDelayRecorder::new(0);
    let mut rows = recorder.observe(&tree, &at(1001), 1500, &[&log], &cfg);
    rows.extend(recorder.observe(&tree, &Antichain::new(), 1500, &[&log], &cfg));
    assert_eq!(
        bucket_counts(&rows),
        BTreeMap::from([(scheme(4).bucket(1), 1000)])
    );
}

fn arb_local_tree() -> impl Strategy<Value = ResolvedTree> {
    prop_oneof![
        Just(aligned(1000)),
        Just(ResolvedTree::Min(vec![aligned(7000), aligned(11000)])),
        Just(ResolvedTree::Leaf(Ticker::Continuous)),
        Just(ResolvedTree::Min(vec![unaligned(300), aligned(1000)])),
    ]
}

fn arb_log() -> impl Strategy<Value = FrontierLog> {
    proptest::collection::vec((1u64..30_000, 0u64..40_000), 0..12).prop_map(|steps| {
        let (mut frontier, mut observed) = (0, 0);
        let mut log = FrontierLog::default();
        for (df, dt) in steps {
            frontier += df;
            observed += dt;
            log.record(ts(frontier), observed);
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
        prop_oneof![Just(1u64), Just(3u64)],
    );
    proptest!(ProptestConfig::with_cases(64), |((tree, steps, logs, interval_ms) in strategy)| {
        let cfg = RecorderConfig { interval_ms, period_ms: 10_000, ..config(4) };
        let inputs: Vec<&FrontierLog> = logs.iter().collect();
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
