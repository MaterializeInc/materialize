// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_repr::Timestamp;
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use proptest::prelude::*;

use super::*;

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

/// A hyperperiod with more than 10^6 change points takes the conservative bound, twice the
/// largest path sum of periods and intervals.
#[mz_ore::test]
fn bound_refresh_conservative_fallback() {
    let tree = refresh(every(1_000_003), aligned(1));
    assert_eq!(tree.assignment_bound(), Some(2 * (1_000_003 + 1)));
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
