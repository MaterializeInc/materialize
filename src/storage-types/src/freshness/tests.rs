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
    assert_eq!(join(&[7000, 11000]).assignment_bound(), 17000);
    assert_eq!(join(&[1000, 7000]).assignment_bound(), 7000);
    assert_eq!(aligned(100).assignment_bound(), 100);
    // The pairwise maximum, from 10 + 15 - 5.
    assert_eq!(join(&[6000, 10000, 15000]).assignment_bound(), 20000);
    // Nested minima flatten.
    let nested = ResolvedTree::Min(vec![aligned(7000), ResolvedTree::Min(vec![aligned(11000)])]);
    assert_eq!(nested.assignment_bound(), 17000);
}

#[mz_ore::test]
fn bound_aligned_unaligned() {
    let tree = ResolvedTree::Min(vec![aligned(7000), unaligned(1000)]);
    assert_eq!(tree.assignment_bound(), 8000);
    let tree = ResolvedTree::Min(vec![unaligned(3000), unaligned(1000)]);
    assert_eq!(tree.assignment_bound(), 4000);
    assert_eq!(unaligned(1000).assignment_bound(), 1000);
}

#[mz_ore::test]
fn bound_continuous() {
    assert_eq!(ResolvedTree::Leaf(Ticker::Continuous).assignment_bound(), 0);
    let tree = ResolvedTree::Min(vec![aligned(7000), ResolvedTree::Leaf(Ticker::Continuous)]);
    assert_eq!(tree.assignment_bound(), 7000);
}

#[mz_ore::test]
fn bound_refresh() {
    let tree = refresh(every(60_000), aligned(1000));
    assert_eq!(tree.assignment_bound(), 60_000);
    let tree = refresh(
        every(60_000),
        ResolvedTree::Min(vec![aligned(7000), aligned(11000)]),
    );
    assert_eq!(tree.assignment_bound(), 77_000);
    let month = 30 * 24 * 60 * 60 * 1000;
    let tree = refresh(every(month), aligned(1000));
    assert!(tree.assignment_bound() >= month);
}

/// A hyperperiod with more than 10^6 change points takes the conservative bound.
#[mz_ore::test]
fn bound_refresh_conservative_fallback() {
    let tree = refresh(every(1_000_003), aligned(1));
    assert_eq!(tree.assignment_bound(), 1 + 1_000_003);
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
        prop_assert_eq!(tree.assignment_bound(), oracle_bound(&tree, hyper));
    });
}

/// Under a refresh the bound scans the expected frontier, which assumes the worst phase for
/// unaligned leaves, so it never falls below the oracle. Without a refresh the unaligned
/// closed form is the bound for an unknown phase and the oracle's worst-phase model does not
/// apply.
#[mz_ore::test]
fn bound_never_below_oracle_with_unaligned() {
    let ticker = prop_oneof![
        (1u64..=20).prop_map(|interval_ms| Ticker::Aligned { interval_ms }),
        (1u64..=20).prop_map(|interval_ms| Ticker::Unaligned { interval_ms }),
        Just(Ticker::Continuous),
    ];
    let leaves = proptest::collection::vec(ticker, 1..=3);
    proptest!(|(leaves in leaves, period in 1u64..=30)| {
        let hyper = leaves.iter().fold(period, |a, t| match t {
            Ticker::Aligned { interval_ms } | Ticker::Unaligned { interval_ms } => {
                lcm(a, *interval_ms)
            }
            Ticker::Continuous => a,
        });
        let min = ResolvedTree::Min(leaves.into_iter().map(ResolvedTree::Leaf).collect());
        let tree = refresh(every(period), min);
        prop_assert!(tree.assignment_bound() >= oracle_bound(&tree, hyper));
    });
}
