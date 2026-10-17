// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;

use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use mz_ore::pool::Pool;
use proptest::prelude::*;
use timely::order::Product;

use super::*;
use crate::columnar::batcher::{Chunker, ColumnChunker, ColumnMerger};
use crate::columnar::chunk::{
    AccountedChunkBatcher, ChunkChunker, ComputeSpill, set_spill_override,
};
use crate::columnation::ColInternalMerger;

type Update<T> = (u64, T, i64);

/// One step of an `arrange_core`-shaped drive.
#[derive(Clone, Debug)]
pub(super) enum Step<T> {
    /// Updates arriving in one input container. Times must not be before the last seal.
    Push(Vec<Update<T>>),
    /// A seal at the given frontier.
    Seal(Antichain<T>),
}

type ColumnArm<T> = (
    MergeBatcher<ColumnMerger<u64, T, i64>>,
    ColumnChunker<(u64, T, i64)>,
);
type ChunkArm<T> = (
    AccountedChunkBatcher<u64, T, i64, ComputeSpill>,
    ChunkChunker<u64, T, i64, ComputeSpill>,
);
type ColumnationArm<T> = (
    MergeBatcher<ColInternalMerger<u64, T, i64>>,
    Chunker<ColumnationStack<(u64, T, i64)>>,
);

fn collect<O, T>(chunks: Vec<O>, into: &mut BTreeMap<(u64, T), i64>)
where
    O: TimedUpdates<u64, T, i64>,
    T: Columnar + Timestamp + Ord,
    for<'a> <(u64, T, i64) as Columnar>::Container:
        columnar::Push<columnar::Ref<'a, (u64, T, i64)>>,
{
    for chunk in chunks {
        let mut all = Column::default();
        let mut none = Column::default();
        chunk.partition_into(|_| true, &mut all, &mut none);
        for (data, time, diff) in all.borrow().into_index_iter() {
            let key = (u64::into_owned(data), T::into_owned(time));
            *into.entry(key).or_default() += i64::into_owned(diff);
        }
    }
    into.retain(|_, diff| *diff != 0);
}

fn feed<Ba, Chu, T>(batcher: &mut Ba, chunker: &mut Chu, updates: &[Update<T>])
where
    Ba: PushInto<Chu::Container>,
    Chu: ContainerBuilder<Container: Default> + for<'a> PushInto<&'a mut Column<(u64, T, i64)>>,
    T: Columnar,
    for<'a> <(u64, T, i64) as Columnar>::Container: columnar::Push<&'a (u64, T, i64)>,
{
    let mut column = Column::default();
    for update in updates {
        column.push_into(update);
    }
    chunker.push_into(&mut column);
    while let Some(chunk) = chunker.extract() {
        batcher.push_into(std::mem::take(chunk));
    }
}

fn flush<Ba, Chu>(batcher: &mut Ba, chunker: &mut Chu)
where
    Ba: PushInto<Chu::Container>,
    Chu: ContainerBuilder<Container: Default>,
{
    while let Some(chunk) = chunker.finish() {
        batcher.push_into(std::mem::take(chunk));
    }
}

/// Drives the plain batcher `B` and the wrapper over `B` through `steps`, and checks that
/// each seal emits the same updates and description, and that the wrapper's frontier lies
/// between the sealed frontier and the held updates. Returns the wrapper's stats.
pub(super) fn check<B, Chu, T>(steps: &[Step<T>], theta: u64) -> TemporalBatcherStats
where
    B: Batcher<Time = T>,
    B::Output: TimedUpdates<u64, T, i64>,
    Chu: ContainerBuilder<Container = B::Output> + for<'a> PushInto<&'a mut Column<(u64, T, i64)>>,
    T: BucketTime + Columnar + Ord + Default + Debug + differential_dataflow::lattice::Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    <T::Key as Timestamp>::Summary: From<u64>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <(u64, T, i64) as Columnar>::Container:
        columnar::Push<columnar::Ref<'a, (u64, T, i64)>> + columnar::Push<&'a (u64, T, i64)>,
{
    let mut plain = B::new(None, 0);
    let mut plain_chunker = Chu::default();
    let mut wrapped = TemporalBatcher::<B, Chu, u64, T, i64>::with_theta(None, 0, theta);
    let mut wrapped_chunker = Chu::default();
    let mut held: BTreeMap<(u64, T), i64> = BTreeMap::new();

    for (index, step) in steps.iter().enumerate() {
        match step {
            Step::Push(updates) => {
                feed(&mut plain, &mut plain_chunker, updates);
                feed(&mut wrapped, &mut wrapped_chunker, updates);
                for (data, time, diff) in updates {
                    *held.entry((*data, time.clone())).or_default() += diff;
                }
                held.retain(|_, diff| *diff != 0);
            }
            Step::Seal(upper) => {
                flush(&mut plain, &mut plain_chunker);
                flush(&mut wrapped, &mut wrapped_chunker);
                let (plain_out, plain_desc) = plain.seal(upper.clone());
                let (wrapped_out, wrapped_desc) = wrapped.seal(upper.clone());
                assert_eq!(plain_desc, wrapped_desc, "step {index}: descriptions");

                let mut plain_updates = BTreeMap::new();
                collect(plain_out, &mut plain_updates);
                let mut wrapped_updates = BTreeMap::new();
                collect(wrapped_out, &mut wrapped_updates);
                let mut expected = BTreeMap::new();
                held.retain(|(data, time), diff| {
                    if upper.less_equal(time) {
                        true
                    } else {
                        expected.insert((*data, time.clone()), *diff);
                        false
                    }
                });
                assert_eq!(plain_updates, expected, "step {index}: plain batcher");
                assert_eq!(wrapped_updates, expected, "step {index}: wrapped batcher");

                let frontier = wrapped.frontier().to_owned();
                for time in frontier.iter() {
                    assert!(
                        upper.less_equal(time),
                        "step {index}: frontier {frontier:?} below upper {upper:?}"
                    );
                }
                for (_, time) in held.keys() {
                    assert!(
                        frontier.less_equal(time),
                        "step {index}: frontier {frontier:?} above held time {time:?}"
                    );
                }
            }
        }
    }
    wrapped.stats()
}

fn seal(time: u64) -> Step<u64> {
    Step::Seal(Antichain::from_elem(time))
}

fn seal_all<T: Timestamp>() -> Step<T> {
    Step::Seal(Antichain::new())
}

/// A hydration-shaped input: a snapshot at `0`, a future tail spread over `[1, span)`, then
/// ticks of `tick` that also retract part of the tail, and a final drain.
fn hydration(snapshot: u64, tail: u64, span: u64, tick: u64, ticks: u64) -> Vec<Step<u64>> {
    let mut steps = Vec::new();
    let mut updates = Vec::new();
    for data in 0..snapshot {
        updates.push((data, 0, 1));
    }
    for i in 0..tail {
        let time = 1 + (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 8) % (span - 1);
        updates.push((i % 97, time, 1));
    }
    steps.push(Step::Push(updates));
    steps.push(seal(1));
    for k in 1..=ticks {
        let lower = k * tick;
        steps.push(Step::Push(vec![
            (1, lower, 1),
            (2, lower + span / 2, 1),
            (2, lower + span / 2, -1),
            (3, lower + span, 1),
        ]));
        steps.push(seal(lower + 1));
    }
    steps.push(seal_all());
    steps
}

/// Hand-written cases: cancellation, records at the far bound, a bucket that cancels to
/// empty, and an empty-frontier drain.
fn edge_cases() -> Vec<Step<u64>> {
    vec![
        Step::Push(vec![(1, 5, 1), (2, 1_000, 1), (3, 1_000, 1)]),
        seal(10),
        // Far bound is 15 at θ = 5. A record at exactly 15 parks, one at 14 does not.
        Step::Push(vec![(4, 15, 1), (5, 14, 1), (6, 15, 2)]),
        // Retract a far insert, so the bucket holding time 1_000 cancels to empty.
        Step::Push(vec![(2, 1_000, -1), (3, 1_000, -1)]),
        seal(14),
        seal(15),
        Step::Push(vec![(7, 5_000, 1), (7, 5_000, -1), (8, 6_000, 1)]),
        seal(16),
        seal(5_000),
        seal(5_001),
        Step::Push(vec![(9, 1 << 40, 1)]),
        seal(6_000),
        seal_all(),
    ]
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn edge_cases_match_plain() {
    for theta in [0, 1, 5, 100] {
        check_arm::<ColumnArm<u64>, _>(&edge_cases(), theta);
        check_arm::<ChunkArm<u64>, _>(&edge_cases(), theta);
        check_arm::<ColumnationArm<u64>, _>(&edge_cases(), theta);
    }
}

fn check_arm<A: Arm<T>, T>(steps: &[Step<T>], theta: u64) -> TemporalBatcherStats {
    A::check(steps, theta)
}

/// A batcher and chunker pair, as one type parameter.
trait Arm<T> {
    fn check(steps: &[Step<T>], theta: u64) -> TemporalBatcherStats;
}

impl<B, Chu, T> Arm<T> for (B, Chu)
where
    B: Batcher<Time = T>,
    B::Output: TimedUpdates<u64, T, i64>,
    Chu: ContainerBuilder<Container = B::Output> + for<'a> PushInto<&'a mut Column<(u64, T, i64)>>,
    T: BucketTime + Columnar + Ord + Default + Debug + differential_dataflow::lattice::Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    <T::Key as Timestamp>::Summary: From<u64>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <(u64, T, i64) as Columnar>::Container:
        columnar::Push<columnar::Ref<'a, (u64, T, i64)>> + columnar::Push<&'a (u64, T, i64)>,
{
    fn check(steps: &[Step<T>], theta: u64) -> TemporalBatcherStats {
        check::<B, Chu, T>(steps, theta)
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn hydration_matches_plain() {
    let steps = hydration(1_000, 20_000, 100_000, 1_000, 20);
    for theta in [0, 1_000, 10_000] {
        let stats = check_arm::<ColumnArm<u64>, _>(&steps, theta);
        assert!(stats.first_seal_parked > 0, "θ = {theta}: nothing parked");
        check_arm::<ChunkArm<u64>, _>(&steps, theta);
        check_arm::<ColumnationArm<u64>, _>(&steps, theta);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn hydration_matches_plain_spilled() {
    let pool = Pool::new().expect("pool creation");
    set_spill_override(Some(pool.clone()));
    let steps = hydration(1_000, 200_000, 100_000, 1_000, 20);
    check_arm::<ChunkArm<u64>, _>(&steps, 1_000);
    set_spill_override(None);
    assert!(pool.stats().inserts > 0, "nothing spilled");
}

/// Steps whose times never precede the last seal, ending in a drain.
fn arb_steps() -> impl Strategy<Value = Vec<Step<u64>>> {
    let op = prop_oneof![
        3 => proptest::collection::vec((0..6u64, 0..400u64, -2..=2i64), 0..20)
            .prop_map(|updates| (Some(updates), 0)),
        2 => (0..60u64).prop_map(|advance| (None, advance)),
    ];
    proptest::collection::vec(op, 0..40).prop_map(|ops| {
        let mut upper = 0;
        let mut steps = Vec::new();
        for (updates, advance) in ops {
            match updates {
                Some(updates) => steps.push(Step::Push(
                    updates
                        .into_iter()
                        .map(|(data, offset, diff)| (data, upper + offset, diff))
                        .collect(),
                )),
                None => {
                    upper += advance;
                    steps.push(seal(upper));
                }
            }
        }
        steps.push(seal_all());
        steps
    })
}

proptest! {
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn matches_plain(steps in arb_steps(), theta in prop_oneof![Just(0u64), Just(1), Just(7), Just(50)]) {
        check_arm::<ColumnArm<u64>, _>(&steps, theta);
        check_arm::<ChunkArm<u64>, _>(&steps, theta);
        check_arm::<ColumnationArm<u64>, _>(&steps, theta);
    }
}

type Time2 = Product<u64, u64>;

fn t2(outer: u64, inner: u64) -> Time2 {
    Product::new(outer, inner)
}

fn seal2(elements: &[(u64, u64)]) -> Step<Time2> {
    Step::Seal(Antichain::from_iter(
        elements.iter().map(|(outer, inner)| t2(*outer, *inner)),
    ))
}

fn check_all2(steps: &[Step<Time2>], theta: u64) -> TemporalBatcherStats {
    let stats = check_arm::<ColumnArm<Time2>, _>(steps, theta);
    check_arm::<ChunkArm<Time2>, _>(steps, theta);
    check_arm::<ColumnationArm<Time2>, _>(steps, theta);
    stats
}

/// Iterative times, bucketed by the outer coordinate: uppers with and without an
/// inner-minimum element, and a parked update whose inner coordinate is below the inner
/// coordinates of the other frontier elements.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn product_edge_cases_match_plain() {
    let steps = vec![
        Step::Push(vec![
            (1, t2(0, 0), 1),
            (2, t2(20, 1), 1),
            (3, t2(40, 0), 1),
            (4, t2(9, 7), 1),
        ]),
        // Edge is 8, so with small θ the updates at outer 20 and 40 park.
        seal2(&[(5, 3), (8, 0)]),
        Step::Push(vec![(5, t2(30, 0), 1), (6, t2(8, 0), 1), (7, t2(6, 4), 1)]),
        // `(15, 0) <= (20, 1)`, so the parked update is not ready, although
        // `(18, 2)` has a smaller inner coordinate.
        seal2(&[(15, 0), (18, 2)]),
        Step::Push(vec![
            (8, t2(50, 0), 1),
            (8, t2(50, 0), -1),
            (9, t2(60, 3), 1),
        ]),
        seal2(&[(17, 0), (19, 2)]),
        // Edge is 30, so `(20, 1)` peels, and it is ready because neither element is at or
        // before it. Using the least outer coordinate, 25, would keep it parked.
        seal2(&[(25, 2), (30, 0)]),
        Step::Push(vec![(10, t2(70, 1), 1), (11, t2(31, 0), 1)]),
        seal2(&[(31, 1), (41, 0)]),
        // No inner-minimum element: nothing parks, and the chain drains into the inner
        // batcher, which keeps `(60, 3)` and `(70, 1)` until the final seal.
        seal2(&[(42, 1)]),
        Step::Push(vec![(12, t2(90, 1), 1)]),
        seal2(&[(43, 1)]),
        seal_all(),
    ];
    for theta in [0, 1, 5, 100] {
        check_all2(&steps, theta);
    }
}

/// Uppers that advance from the previous upper, mixing elements with and without a minimal
/// inner coordinate, with updates at or after some element of the last upper.
fn arb_product_steps() -> impl Strategy<Value = Vec<Step<Time2>>> {
    let op = prop_oneof![
        3 => proptest::collection::vec((0..6u64, 0..3usize, 0..40u64, 0..4u64, -2..=2i64), 0..20)
            .prop_map(|updates| (Some(updates), Vec::new())),
        2 => proptest::collection::vec((0..3usize, 0..20u64, 0..3u64), 1..4)
            .prop_map(|advances| (None, advances)),
    ];
    proptest::collection::vec(op, 0..40).prop_map(|ops| {
        let mut upper = vec![t2(0, 0)];
        let mut steps = Vec::new();
        for (updates, advances) in ops {
            match updates {
                Some(updates) => steps.push(Step::Push(
                    updates
                        .into_iter()
                        .map(|(data, base, outer, inner, diff)| {
                            let base = &upper[base % upper.len()];
                            (data, t2(base.outer + outer, base.inner + inner), diff)
                        })
                        .collect(),
                )),
                None => {
                    let mut next = Antichain::new();
                    for (base, outer, inner) in advances {
                        let base = &upper[base % upper.len()];
                        next.insert(t2(base.outer + outer, base.inner + inner));
                    }
                    upper = next.elements().to_vec();
                    steps.push(Step::Seal(next));
                }
            }
        }
        steps.push(seal_all());
        steps
    })
}

proptest! {
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn product_matches_plain(steps in arb_product_steps(), theta in prop_oneof![Just(0u64), Just(1), Just(7)]) {
        check_all2(&steps, theta);
    }
}
