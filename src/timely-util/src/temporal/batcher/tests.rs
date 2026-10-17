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
type ChunkArm = (
    AccountedChunkBatcher<u64, u64, i64, ComputeSpill>,
    ChunkChunker<u64, u64, i64, ComputeSpill>,
);
type ColumnationArm = (
    MergeBatcher<ColInternalMerger<u64, u64, i64>>,
    Chunker<ColumnationStack<(u64, u64, i64)>>,
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
        check_arm::<ColumnArm<u64>>(&edge_cases(), theta);
        check_arm::<ChunkArm>(&edge_cases(), theta);
        check_arm::<ColumnationArm>(&edge_cases(), theta);
    }
}

fn check_arm<A: Arm>(steps: &[Step<u64>], theta: u64) -> TemporalBatcherStats {
    A::check(steps, theta)
}

/// A batcher and chunker pair, as one type parameter.
trait Arm {
    fn check(steps: &[Step<u64>], theta: u64) -> TemporalBatcherStats;
}

impl<B, Chu> Arm for (B, Chu)
where
    B: Batcher<Time = u64>,
    B::Output: TimedUpdates<u64, u64, i64>,
    Chu:
        ContainerBuilder<Container = B::Output> + for<'a> PushInto<&'a mut Column<(u64, u64, i64)>>,
{
    fn check(steps: &[Step<u64>], theta: u64) -> TemporalBatcherStats {
        check::<B, Chu, u64>(steps, theta)
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn hydration_matches_plain() {
    let steps = hydration(1_000, 20_000, 100_000, 1_000, 20);
    for theta in [0, 1_000, 10_000] {
        let stats = check_arm::<ColumnArm<u64>>(&steps, theta);
        assert!(stats.first_seal_parked > 0, "θ = {theta}: nothing parked");
        check_arm::<ChunkArm>(&steps, theta);
        check_arm::<ColumnationArm>(&steps, theta);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn hydration_matches_plain_spilled() {
    let pool = Pool::new().expect("pool creation");
    set_spill_override(Some(pool.clone()));
    let steps = hydration(1_000, 200_000, 100_000, 1_000, 20);
    check_arm::<ChunkArm>(&steps, 1_000);
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
        check_arm::<ColumnArm<u64>>(&steps, theta);
        check_arm::<ChunkArm>(&steps, theta);
        check_arm::<ColumnationArm>(&steps, theta);
    }
}
