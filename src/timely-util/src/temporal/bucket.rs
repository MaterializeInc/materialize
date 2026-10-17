// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Spillable, time-major bucket storage for a [`BucketChain`](super::BucketChain).

use columnar::{Columnar, Container as _, Len, Push};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Batcher;
use timely::container::{ContainerBuilder as _, PushInto as _};
use timely::progress::{Antichain, Timestamp};

use crate::columnar::Column;
use crate::columnar::chunk::{AccountedChunkBatcher, ChunkChunker, ComputeSpill};
use crate::temporal::{Bucket, BucketTimestamp};

/// A wrapper around [`AccountedChunkBatcher`] that implements the bucketing API.
///
/// This is the same merge batcher the arrange sites' chunked arm uses, so the
/// bucket chain and those arrangements share one merge-batcher implementation.
/// The choice is unconditional here: the bucket chain does not consult the
/// arrange batcher selector.
///
/// The batcher consumes sorted, consolidated chunks, so this
/// wrapper carries a [`ChunkChunker`] that sorts and consolidates the input
/// columns into chunks within the batcher's byte bound.
///
/// Updates are stored time-major, as `((time, data), time, diff)`, so each
/// chunk covers a narrow range of times. Splitting at a time then loads only
/// the chunks whose time bounds straddle it, and moves every other chunk
/// whole, spilled or not. With data-major chunks every chunk spans the
/// bucket's times, and each split loads and rewrites the whole bucket. The
/// leading copy of the time orders the chunks, and the trailing copy is the
/// batcher's time, which `seal` partitions by.
pub struct TimeMajorBucket<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Ord + Default + Timestamp + Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
{
    logger: Option<differential_dataflow::logging::Logger>,
    operator_id: usize,
    chunker: ChunkChunker<(T, D), T, R, ComputeSpill>,
    inner: AccountedChunkBatcher<(T, D), T, R, ComputeSpill>,
}

impl<D, T, R> TimeMajorBucket<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Ord + Clone + Default + Timestamp + Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: Push<&'a R>,
{
    /// Construct a new `TimeMajorBucket` with the given logger and operator ID.
    pub fn new(logger: Option<differential_dataflow::logging::Logger>, operator_id: usize) -> Self {
        Self {
            logger: logger.clone(),
            operator_id,
            chunker: ChunkChunker::default(),
            inner: AccountedChunkBatcher::new(logger, operator_id),
        }
    }

    /// Consolidate `buffer` through the chunker and feed any complete chunks to
    /// the batcher. Leaves `buffer` empty, retaining its allocation.
    pub fn push_container(&mut self, buffer: &mut Column<(D, T, R)>) {
        if buffer.is_empty() {
            return;
        }
        // A fresh time-major copy per push. Retaining it would keep a
        // push-sized allocation in every bucket of the chain, outside the
        // pool and the batcher's size accounting.
        let mut staging = <((T, D), T, R) as Columnar>::Container::default();
        let (data, time, diff) = buffer.borrow();
        staging.0.0.extend_from_self(time, 0..time.len());
        staging.0.1.extend_from_self(data, 0..data.len());
        staging.1.extend_from_self(time, 0..time.len());
        staging.2.extend_from_self(diff, 0..diff.len());
        buffer.clear();
        self.chunker.push_into(&mut Column::Typed(staging));
        while let Some(chunk) = self.chunker.extract() {
            self.inner.push_into(std::mem::take(chunk));
        }
    }

    /// Flush any partial chunk still held by the chunker into the batcher.
    fn flush(&mut self) {
        while let Some(chunk) = self.chunker.finish() {
            self.inner.push_into(std::mem::take(chunk));
        }
    }

    /// Reveal the contents of the merge batcher, as one column per chunk.
    ///
    /// A spilled chunk loads when the iterator reaches it.
    pub fn done(mut self) -> impl Iterator<Item = Column<(D, T, R)>> {
        self.flush();
        let (chain, _description) = self.inner.seal(Antichain::new());
        chain.into_iter().map(|chunk| {
            let body = chunk.into_body();
            let ((_, data), time, diff) = body.borrow();
            let mut column = <(D, T, R) as Columnar>::Container::default();
            column.0.extend_from_self(data, 0..data.len());
            column.1.extend_from_self(time, 0..time.len());
            column.2.extend_from_self(diff, 0..diff.len());
            Column::Typed(column)
        })
    }
}

impl<D, T, R> Bucket for TimeMajorBucket<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Ord + Clone + Default + Lattice + BucketTimestamp,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: Push<&'a R>,
{
    type Timestamp = T;

    fn split(mut self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self) {
        self.flush();
        let upper = Antichain::from_elem(timestamp.clone());
        let mut lower = Self::new(self.logger.clone(), self.operator_id);
        // Sealing at `timestamp` ships exactly the updates strictly less than it,
        // as sorted, consolidated chunks; feed them to the lower batcher whole,
        // so spilled bodies move without being loaded. The `fuel` charge covers
        // the sealing work those records paid for.
        let (chain, _description) = self.inner.seal(upper);
        for chunk in chain {
            *fuel = fuel.saturating_sub(timely::Accountable::record_count(&chunk));
            lower.inner.push_into(chunk);
        }
        (lower, self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use columnar::Index;
    use mz_ore::pool::Pool;

    use super::*;
    use crate::columnar::chunk::set_spill_override;
    use crate::temporal::{BucketChain, BucketRange};

    type Wrapper = TimeMajorBucket<u64, u64, i64>;
    type Update = (u64, u64, i64);

    /// Updates with colliding `(data, time)` pairs and retractions, spread
    /// over times `[1_000, 101_000)`.
    fn updates(round: u64) -> Vec<Update> {
        (0..20_000u64)
            .map(|i| {
                let x = (i + round * 20_000).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let data = (x >> 8) % 5_000;
                let time = 1_000 + (x >> 24) % 100_000;
                let diff = if x & (1 << 40) == 0 { 1 } else { -1 };
                (data, time, diff)
            })
            .collect()
    }

    /// Push `updates` into the buckets of `chain` that hold their times, the
    /// way the operator does.
    fn insert(chain: &mut BucketChain<Wrapper>, mut updates: Vec<Update>) {
        updates.sort_by_key(|(_, time, _)| *time);
        let mut buffer = Column::default();
        let mut range: Option<BucketRange<u64>> = None;
        for update in &updates {
            if !range
                .as_ref()
                .is_some_and(|range| range.contains(&update.1))
            {
                if let Some(range) = range.take() {
                    let bucket = chain.find_mut(&range.start).expect("must exist");
                    bucket.push_container(&mut buffer);
                }
                range = Some(chain.range_of(&update.1).expect("must exist"));
            }
            buffer.push_into(update);
        }
        if let Some(range) = range {
            let bucket = chain.find_mut(&range.start).expect("must exist");
            bucket.push_container(&mut buffer);
        }
    }

    fn accumulate(map: &mut BTreeMap<(u64, u64), i64>, updates: impl Iterator<Item = Update>) {
        for (data, time, diff) in updates {
            *map.entry((data, time)).or_default() += diff;
        }
        map.retain(|_, diff| *diff != 0);
    }

    /// Peels at a sequence of frontiers, with updates arriving between peels
    /// for times in buckets that earlier peels split, and compares each
    /// peel's output with the reference accumulation of the times it covers.
    fn peel_matches_reference() {
        let mut chain = BucketChain::new(Wrapper::new(None, 0));
        let mut reference = BTreeMap::new();
        let mut lower = 0;
        for round in 0..6u64 {
            let fresh: Vec<_> = updates(round)
                .into_iter()
                .filter(|(_, time, _)| lower <= *time)
                .collect();
            accumulate(&mut reference, fresh.iter().copied());
            insert(&mut chain, fresh);

            let upper = (round < 5).then(|| 1_000 + (round + 1) * 17_000);
            let frontier = Antichain::from_iter(upper);
            let mut revealed = BTreeMap::new();
            for column in chain
                .peel(frontier.borrow())
                .into_iter()
                .flat_map(|x| x.done())
            {
                accumulate(
                    &mut revealed,
                    column.borrow().into_index_iter().map(Update::into_owned),
                );
            }
            let mut expected = BTreeMap::new();
            reference.retain(|(data, time), diff| {
                if frontier.less_equal(time) {
                    true
                } else {
                    expected.insert((*data, *time), *diff);
                    false
                }
            });
            assert!(!expected.is_empty(), "round {round} reveals nothing");
            assert_eq!(revealed, expected, "round {round}");

            let mut fuel = 0;
            while fuel <= 0 {
                fuel = 1_000_000;
                chain.restore(&mut fuel);
            }
            lower = upper.unwrap_or(u64::MAX);
        }
        assert!(reference.is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn peel_matches_reference_resident() {
        peel_matches_reference();
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn peel_matches_reference_spilled() {
        let pool = Pool::new().expect("pool creation");
        set_spill_override(Some(pool.clone()));
        peel_matches_reference();
        set_spill_override(None);
        assert!(pool.stats().inserts > 0, "nothing spilled");
    }
}
