// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`Batcher`] wrapper that parks far-future updates in a [`BucketChain`].
//!
//! The inner batcher re-merges and re-extracts every update it holds on each seal, so updates
//! far in advance of the sealed frontier cost work on every seal until they become ready. The
//! wrapper keeps updates whose key is at least `far_bound`, the last seal's inner-minimum key
//! plus the summary, in a chain of time-major buckets instead, and moves them into the inner batcher once
//! a seal's frontier catches up with their bucket.
//!
//! The snapshot and its future tail arrive before the first seal. A wrapper that knows the
//! dataflow's `as_of` parks the tail on arrival, see [`set_render_as_of`]. Without it nothing
//! parks before the first seal.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use columnar::{Columnar, Index, Len};
use differential_dataflow::logging::Logger;
use differential_dataflow::trace::{Batcher, Description};
use timely::container::{ContainerBuilder, PushInto};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, PathSummary, Timestamp};

use crate::columnar::Column;
use crate::columnar::body::ColumnBody;
use crate::columnar::chunk::{ColumnChunk, SpillGate};
use crate::columnation::ColumnationStack;
use crate::temporal::bucket::TimeMajorBucket;
use crate::temporal::{BucketChain, BucketTime};

/// Restore fuel spent per seal. Leftover work waits for the next seal, because the batcher
/// cannot reschedule its operator.
const RESTORE_FUEL: i64 = 1_000_000;

/// The summary, in key units, that [`TemporalBatcher::new`] reads.
static SUMMARY: AtomicU64 = AtomicU64::new(2_000);

/// Set the summary that subsequently constructed [`TemporalBatcher`]s use.
pub fn set_temporal_batcher_summary(summary: u64) {
    SUMMARY.store(summary, Ordering::Relaxed);
}

thread_local! {
    /// The `as_of` key that [`TemporalBatcher::new`] reads, see [`set_render_as_of`].
    static RENDER_AS_OF: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Set `as_of` as the key of the dataflow `as_of` that [`TemporalBatcher`]s constructed on
/// this thread read, until the returned guard drops and restores the previous value.
///
/// A batcher's updates must all be at or after the `as_of` it reads. Differential constructs
/// batchers while the dataflow is built, so the dataflow builder holds the guard while building.
#[must_use]
pub fn set_render_as_of(as_of: Option<u64>) -> RenderAsOfGuard {
    RenderAsOfGuard(RENDER_AS_OF.with(|cell| cell.replace(as_of)))
}

/// Restores the previous render `as_of` on drop, see [`set_render_as_of`].
pub struct RenderAsOfGuard(Option<u64>);

impl Drop for RenderAsOfGuard {
    fn drop(&mut self) {
        RENDER_AS_OF.with(|cell| cell.set(self.0));
    }
}

/// A batcher output chunk whose updates the [`TemporalBatcher`] can read by time.
pub trait TimedUpdates<D: Columnar, T: Columnar, R: Columnar> {
    /// The number of updates.
    fn update_count(&self) -> usize;
    /// True if `pred` holds for the time of some update.
    fn any_time(&self, pred: impl FnMut(&T) -> bool) -> bool;
    /// Append the updates whose time satisfies `pred` to `matched` and the others to `rest`,
    /// preserving order.
    fn partition_into(
        &self,
        pred: impl FnMut(&T) -> bool,
        matched: &mut Column<(D, T, R)>,
        rest: &mut Column<(D, T, R)>,
    );
}

fn view_any_time<D, T, R>(
    view: columnar::BorrowedOf<'_, (D, T, R)>,
    mut pred: impl FnMut(&T) -> bool,
) -> bool
where
    D: Columnar,
    T: Columnar + Timestamp,
    R: Columnar,
{
    let times = view.1;
    let mut scratch = T::minimum();
    (0..times.len()).any(|index| {
        scratch.copy_from(times.get(index));
        pred(&scratch)
    })
}

fn view_partition<D, T, R>(
    view: columnar::BorrowedOf<'_, (D, T, R)>,
    mut pred: impl FnMut(&T) -> bool,
    matched: &mut Column<(D, T, R)>,
    rest: &mut Column<(D, T, R)>,
) where
    D: Columnar,
    T: Columnar + Timestamp,
    R: Columnar,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<columnar::Ref<'a, (D, T, R)>>,
{
    let mut scratch = T::minimum();
    for index in 0..view.len() {
        let update = view.get(index);
        scratch.copy_from(update.1);
        if pred(&scratch) {
            matched.push_into(update);
        } else {
            rest.push_into(update);
        }
    }
}

impl<D, T, R> TimedUpdates<D, T, R> for ColumnBody<(D, T, R)>
where
    D: Columnar,
    T: Columnar + Timestamp,
    R: Columnar,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<columnar::Ref<'a, (D, T, R)>>,
{
    fn update_count(&self) -> usize {
        self.len()
    }
    fn any_time(&self, pred: impl FnMut(&T) -> bool) -> bool {
        view_any_time::<D, T, R>(self.borrow(), pred)
    }
    fn partition_into(
        &self,
        pred: impl FnMut(&T) -> bool,
        matched: &mut Column<(D, T, R)>,
        rest: &mut Column<(D, T, R)>,
    ) {
        view_partition(self.borrow(), pred, matched, rest)
    }
}

impl<D, T, R, G> TimedUpdates<D, T, R> for ColumnChunk<D, T, R, G>
where
    D: Columnar,
    T: Columnar + Timestamp,
    R: Columnar,
    G: SpillGate,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<columnar::Ref<'a, (D, T, R)>>,
{
    fn update_count(&self) -> usize {
        usize::try_from(timely::Accountable::record_count(self)).expect("non-negative")
    }
    fn any_time(&self, pred: impl FnMut(&T) -> bool) -> bool {
        self.with_body(|body| body.any_time(pred))
    }
    fn partition_into(
        &self,
        pred: impl FnMut(&T) -> bool,
        matched: &mut Column<(D, T, R)>,
        rest: &mut Column<(D, T, R)>,
    ) {
        self.with_body(|body| body.partition_into(pred, matched, rest))
    }
}

impl<D, T, R> TimedUpdates<D, T, R> for ColumnationStack<(D, T, R)>
where
    D: Columnar + columnation::Columnation,
    T: Columnar + columnation::Columnation,
    R: Columnar + columnation::Columnation,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<&'a (D, T, R)>,
{
    fn update_count(&self) -> usize {
        self.len()
    }
    fn any_time(&self, mut pred: impl FnMut(&T) -> bool) -> bool {
        self.iter().any(|(_, time, _)| pred(time))
    }
    fn partition_into(
        &self,
        mut pred: impl FnMut(&T) -> bool,
        matched: &mut Column<(D, T, R)>,
        rest: &mut Column<(D, T, R)>,
    ) {
        for update in self.iter() {
            if pred(&update.1) {
                matched.push_into(update);
            } else {
                rest.push_into(update);
            }
        }
    }
}

/// Counters for measuring the wrapper.
#[derive(Clone, Copy, Debug, Default)]
pub struct TemporalBatcherStats {
    /// Updates pushed into the chain.
    pub parked: usize,
}

/// A [`Batcher`] that parks updates at least the summary in advance of the last sealed frontier in a
/// [`BucketChain`], and otherwise defers to the inner batcher `B`.
///
/// `Chu` re-chunks columns for `B`: the near part of a split chunk and the updates peeled
/// from the chain.
pub struct TemporalBatcher<B, Chu, D, T, R>
where
    B: Batcher<Time = T>,
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: BucketTime + Columnar + Ord + Default + differential_dataflow::lattice::Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar
        + Default
        + differential_dataflow::difference::Semigroup
        + for<'a> differential_dataflow::difference::Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
{
    inner: B,
    chunker: Chu,
    chain: BucketChain<TimeMajorBucket<D, T, R>>,
    summary: u64,
    /// The key of the dataflow `as_of`, if known.
    as_of: Option<T::Key>,
    /// The key at or after which pushed updates park in the chain. `None` parks nothing.
    far_bound: Option<T::Key>,
    lower: Antichain<T>,
    frontier: Antichain<T>,
    near: Column<(D, T, R)>,
    far: Column<(D, T, R)>,
    buffer: Column<(D, T, R)>,
    keys: Vec<(T::Key, usize)>,
    stats: TemporalBatcherStats,
}

impl<B, Chu, D, T, R> TemporalBatcher<B, Chu, D, T, R>
where
    B: Batcher<Time = T>,
    B::Output: TimedUpdates<D, T, R>,
    Chu: ContainerBuilder<Container = B::Output> + for<'a> PushInto<&'a mut Column<(D, T, R)>>,
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: BucketTime + Columnar + Ord + Default + differential_dataflow::lattice::Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    <T::Key as Timestamp>::Summary: From<u64>,
    R: Columnar
        + Default
        + differential_dataflow::difference::Semigroup
        + for<'a> differential_dataflow::difference::Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<columnar::Ref<'a, (D, T, R)>>,
{
    /// Construct a wrapper that parks updates at least `summary` key units in advance of the
    /// last sealed frontier, or of `as_of` before the first seal.
    ///
    /// All pushed updates must be at or after `as_of`.
    pub fn with_config(
        logger: Option<Logger>,
        operator_id: usize,
        summary: u64,
        as_of: Option<T::Key>,
    ) -> Self {
        let mut chain = BucketChain::new(TimeMajorBucket::new(logger.clone(), operator_id));
        if let Some(as_of) = &as_of {
            // Shape the empty chain around `as_of`, so the tail lands in buckets of its own
            // size instead of one bucket spanning the domain that every seal splits.
            let _ = chain.peel(Antichain::from_elem(as_of.clone()).borrow());
            let mut fuel = 0;
            while fuel <= 0 {
                fuel = RESTORE_FUEL;
                chain.restore(&mut fuel);
            }
        }
        let far_bound = as_of
            .as_ref()
            .and_then(|as_of| <T::Key as Timestamp>::Summary::from(summary).results_in(as_of));
        Self {
            inner: B::new(logger, operator_id),
            chunker: Chu::default(),
            chain,
            summary,
            as_of,
            far_bound,
            lower: Antichain::from_elem(T::minimum()),
            frontier: Antichain::new(),
            near: Default::default(),
            far: Default::default(),
            buffer: Default::default(),
            keys: Vec::new(),
            stats: Default::default(),
        }
    }

    /// Counters for measuring the wrapper.
    pub fn stats(&self) -> TemporalBatcherStats {
        self.stats
    }

    /// The least key `e` with `lower_of(e)` in `upper`, if any.
    fn inner_minimum_key(upper: AntichainRef<T>) -> Option<T::Key> {
        upper
            .iter()
            .filter_map(|time| {
                let key = time.key();
                (T::lower_of(&key) == *time).then_some(key)
            })
            .min()
    }

    /// Move the contents of `self.far` into the chain, in per-bucket runs.
    fn park_far(&mut self) {
        let view = self.far.borrow();
        self.keys.clear();
        let mut scratch = T::minimum();
        for index in 0..view.len() {
            scratch.copy_from(view.1.get(index));
            self.keys.push((scratch.key(), index));
        }
        self.keys.sort_unstable();
        let mut buffered = None;
        for (key, index) in self.keys.drain(..) {
            let contained = buffered
                .as_ref()
                .is_some_and(|range: &crate::temporal::BucketRange<T::Key>| range.contains(&key));
            if !contained {
                if let Some(range) = buffered.take() {
                    let bucket = self.chain.find_mut(&range.start).expect("must exist");
                    bucket.push_container(&mut self.buffer);
                }
                buffered = Some(self.chain.range_of(&key).expect("must exist"));
            }
            self.buffer.push_into(view.get(index));
        }
        if let Some(range) = buffered {
            let bucket = self.chain.find_mut(&range.start).expect("must exist");
            bucket.push_container(&mut self.buffer);
        }
        self.far.clear();
    }

    /// Re-chunk `column` into the inner batcher.
    fn push_inner_column(&mut self, column: &mut Column<(D, T, R)>) {
        self.chunker.push_into(column);
        while let Some(chunk) = self.chunker.extract() {
            self.inner.push_into(std::mem::take(chunk));
        }
    }
}

impl<B, Chu, D, T, R> Batcher for TemporalBatcher<B, Chu, D, T, R>
where
    B: Batcher<Time = T>,
    B::Output: TimedUpdates<D, T, R>,
    Chu: ContainerBuilder<Container = B::Output> + for<'a> PushInto<&'a mut Column<(D, T, R)>>,
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: BucketTime + Columnar + Ord + Default + differential_dataflow::lattice::Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    <T::Key as Timestamp>::Summary: From<u64>,
    R: Columnar
        + Default
        + differential_dataflow::difference::Semigroup
        + for<'a> differential_dataflow::difference::Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<columnar::Ref<'a, (D, T, R)>>,
    T::Key: From<u64>,
{
    type Output = B::Output;
    type Time = T;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        let as_of = RENDER_AS_OF.with(Cell::get).map(T::Key::from);
        Self::with_config(logger, operator_id, SUMMARY.load(Ordering::Relaxed), as_of)
    }

    fn seal(&mut self, upper: Antichain<T>) -> (Vec<Self::Output>, Description<T>) {
        // Parking is sound relative to `edge`, the least key whose `lower_of` is in `upper`.
        // An update parks only with a key of at least `edge + summary`, and at every later seal at
        // `F'` it is either peeled or still has a key of at least `edge(F')`. Then
        // `lower_of(edge(F')) <= t` with `lower_of(edge(F'))` in `F'`, so `t` is not ready.
        // Updates that might be ready are therefore always in the inner batcher, which decides
        // readiness exactly. The argument assumes nothing about how frontiers of an iterative
        // scope evolve. Without such an element nothing parks and the chain drains, so the
        // wrapper behaves like the inner batcher. The same argument covers updates parked
        // before the first seal relative to `as_of`, because the first seal peels at its edge,
        // or entirely without one.
        //
        // Joining the edge with `as_of` keeps the far bound past the snapshot while uppers are
        // still before `as_of`, as the bucketing operator does with its input frontier.
        let edge = Self::inner_minimum_key(upper.borrow());
        let far_bound = edge.as_ref().and_then(|edge| {
            let base = match &self.as_of {
                Some(as_of) if as_of > edge => as_of,
                _ => edge,
            };
            <T::Key as Timestamp>::Summary::from(self.summary).results_in(base)
        });

        // Peeling and restoring an empty chain only splits empty buckets.
        let parked = self
            .chain
            .buckets()
            .any(|bucket| !bucket.lower().is_empty());
        if parked {
            let peel_at = Antichain::from_iter(edge);
            for bucket in self.chain.peel(peel_at.borrow()) {
                for mut column in bucket.done() {
                    self.push_inner_column(&mut column);
                }
            }
        }
        while let Some(chunk) = self.chunker.finish() {
            self.inner.push_into(std::mem::take(chunk));
        }

        let (readied, _) = self.inner.seal(upper.clone());
        let description = Description::new(
            self.lower.clone(),
            upper.clone(),
            Antichain::from_elem(T::minimum()),
        );
        self.lower = upper;
        self.far_bound = far_bound;

        if parked {
            let mut fuel = RESTORE_FUEL;
            self.chain.restore(&mut fuel);
        }

        (readied, description)
    }

    fn frontier(&mut self) -> AntichainRef<'_, T> {
        self.frontier.clear();
        for time in self.inner.frontier().iter() {
            self.frontier.insert_ref(time);
        }
        for bucket in self.chain.buckets() {
            for time in bucket.lower().iter() {
                self.frontier.insert_ref(time);
            }
        }
        // `arrange_core` downgrades its capabilities to this frontier, so an element below the
        // sealed frontier would hold back the arrangement's output.
        mz_ore::soft_assert_or_log!(
            self.frontier.iter().all(|time| self.lower.less_equal(time)),
            "temporal batcher frontier {:?} below sealed frontier {:?}",
            self.frontier,
            self.lower,
        );
        self.frontier.borrow()
    }
}

impl<B, Chu, D, T, R> PushInto<B::Output> for TemporalBatcher<B, Chu, D, T, R>
where
    B: Batcher<Time = T>,
    B::Output: TimedUpdates<D, T, R>,
    Chu: ContainerBuilder<Container = B::Output> + for<'a> PushInto<&'a mut Column<(D, T, R)>>,
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: BucketTime + Columnar + Ord + Default + differential_dataflow::lattice::Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    <T::Key as Timestamp>::Summary: From<u64>,
    R: Columnar
        + Default
        + differential_dataflow::difference::Semigroup
        + for<'a> differential_dataflow::difference::Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<columnar::Ref<'a, (D, T, R)>>,
{
    fn push_into(&mut self, chunk: B::Output) {
        let Some(far_bound) = self.far_bound.clone() else {
            self.inner.push_into(chunk);
            return;
        };
        if !chunk.any_time(|time| time.key() >= far_bound) {
            self.inner.push_into(chunk);
            return;
        }
        chunk.partition_into(
            |time| time.key() >= far_bound,
            &mut self.far,
            &mut self.near,
        );
        drop(chunk);
        self.stats.parked += self.far.borrow().len();
        self.park_far();
        let mut near = std::mem::take(&mut self.near);
        if !near.is_empty() {
            self.push_inner_column(&mut near);
        }
        near.clear();
        self.near = near;
    }
}

#[cfg(test)]
mod tests;
