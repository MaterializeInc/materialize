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
//! plus θ, in a chain of time-major buckets instead, and moves them into the inner batcher once
//! a seal's frontier catches up with their bucket.
//!
//! The batcher does not know the dataflow's `as_of`, so the snapshot and its future tail all
//! arrive before the first seal. The first seal with a far bound rebuilds the inner batcher,
//! moving its future tail into the chain.

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

/// θ, in key units, that [`TemporalBatcher::new`] reads.
static THETA: AtomicU64 = AtomicU64::new(2_000);

/// Set the θ that subsequently constructed [`TemporalBatcher`]s use.
pub fn set_temporal_batcher_theta(theta: u64) {
    THETA.store(theta, Ordering::Relaxed);
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
    /// Updates the rebuild moved out of the old inner batcher.
    pub rebuild_touched: usize,
    /// Updates the rebuild parked in the chain.
    pub rebuild_parked: usize,
}

/// A [`Batcher`] that parks updates at least θ in advance of the last sealed frontier in a
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
    theta: u64,
    /// The key at or after which pushed updates park in the chain. `None` parks nothing.
    far_bound: Option<T::Key>,
    rebuilt: bool,
    lower: Antichain<T>,
    frontier: Antichain<T>,
    logger: Option<Logger>,
    operator_id: usize,
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
    /// Construct a wrapper that parks updates at least `theta` key units in advance of the
    /// last sealed frontier.
    pub fn with_theta(logger: Option<Logger>, operator_id: usize, theta: u64) -> Self {
        Self {
            inner: B::new(logger.clone(), operator_id),
            chunker: Chu::default(),
            chain: BucketChain::new(TimeMajorBucket::new(logger.clone(), operator_id)),
            theta,
            far_bound: None,
            rebuilt: false,
            lower: Antichain::from_elem(T::minimum()),
            frontier: Antichain::new(),
            logger,
            operator_id,
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

    /// Seal the inner batcher at `upper`, and move its remaining updates with key at least
    /// `far` into the chain. The others move into a fresh inner batcher, by sealing at
    /// `{lower_of(far)}` and then at the empty frontier.
    fn rebuild(&mut self, upper: &Antichain<T>, far: &T::Key) -> Vec<B::Output> {
        let fresh = B::new(self.logger.clone(), self.operator_id);
        let mut old = std::mem::replace(&mut self.inner, fresh);
        let (readied, _) = old.seal(upper.clone());
        let (near, _) = old.seal(Antichain::from_elem(T::lower_of(far)));
        for chunk in near {
            self.stats.rebuild_touched += chunk.update_count();
            self.inner.push_into(chunk);
        }
        let (rest, _) = old.seal(Antichain::new());
        for chunk in rest {
            let count = chunk.update_count();
            self.stats.rebuild_touched += count;
            self.stats.rebuild_parked += count;
            chunk.partition_into(|_| true, &mut self.far, &mut self.near);
            self.park_far();
        }
        readied
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
{
    type Output = B::Output;
    type Time = T;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self::with_theta(logger, operator_id, THETA.load(Ordering::Relaxed))
    }

    fn seal(&mut self, upper: Antichain<T>) -> (Vec<Self::Output>, Description<T>) {
        // Parking is sound relative to `edge`, the least key whose `lower_of` is in `upper`.
        // An update parks only with a key of at least `edge + θ`, and at every later seal at
        // `F'` it is either peeled or still has a key of at least `edge(F')`. Then
        // `lower_of(edge(F')) <= t` with `lower_of(edge(F'))` in `F'`, so `t` is not ready.
        // Updates that might be ready are therefore always in the inner batcher, which decides
        // readiness exactly. The argument assumes nothing about how frontiers of an iterative
        // scope evolve. Without such an element nothing parks and the chain drains, so the
        // wrapper behaves like the inner batcher.
        let edge = Self::inner_minimum_key(upper.borrow());
        let far_bound = edge
            .as_ref()
            .and_then(|edge| <T::Key as Timestamp>::Summary::from(self.theta).results_in(edge));

        // Until a seal yields a far bound, nothing parks and the inner batcher holds every
        // update, including a hydration tail, so the rebuild waits for the first seal that has
        // one. NOTE: with advancing uppers, a seal without an inner-minimum element is never
        // followed by one with, because `lower_of(k)` is at or after an earlier element only if
        // that element is inner-minimum too. Waiting only costs a check.
        let mut rebuilt = None;
        if !self.rebuilt
            && let Some(far) = &far_bound
        {
            self.rebuilt = true;
            rebuilt = Some(self.rebuild(&upper, far));
        }

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

        let (mut readied, _) = self.inner.seal(upper.clone());
        if let Some(rebuilt) = rebuilt {
            // The fresh inner batcher and the chain only hold updates the rebuild found not
            // ready, so the rebuild's chain is the whole output. The seal above still runs to
            // establish the fresh batcher's frontier.
            assert!(
                readied.iter().all(|chunk| chunk.update_count() == 0),
                "temporal batcher readied updates after its rebuild"
            );
            readied = rebuilt;
        }
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
