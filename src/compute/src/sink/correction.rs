// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `Correction` data structure used by `persist_sink::write_batches` to stash updates before
//! they are written into batches.

use std::collections::BTreeMap;
use std::ops::{AddAssign, Bound, RangeBounds, SubAssign};

use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use itertools::Itertools;
use mz_compute_types::dyncfgs::{CONSOLIDATING_VEC_GROWTH_DAMPENER, ENABLE_CORRECTION_V2};
use mz_dyncfg::ConfigSet;
use mz_ore::iter::IteratorExt;
use mz_persist_client::metrics::{SinkMetrics, SinkWorkerMetrics, UpdateDelta};
use mz_repr::{Diff, Timestamp};
use timely::PartialOrder;
use timely::progress::Antichain;

use crate::sink::correction_v2::{CorrectionV2, Data};

/// A data structure suitable for storing updates in a self-correcting persist sink.
///
/// Selects one of two correction buffer implementations. `V1` is the original simple
/// implementation that stores updates in non-spillable memory. `V2` improves on `V1` by supporting
/// spill-to-disk but is less battle-tested so for now we want to keep the option of reverting to
/// `V1` in a pinch. The plan is to remove `V1` eventually.
pub(super) enum Correction<D: Data> {
    V1(CorrectionV1<D>),
    V2(CorrectionV2<D>),
}

impl<D: Data> Correction<D> {
    /// Construct a new `Correction` instance.
    pub fn new(
        metrics: SinkMetrics,
        worker_metrics: SinkWorkerMetrics,
        config: &ConfigSet,
    ) -> Self {
        if ENABLE_CORRECTION_V2.get(config) {
            Self::V2(CorrectionV2::new(metrics, worker_metrics))
        } else {
            let growth_dampener = CONSOLIDATING_VEC_GROWTH_DAMPENER.get(config);
            Self::V1(CorrectionV1::new(metrics, worker_metrics, growth_dampener))
        }
    }

    /// Insert a batch of updates.
    pub fn insert(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        match self {
            Self::V1(c) => c.insert(updates),
            Self::V2(c) => c.insert(updates),
        }
    }

    /// Insert a batch of updates, after negating their diffs.
    pub fn insert_negated(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        match self {
            Self::V1(c) => c.insert_negated(updates),
            Self::V2(c) => c.insert_negated(updates),
        }
    }

    /// Consolidate and return updates before the given `upper`.
    pub fn updates_before(
        &mut self,
        upper: &Antichain<Timestamp>,
    ) -> Box<dyn Iterator<Item = (D, Timestamp, Diff)> + '_> {
        match self {
            Self::V1(c) => Box::new(c.updates_before(upper)),
            Self::V2(c) => Box::new(c.updates_before(upper)),
        }
    }

    /// Advance the since frontier.
    ///
    /// # Panics
    ///
    /// Panics if the given `since` is less than the current since frontier.
    pub fn advance_since(&mut self, since: Antichain<Timestamp>) {
        match self {
            Self::V1(c) => c.advance_since(since),
            Self::V2(c) => c.advance_since(since),
        }
    }

    /// Consolidate all updates at the current `since`.
    pub fn consolidate_at_since(&mut self) {
        match self {
            Self::V1(c) => c.consolidate_at_since(),
            Self::V2(c) => c.consolidate_at_since(),
        }
    }
}

/// A collection holding `persist_sink` updates.
///
/// The `CorrectionV1` data structure is purpose-built for the `persist_sink::write_batches`
/// operator:
///
///  * It stores updates by time, to enable efficient separation between updates that should
///    be written to a batch and updates whose time has not yet arrived.
///  * It eschews an interface for directly removing previously inserted updates. Instead, updates
///    are removed by inserting them again, with negated diffs. Stored updates are continuously
///    consolidated to give them opportunity to cancel each other out.
///  * It provides an interface for advancing all contained updates to a given frontier.
pub(super) struct CorrectionV1<D> {
    /// Stashed updates by time.
    updates: BTreeMap<Timestamp, ConsolidatingVec<D>>,
    /// Frontier to which all update times are advanced.
    since: Antichain<Timestamp>,

    /// Total length and capacity of vectors in `updates`.
    ///
    /// Tracked to maintain metrics.
    total_size: LengthAndCapacity,
    /// Global persist sink metrics.
    metrics: SinkMetrics,
    /// Per-worker persist sink metrics.
    worker_metrics: SinkWorkerMetrics,
    /// Configuration for `ConsolidatingVec` driving the growth rate down from doubling.
    growth_dampener: usize,
}

impl<D> CorrectionV1<D> {
    /// Construct a new `CorrectionV1` instance.
    pub fn new(
        metrics: SinkMetrics,
        worker_metrics: SinkWorkerMetrics,
        growth_dampener: usize,
    ) -> Self {
        Self {
            updates: Default::default(),
            since: Antichain::from_elem(Timestamp::MIN),
            total_size: Default::default(),
            metrics,
            worker_metrics,
            growth_dampener,
        }
    }

    /// Update persist sink metrics to the given new length and capacity.
    fn update_metrics(&mut self, new_size: LengthAndCapacity) {
        let old_size = self.total_size;
        let len_delta = UpdateDelta::new(new_size.length, old_size.length);
        let cap_delta = UpdateDelta::new(new_size.capacity, old_size.capacity);
        self.metrics
            .report_correction_update_deltas(len_delta, cap_delta);
        self.worker_metrics
            .report_correction_update_totals(new_size.length, new_size.capacity);

        self.total_size = new_size;
    }
}

impl<D: Data> CorrectionV1<D> {
    /// Insert a batch of updates.
    pub fn insert(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        let Some(since_ts) = self.since.as_option() else {
            // If the since frontier is empty, discard all updates.
            return;
        };

        for (_, time, _) in &mut *updates {
            *time = std::cmp::max(*time, *since_ts);
        }
        self.insert_inner(updates);
    }

    /// Insert a batch of updates, after negating their diffs.
    pub fn insert_negated(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        let Some(since_ts) = self.since.as_option() else {
            // If the since frontier is empty, discard all updates.
            updates.clear();
            return;
        };

        for (_, time, diff) in &mut *updates {
            *time = std::cmp::max(*time, *since_ts);
            *diff = -*diff;
        }
        self.insert_inner(updates);
    }

    /// Insert a batch of updates.
    ///
    /// The given `updates` must all have been advanced by `self.since`.
    fn insert_inner(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        consolidate_updates(updates);
        updates.sort_unstable_by_key(|(_, time, _)| *time);

        let mut new_size = self.total_size;
        let mut updates = updates.drain(..).peekable();
        while let Some(&(_, time, _)) = updates.peek() {
            debug_assert!(
                self.since.less_equal(&time),
                "update not advanced by `since`"
            );

            let data = updates
                .peeking_take_while(|(_, t, _)| *t == time)
                .map(|(d, _, r)| (d, r));

            use std::collections::btree_map::Entry;
            match self.updates.entry(time) {
                Entry::Vacant(entry) => {
                    let mut vec: ConsolidatingVec<_> = data.collect();
                    vec.growth_dampener = self.growth_dampener;
                    new_size += (vec.len(), vec.capacity());
                    entry.insert(vec);
                }
                Entry::Occupied(mut entry) => {
                    let vec = entry.get_mut();
                    new_size -= (vec.len(), vec.capacity());
                    vec.extend(data);
                    new_size += (vec.len(), vec.capacity());
                }
            }
        }

        self.update_metrics(new_size);
    }

    /// Consolidate and return updates within the given bounds.
    ///
    /// # Panics
    ///
    /// Panics if `lower` is not less than or equal to `upper`.
    pub fn updates_within<'a>(
        &'a mut self,
        lower: &Antichain<Timestamp>,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + ExactSizeIterator + use<'a, D> {
        assert!(PartialOrder::less_equal(lower, upper));

        let start = match lower.as_option() {
            Some(ts) => Bound::Included(*ts),
            None => Bound::Excluded(Timestamp::MAX),
        };
        let end = match upper.as_option() {
            Some(ts) => Bound::Excluded(*ts),
            None => Bound::Unbounded,
        };

        let update_count = self.consolidate((start, end));

        let range = self.updates.range((start, end));
        range
            .flat_map(|(t, data)| data.iter().map(|(d, r)| (d.clone(), *t, *r)))
            .exact_size(update_count)
    }

    /// Consolidate and return updates before the given `upper`.
    pub fn updates_before<'a>(
        &'a mut self,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + ExactSizeIterator + use<'a, D> {
        let lower = Antichain::from_elem(Timestamp::MIN);
        self.updates_within(&lower, upper)
    }

    /// Consolidate the updates at the times in the given range.
    ///
    /// Returns the number of updates remaining in the range afterwards.
    fn consolidate<R>(&mut self, range: R) -> usize
    where
        R: RangeBounds<Timestamp>,
    {
        let mut new_size = self.total_size;

        let updates = self.updates.range_mut(range);
        let count = updates.fold(0, |acc, (_, data)| {
            new_size -= (data.len(), data.capacity());
            data.consolidate();
            new_size += (data.len(), data.capacity());
            acc + data.len()
        });

        self.update_metrics(new_size);
        count
    }

    /// Advance the since frontier.
    ///
    /// # Panics
    ///
    /// Panics if the given `since` is less than the current since frontier.
    pub fn advance_since(&mut self, since: Antichain<Timestamp>) {
        assert!(PartialOrder::less_equal(&self.since, &since));

        if since != self.since {
            self.advance_by(&since);
            self.since = since;
        }
    }

    /// Advance all contained updates by the given frontier.
    ///
    /// If the given frontier is empty, all remaining updates are discarded.
    pub fn advance_by(&mut self, frontier: &Antichain<Timestamp>) {
        let Some(target_ts) = frontier.as_option() else {
            self.updates.clear();
            self.update_metrics(Default::default());
            return;
        };

        let mut new_size = self.total_size;
        while let Some((ts, data)) = self.updates.pop_first() {
            if frontier.less_equal(&ts) {
                // We have advanced all updates that can advance.
                self.updates.insert(ts, data);
                break;
            }

            use std::collections::btree_map::Entry;
            match self.updates.entry(*target_ts) {
                Entry::Vacant(entry) => {
                    entry.insert(data);
                }
                Entry::Occupied(mut entry) => {
                    let vec = entry.get_mut();
                    new_size -= (data.len(), data.capacity());
                    new_size -= (vec.len(), vec.capacity());
                    vec.extend(data);
                    new_size += (vec.len(), vec.capacity());
                }
            }
        }

        self.update_metrics(new_size);
    }

    /// Consolidate all updates at the current `since`.
    pub fn consolidate_at_since(&mut self) {
        let Some(since_ts) = self.since.as_option() else {
            return;
        };

        let start = Bound::Included(*since_ts);
        let end = match since_ts.try_step_forward() {
            Some(ts) => Bound::Excluded(ts),
            None => Bound::Unbounded,
        };

        self.consolidate((start, end));
    }
}

impl<D> Drop for CorrectionV1<D> {
    fn drop(&mut self) {
        self.update_metrics(Default::default());
    }
}

/// Helper type for convenient tracking of length and capacity together.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct LengthAndCapacity {
    pub length: usize,
    pub capacity: usize,
}

impl AddAssign<Self> for LengthAndCapacity {
    fn add_assign(&mut self, size: Self) {
        self.length += size.length;
        self.capacity += size.capacity;
    }
}

impl AddAssign<(usize, usize)> for LengthAndCapacity {
    fn add_assign(&mut self, (len, cap): (usize, usize)) {
        self.length += len;
        self.capacity += cap;
    }
}

impl SubAssign<(usize, usize)> for LengthAndCapacity {
    fn sub_assign(&mut self, (len, cap): (usize, usize)) {
        self.length -= len;
        self.capacity -= cap;
    }
}

/// A vector that consolidates its contents.
///
/// The vector is filled with updates until it reaches capacity. At this point, the updates are
/// consolidated to free up space. This process repeats until the consolidation recovered less than
/// half of the vector's capacity, at which point the capacity is doubled.
#[derive(Debug)]
pub(crate) struct ConsolidatingVec<D> {
    data: Vec<(D, Diff)>,
    /// A lower bound for how small we'll shrink the Vec's capacity. NB: The cap
    /// might start smaller than this.
    min_capacity: usize,
    /// Dampener in the growth rate. 0 corresponds to doubling and in general `n` to `1+1/(n+1)`.
    ///
    /// If consolidation didn't free enough space, at least a linear amount, increase the capacity
    /// Setting this to 0 results in doubling whenever the list is at least half full.
    /// Larger numbers result in more conservative approaches that use more CPU, but less memory.
    growth_dampener: usize,
}

impl<D: Ord> ConsolidatingVec<D> {
    /// Creates a new instance from the necessary configuration arguments.
    pub fn new(min_capacity: usize, growth_dampener: usize) -> Self {
        ConsolidatingVec {
            data: Vec::new(),
            min_capacity,
            growth_dampener,
        }
    }

    /// Return the length of the vector.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Return the capacity of the vector.
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Pushes `item` into the vector.
    ///
    /// If the vector does not have sufficient capacity, we'll first consolidate and then increase
    /// its capacity if the consolidated results still occupy a significant fraction of the vector.
    ///
    /// The worst-case cost of this function is O(n log n) in the number of items the vector stores,
    /// but amortizes to O(log n).
    pub fn push(&mut self, item: (D, Diff)) {
        let capacity = self.data.capacity();
        if self.data.len() == capacity {
            // The vector is full. First, consolidate to try to recover some space.
            self.consolidate();

            // We may need more capacity if our current capacity is within `1+1/(n+1)` of the length.
            // This corresponds to `cap < len + len/(n+1)`, which is the logic we use.
            let length = self.data.len();
            let dampener = self.growth_dampener;
            if capacity < length + length / (dampener + 1) {
                // We would like to increase the capacity by a factor of `1+1/(n+1)`, which involves
                // determining the target capacity, and then reserving an amount that achieves this
                // while working around the existing length.
                let new_cap = capacity + capacity / (dampener + 1);
                self.data.reserve_exact(new_cap - length);
            }
        }

        self.data.push(item);
    }

    /// Consolidate the contents.
    pub fn consolidate(&mut self) {
        consolidate(&mut self.data);

        // We may have the opportunity to reclaim allocated memory.
        // Given that `push` will at most double the capacity when the vector is more than half full, and
        // we want to avoid entering into a resizing cycle, we choose to only shrink if the
        // vector's length is less than one fourth of its capacity.
        if self.data.len() < self.data.capacity() / 4 {
            self.data.shrink_to(self.min_capacity);
        }
    }

    /// Return an iterator over the borrowed items.
    pub fn iter(&self) -> impl Iterator<Item = &(D, Diff)> {
        self.data.iter()
    }

    /// Returns mutable access to the underlying items.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut (D, Diff)> {
        self.data.iter_mut()
    }
}

impl<D> IntoIterator for ConsolidatingVec<D> {
    type Item = (D, Diff);
    type IntoIter = std::vec::IntoIter<(D, Diff)>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<D> FromIterator<(D, Diff)> for ConsolidatingVec<D> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (D, Diff)>,
    {
        Self {
            data: Vec::from_iter(iter),
            min_capacity: 0,
            growth_dampener: 0,
        }
    }
}

impl<D: Ord> Extend<(D, Diff)> for ConsolidatingVec<D> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (D, Diff)>,
    {
        for item in iter {
            self.push(item);
        }
    }
}
