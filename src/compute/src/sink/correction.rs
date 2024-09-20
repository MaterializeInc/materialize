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
use std::ops::{AddAssign, Bound, SubAssign};

use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::Data;
use itertools::Itertools;
use mz_ore::iter::IteratorExt;
use mz_persist_client::metrics::{SinkMetrics, SinkWorkerMetrics, UpdateDelta};
use mz_repr::{Diff, Timestamp};
use timely::progress::Antichain;
use timely::PartialOrder;

/// A collection holding `persist_sink` updates.
///
/// The `Correction` data structure is purpose-built for the `persist_sink::write_batches`
/// operator:
///
///  * It stores updates by time, to enable efficient separation between updates that should
///    be written to a batch and updates whose time has not yet arrived.
///  * It eschews an interface for directly removing previously inserted updates. Instead, updates
///    are removed by inserting them again, with negated diffs. Stored updates are continuously
///    consolidated to give them opportunity to cancel each other out.
///  * It provides an interface for advancing all contained updates to a given frontier.
pub(super) struct Correction<D> {
    /// Stashed updates by time.
    updates: BTreeMap<Timestamp, ConsolidatingVec<D>>,

    /// Total length and capacity of vectors in `updates`.
    ///
    /// Tracked to maintain metrics.
    total_size: LengthAndCapacity,
    /// Global persist sink metrics.
    metrics: SinkMetrics,
    /// Per-worker persist sink metrics.
    worker_metrics: SinkWorkerMetrics,
}

impl<D> Correction<D> {
    /// Construct a new `Correction` instance.
    pub fn new(metrics: SinkMetrics, worker_metrics: SinkWorkerMetrics) -> Self {
        Self {
            updates: Default::default(),
            total_size: Default::default(),
            metrics,
            worker_metrics,
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

impl<D: Data> Correction<D> {
    /// Insert a batch of updates.
    pub fn insert(&mut self, mut updates: Vec<(D, Timestamp, Diff)>) {
        consolidate_updates(&mut updates);
        updates.sort_unstable_by_key(|(_, time, _)| *time);

        let mut new_size = self.total_size;
        let mut updates = updates.into_iter().peekable();
        while let Some(&(_, time, _)) = updates.peek() {
            let data = updates
                .peeking_take_while(|(_, t, _)| *t == time)
                .map(|(d, _, r)| (d, r));

            use std::collections::btree_map::Entry;
            match self.updates.entry(time) {
                Entry::Vacant(entry) => {
                    let vec: ConsolidatingVec<_> = data.collect();
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
    pub fn updates_within(
        &mut self,
        lower: &Antichain<Timestamp>,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + ExactSizeIterator + '_ {
        assert!(PartialOrder::less_equal(lower, upper));

        let start = match lower.as_option() {
            Some(ts) => Bound::Included(*ts),
            None => Bound::Excluded(Timestamp::MAX),
        };
        let end = match upper.as_option() {
            Some(ts) => Bound::Excluded(*ts),
            None => Bound::Unbounded,
        };

        let mut new_size = self.total_size;

        // Consolidate relevant times and compute the total number of updates.
        let range = self.updates.range_mut((start, end));
        let update_count = range.fold(0, |acc, (_, data)| {
            new_size -= (data.len(), data.capacity());
            data.consolidate();
            new_size += (data.len(), data.capacity());
            acc + data.len()
        });

        self.update_metrics(new_size);

        let range = self.updates.range((start, end));
        range
            .flat_map(|(t, data)| data.iter().map(|(d, r)| (d.clone(), *t, *r)))
            .exact_size(update_count)
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
}

impl<D> Drop for Correction<D> {
    fn drop(&mut self) {
        self.update_metrics(Default::default());
    }
}

/// Helper type for convenient tracking of length and capacity together.
#[derive(Clone, Copy, Default)]
struct LengthAndCapacity {
    length: usize,
    capacity: usize,
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
}

impl<D: Ord> ConsolidatingVec<D> {
    pub fn with_min_capacity(min_capacity: usize) -> Self {
        ConsolidatingVec {
            data: Vec::new(),
            min_capacity,
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
    /// If the vector does not have sufficient capacity, we try to consolidate and/or double its
    /// capacity.
    ///
    /// The worst-case cost of this function is O(n log n) in the number of items the vector stores,
    /// but amortizes to O(1).
    pub fn push(&mut self, item: (D, Diff)) {
        let capacity = self.data.capacity();
        if self.data.len() == capacity {
            // The vector is full. First, consolidate to try to recover some space.
            self.consolidate();

            // If consolidation didn't free at least half the available capacity, double the
            // capacity. This ensures we won't consolidate over and over again with small gains.
            if self.data.len() > capacity / 2 {
                self.data.reserve(capacity);
            }
        }

        self.data.push(item);
    }

    /// Consolidate the contents.
    pub fn consolidate(&mut self) {
        consolidate(&mut self.data);

        // We may have the opportunity to reclaim allocated memory.
        // Given that `push` will double the capacity when the vector is more than half full, and
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
