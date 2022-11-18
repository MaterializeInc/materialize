// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::HashMap;

use timely::order::PartialOrder;
use timely::progress::frontier::{Antichain, MutableAntichain};

use mz_expr::PartitionId;
use mz_repr::Diff;
use mz_timely_util::order::Partitioned;

use crate::types::sources::MzOffset;

/// OffsetAntichain is similar to a timely `Antichain<(PartitionId, T: TotalOrder)>`,
/// but additionally:
///
/// - Uses a HashMap as the implementation to allow absence of a `PartitionId` to mean
/// that `PartitionId` is at `T::minimum`. This helps avoid needing to hold onto a HUGE
/// `Antichain` for all possible `PartitionId`s
///     - Note this means that a partition being "finished" (like a normal "empty"
///     `Antichain`, is not currently supported, but could be added
///     - Note that this `Antichain` can also have been filtered, as in, missing some
///     partitions for which data exists but we don't care about. This is semantically
///     different than if we just don't have data, but it is represented the same
/// - Is not generic over `T`, but instead uses `MzOffset`, which:
///     - implements `TotalOrder`
///     - implements `checked_sub`
/// - Allows users to go from a _frontier_ to an actual set of offsets that are
/// connected to real data.
///     - This is a consequence of implementation, where the _frontier_ is ALWAYS
///     generated from real data offsets, in an invertible way.
///
/// `OffsetAntichain` has 4 sets of Api's:
/// - "read" apis like `get` and `as_vec`
/// - "mutation" apis (currently only `filter_by_partition`)
/// - And 2 "write" apis, that should primarily be used separately from each other:
///   - "Frontier" apis, which directly manipulate the underlying frontier.
///   Useful for implementing primitives like reclocking
///   - "Data" apis, that maintain special invariants:
///     - `insert_data_up_to` updates the frontier based on a given offset
///     that is associated with actual data.
///     - `as_data_offsets` inverts the behavior of `insert_data_up_to`
///     and returns a `HashMap<PartitionId, MzOffset>` of offets
///     of real committed data.
#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub struct OffsetAntichain {
    inner: HashMap<PartitionId, MzOffset>,
}

impl PartialEq<HashMap<PartitionId, MzOffset>> for OffsetAntichain {
    fn eq(&self, other: &HashMap<PartitionId, MzOffset>) -> bool {
        other == &self.inner
    }
}

impl OffsetAntichain {
    /// Initialize an Antichain where all partitions have made no progress.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Initialize an Antichain where all partitions have made no progress,
    /// but with `cap` capacity in the underlying data structure.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(cap),
        }
    }

    // Data apis

    // TODO(aljoscha): These "data" APIs might be more confusing than they are
    // worth.

    /// Produce offsets for all partitions in this `OffsetAntichain` that
    /// were at one point given by `insert_data_up_to`.
    ///
    /// If the partition is yet to make any progress, it may be filtered out.
    ///
    /// Invariant: After initialization, only `insert_data_up_to`
    /// (not `insert` and friends) may be used with this `OffsetAntichain`
    /// for this function to produce meaningful values, unless you are very
    /// careful.
    // TODO(guswynn): better document how source uppers flow through the
    // source reader pipeline.
    pub fn as_data_offsets(&self) -> HashMap<PartitionId, MzOffset> {
        self.inner
            .iter()
            .filter_map(|(pid, offset)| {
                offset
                    .checked_sub(MzOffset::from(1))
                    .map(|offset| (pid.clone(), offset))
            })
            .collect()
    }

    // Frontier apis

    /// Insert a new `MzOffset` frontier value for the `pid`, returning
    /// the old one if it wasn't there.
    pub fn insert(&mut self, pid: PartitionId, m: MzOffset) -> Option<MzOffset> {
        self.inner.insert(pid, m)
    }

    /// Insert a new `MzOffset` frontier value for `pid` if it is larger than
    /// the previously stored value.
    pub fn maybe_insert(&mut self, pid: PartitionId, offset: MzOffset) {
        self.inner
            .entry(pid)
            .and_modify(|prev| *prev = std::cmp::max(*prev, offset))
            .or_insert(offset);
    }

    /// The same as `insert`, but for many values.
    pub fn extend<T: IntoIterator<Item = (PartitionId, MzOffset)>>(&mut self, iter: T) {
        self.inner.extend(iter)
    }
    /// Advance the frontier for `PartitionId` by `diff`
    /// Initializes the offset for `pid` if it doesn't exist.
    pub fn advance(&mut self, pid: PartitionId, diff: MzOffset) {
        *self.inner.entry(pid).or_default() += diff;
    }

    /// Returns `true` iff this [`OffsetAntichain`] is `<=` `other`.
    /// This requires that for all partitions tracked in `other`,
    /// the offset in `self` is <= or absent.
    pub fn less_equal(&self, other: &OffsetAntichain) -> bool {
        for (pid, offset) in other.iter() {
            let self_offset = self.inner.get(pid);
            if let Some(self_offset) = self_offset {
                if self_offset > offset {
                    return false;
                }
            }
        }
        true
    }

    /// Creates a new [`OffsetAntichain`] that starts out as a copy of `self`
    /// but where each offset is upper bounded by the corresponding offset from
    /// `other`, if there is one.
    ///
    /// NOTE: This is not an equivalent of `meet`, as known from timely
    /// `Antichain`. This operation is asymmetric: we want partitions in the
    /// result only if they exist in `self`, we don't want partitions in the
    /// result that only exist in `other. If we did the latter, this could mean
    /// that we advance a frontier further than the original `self` would have.
    pub fn bounded(&self, other: &OffsetAntichain) -> OffsetAntichain {
        let mut result = self.clone();

        for (pid, offset) in other.iter() {
            result
                .inner
                .entry(pid.clone())
                .and_modify(|prev| *prev = std::cmp::min(*prev, *offset));
        }

        result
    }

    // Read Api's

    /// Attempt to the the `MzOffset` value for `pid`'s frontier
    pub fn get(&self, pid: &PartitionId) -> Option<&MzOffset> {
        self.inner.get(pid)
    }

    /// List the contained partitions.
    pub fn partitions(&self) -> impl Iterator<Item = &PartitionId> {
        self.inner.keys()
    }

    /// Iterate over the entire frontier.
    pub fn iter(&self) -> impl Iterator<Item = (&PartitionId, &MzOffset)> {
        self.inner.iter()
    }

    /// Iterate over the entire frontier.
    pub fn into_iter(self) -> impl Iterator<Item = (PartitionId, MzOffset)> {
        self.inner.into_iter()
    }

    /// Convert the frontier into a vector. Useful for certain
    /// old apis in the storage crate.
    pub fn as_vec(&self) -> Vec<(PartitionId, Option<MzOffset>)> {
        let mut vec = Vec::with_capacity(self.inner.len());
        for (pid, offset) in self.inner.iter() {
            vec.push((pid.clone(), Some(offset.clone())));
        }
        vec
    }

    // Mutation Api's

    /// Scope this `OffsetAntichain` down to only partitions that pass
    /// this filter callback.
    pub fn filter_by_partition<F>(&mut self, mut filter: F)
    where
        F: FnMut(&PartitionId) -> bool,
    {
        self.inner.retain(|pid, _| filter(pid))
    }

    /// Build an `OffsetAntichain` from a direct iterator. Useful for tests.
    #[cfg(test)]
    pub fn from_iter<T: IntoIterator<Item = (PartitionId, MzOffset)>>(iter: T) -> Self {
        Self {
            inner: HashMap::from_iter(iter),
        }
    }
}

/// Implementation that converts from an Antichain to an OffsetAntichain. This translation code is
/// here for compatibility and will be removed once the pipeline is switched to native timestamps
/// throughout.
///
/// Native antichains can express a superset of frontiers that the OffsetAntichain can, therefore
/// the conversion here can fail. While it's non-standard to panic in From implementations this is
/// fine here because it's just an interim step that will be removed shortly and all uses would
/// have to panic on error anyway.
impl From<Antichain<Partitioned<PartitionId, MzOffset>>> for OffsetAntichain {
    fn from(frontier: Antichain<Partitioned<PartitionId, MzOffset>>) -> Self {
        use mz_timely_util::order::{Interval, RangeBound};

        // Extract the timestamps of this antichain and order them by partition
        let mut elements = frontier.iter().collect::<Vec<_>>();
        elements.sort_unstable_by(|a, b| match (a.interval(), b.interval()) {
            (Interval::Range(a_lower, _), Interval::Range(b_lower, _)) => a_lower.cmp(b_lower),
            (Interval::Point(pid), Interval::Range(lower, _)) => RangeBound::Elem(pid.clone())
                .cmp(lower)
                .then(Ordering::Less),
            (Interval::Range(lower, _), Interval::Point(pid)) => lower
                .cmp(&RangeBound::Elem(pid.clone()))
                .then(Ordering::Greater),
            (Interval::Point(a_pid), Interval::Point(b_pid)) => a_pid.cmp(b_pid),
        });

        // We now sweep over the partition space and construct an offset antichain, asserting that
        // there are no gaps in the original Antichain which are unrepresentable in OffsetAntichain
        let mut prev_upper = &RangeBound::Bottom;
        let mut antichain = OffsetAntichain::new();
        for element in elements.iter() {
            match element.interval() {
                Interval::Range(lower, upper) => {
                    assert_eq!(prev_upper, lower, "invalid frontier: {frontier:?}");
                    assert_eq!(element.timestamp().offset, 0, "Non-zero range element");
                    prev_upper = upper;
                }
                Interval::Point(pid) => {
                    let lower = RangeBound::Elem(pid.clone());
                    assert_eq!(prev_upper, &lower, "invalid frontier: {frontier:?}");
                    antichain.insert(pid.clone(), *element.timestamp());
                }
            }
        }
        assert_eq!(
            prev_upper,
            &RangeBound::Top,
            "invalid frontier: {frontier:?}"
        );
        antichain
    }
}

/// Implementation that converts from an OffsetAntichain to an Antichain. This translation code is
/// here for compatibility and will be removed once the pipeline is switched to native timestamps
/// throughout.
impl From<OffsetAntichain> for Antichain<Partitioned<PartitionId, MzOffset>> {
    fn from(frontier: OffsetAntichain) -> Self {
        // Extract the timestamps of this frontier and order them by partition
        let mut elements = frontier
            .inner
            .into_iter()
            .filter(|(_pid, offset)| offset.offset != 0)
            .collect::<Vec<_>>();
        elements.sort_unstable();

        // We now sweep over the partition space and construct an antichain, creating range
        // elements for the gaps between the non-zero partitions.
        let mut prev_pid = None;
        let mut antichain = Antichain::new();
        for (pid, offset) in elements {
            antichain.insert(Partitioned::with_range(
                prev_pid,
                Some(pid.clone()),
                MzOffset::from(0),
            ));
            prev_pid = Some(pid.clone());
            antichain.insert(Partitioned::with_partition(pid, offset));
        }
        antichain.insert(Partitioned::with_range(prev_pid, None, MzOffset::from(0)));
        antichain
    }
}

/// A wrapper around [`MutableAntichain`] that allows adding (inserting all
/// contents with a `+1`) and subtracting (inserting all contents with a `-1`)
/// of whole [`OffsetAntichains`](OffsetAntichain).
///
/// The frontier of this mutable antichain can be revealed in the form of an
/// [`OffsetAntichain`].
#[derive(Debug)]
pub struct MutableOffsetAntichain {
    inner: MutableAntichain<PartitionOffset>,
}

impl MutableOffsetAntichain {
    /// Creates a new, empty [`MutableOffsetAntichain`].
    pub fn new() -> Self {
        Self {
            inner: MutableAntichain::new(),
        }
    }

    /// Inserts all partition/offset pairs contained in the given
    /// [`OffsetAntichain`] into this [`MutableOffsetAntichain`], with a `diff`
    /// of `+1`.
    ///
    /// In laymans terms, this adds the contained partition/offset pairs.
    pub fn add(&mut self, offsets: &OffsetAntichain) {
        let iter = offsets
            .iter()
            .map(|(pid, offset)| (PartitionOffset::new(pid.clone(), *offset), 1));
        self.inner.update_iter(iter);
    }

    /// Inserts all partition/offset pairs contained in the given
    /// [`OffsetAntichain`] into this [`MutableOffsetAntichain`], with a `diff`
    /// of `-1`.
    ///
    /// In laymans terms, this subtracts the contained partition/offset pairs.
    pub fn subtract(&mut self, offsets: &OffsetAntichain) {
        let iter = offsets
            .iter()
            .map(|(pid, offset)| (PartitionOffset::new(pid.clone(), *offset), -1));
        self.inner.update_iter(iter);
    }

    pub fn update_iter<I>(&mut self, updates: I)
    where
        I: IntoIterator<Item = ((PartitionId, MzOffset), Diff)>,
    {
        self.inner.update_iter(
            updates
                .into_iter()
                .map(|((pid, offset), diff)| (PartitionOffset::new(pid, offset), diff)),
        );
    }

    /// Reveals the minimal elements with positive count.
    ///
    /// In laymans terms, this returns an [`OffsetAntichain`] that contains all
    /// partitions with positive counts, and their respective minimal offset.
    ///
    // TODO(guswynn): remove this extra allocation.
    pub fn frontier(&self) -> OffsetAntichain {
        let mut result = OffsetAntichain::new();

        for PartitionOffset { partition, offset } in self.inner.frontier().iter() {
            result.insert(partition.clone(), *offset);
        }

        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionOffset {
    partition: PartitionId,
    offset: MzOffset,
}

impl PartitionOffset {
    fn new(partition: PartitionId, offset: MzOffset) -> Self {
        PartitionOffset { partition, offset }
    }
}

impl PartialOrder for PartitionOffset {
    fn less_equal(&self, other: &Self) -> bool {
        self.partial_cmp(other)
            .map(|ord| ord.is_le())
            .unwrap_or(false)
    }
}

// We manually implement `Ord` and `PartialOrd` to show that
// they are compatible with the `PartialOrder` implementation.

impl PartialOrd for PartitionOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Only offsets for the same partition are comparable!
        if self.partition != other.partition {
            None
        } else {
            Some(self.offset.cmp(&other.offset))
        }
    }
}

// NOTE: Ord is only required as an implementation detail of `MutableAntichain`,
// but it feels iffy.
impl Ord for PartitionOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.partition.cmp(&other.partition) {
            Ordering::Equal => self.offset.cmp(&other.offset),
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutable_antichain_basic_usage() {
        let mut mutable_antichain = MutableOffsetAntichain::new();

        let offset_antichain_a = OffsetAntichain::from_iter([
            (pid(0), 5.into()),
            (pid(1), 10.into()),
            (pid(3), 11.into()),
        ]);

        let offset_antichain_b = OffsetAntichain::from_iter([
            (pid(0), 10.into()),
            (pid(1), 5.into()),
            (pid(4), 11.into()),
        ]);

        mutable_antichain.add(&offset_antichain_a);

        let frontier = mutable_antichain.frontier();
        assert_eq!(frontier, offset_antichain_a);

        // Adding the same `OffsetAntichain` again doesn't affect the overall
        // frontier.
        mutable_antichain.add(&offset_antichain_a);
        let frontier = mutable_antichain.frontier();
        assert_eq!(frontier, offset_antichain_a);

        // Adding a different `OffsetAntichain` will make the overall frontier
        // go to the "minimum per partition".
        mutable_antichain.add(&offset_antichain_b);

        let expected_frontier = OffsetAntichain::from_iter([
            (pid(0), 5.into()),
            (pid(1), 5.into()),
            (pid(3), 11.into()),
            (pid(4), 11.into()),
        ]);

        let frontier = mutable_antichain.frontier();
        assert_eq!(frontier, expected_frontier);

        // Subtracting "a" once does not change the frontier.
        mutable_antichain.subtract(&offset_antichain_a);

        let frontier = mutable_antichain.frontier();
        assert_eq!(frontier, expected_frontier);

        // Completely removing any vestiges of "a" will make the frontier mirror
        // "b".
        mutable_antichain.subtract(&offset_antichain_a);

        let frontier = mutable_antichain.frontier();
        assert_eq!(frontier, offset_antichain_b);

        // Removing everything will render the frontier empty.
        mutable_antichain.subtract(&offset_antichain_b);

        let frontier = mutable_antichain.frontier();
        assert_eq!(frontier, OffsetAntichain::new());
    }

    #[test]
    fn mutable_antichain_negative_counts() {
        let mut mutable_antichain = MutableOffsetAntichain::new();

        let offset_antichain_a = OffsetAntichain::from_iter([
            (pid(0), 5.into()),
            (pid(1), 10.into()),
            (pid(3), 11.into()),
        ]);

        // Negative counts should not show up in the frontier.
        mutable_antichain.subtract(&offset_antichain_a);

        let frontier = mutable_antichain.frontier();
        assert_eq!(frontier, OffsetAntichain::new());

        // These cancel out the negative counts. Frontier will still be empty.
        mutable_antichain.add(&offset_antichain_a);

        let frontier = mutable_antichain.frontier();
        assert_eq!(frontier, OffsetAntichain::new());
    }

    #[test]
    fn antichain_bound_basic_usage() {
        let offset_antichain_a = OffsetAntichain::from_iter([
            (pid(0), 5.into()),
            (pid(1), 10.into()),
            (pid(3), 11.into()),
        ]);

        let offset_antichain_b = OffsetAntichain::from_iter([
            (pid(0), 10.into()),
            (pid(1), 5.into()),
            (pid(4), 11.into()),
        ]);

        let bounded = offset_antichain_a.bounded(&offset_antichain_b);

        let expected_bounded = OffsetAntichain::from_iter([
            (pid(0), 5.into()),
            (pid(1), 5.into()),
            (pid(3), 11.into()),
        ]);

        assert_eq!(bounded, expected_bounded);
    }

    /// Testing helper.
    fn pid(pid: i32) -> PartitionId {
        PartitionId::Kafka(pid)
    }
}
