// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Drivers for upstream commit
use std::collections::HashMap;

use mz_expr::PartitionId;

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

    /// Advance the frontier represented by this `OffsetAntichain` for `pid`
    /// to a value that contains `offset`.
    pub fn insert_data_up_to(&mut self, pid: PartitionId, offset: MzOffset) {
        *self.inner.entry(pid).or_default() = offset + 1;
    }

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
    /// The same as `insert`, but for many values.
    pub fn extend<T: IntoIterator<Item = (PartitionId, MzOffset)>>(&mut self, iter: T) {
        self.inner.extend(iter)
    }
    /// Advance the frontier for `PartitionId` by `diff`
    /// Initializes the offset for `pid` if it doesn't exist.
    pub fn advance(&mut self, pid: PartitionId, diff: MzOffset) {
        *self.inner.entry(pid).or_default() += diff;
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
