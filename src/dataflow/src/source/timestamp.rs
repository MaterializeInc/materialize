// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and methods for managing timestamp assignment and invention in sources

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};

use dataflow_types::MzOffset;
use expr::{GlobalId, PartitionId};
use repr::Timestamp;

/// This struct holds per partition timestamp binding state.
pub struct PartitionTimestamps {
    bindings: Vec<(Timestamp, MzOffset)>,
}

impl PartitionTimestamps {
    fn new() -> Self {
        Self {
            bindings: Vec::new(),
        }
    }

    fn compact(&mut self, frontier: AntichainRef<Timestamp>) {
        if self.bindings.is_empty() {
            return;
        }

        // First, let's advance all times not in advance of the frontier to the frontier
        // to the frontier
        for (time, _) in self.bindings.iter_mut() {
            if !frontier.less_equal(time) {
                *time = *frontier.first().expect("known to exist");
            }
        }

        let mut new_bindings = Vec::with_capacity(self.bindings.len());
        // Now let's only keep the largest binding for each offset
        for i in 0..(self.bindings.len() - 1) {
            if self.bindings[i].0 != self.bindings[i + 1].0 {
                new_bindings.push(self.bindings[i]);
            }
        }

        // We always keep the last binding around.
        new_bindings.push(*self.bindings.last().expect("known to exist"));
        self.bindings = new_bindings;
    }

    fn add_binding(&mut self, timestamp: Timestamp, offset: MzOffset) {
        let (last_ts, last_offset) = self.bindings.last().unwrap_or(&(0, MzOffset { offset: 0 }));
        assert!(
            offset >= *last_offset,
            "offset should not go backwards, but {} < {}",
            offset,
            last_offset
        );
        assert!(
            timestamp > *last_ts,
            "timestamp should move forwards, but {} <= {}",
            timestamp,
            last_ts
        );
        self.bindings.push((timestamp, offset));
    }

    fn get_binding(&self, offset: MzOffset) -> Option<(Timestamp, MzOffset)> {
        // Rust's binary search is inconvenient so let's roll our own.
        // Maintain the invariants that the offset at lo (entries[lo].1) is always less
        // than the requested offset, and n is > 1. Check for violations of that before we
        // start the main loop.
        if self.bindings.is_empty() {
            return None;
        }

        if self.bindings[0].1 >= offset {
            return Some(self.bindings[0]);
        }

        let mut n = self.bindings.len();
        let mut lo = 0;

        while n > 1 {
            let half = n / 2;

            // Advance lo if a later element has an offset lower than the one requested.
            if self.bindings[lo + half].1 < offset {
                lo += half;
            }

            n -= half;
        }

        if lo + 1 < self.bindings.len() {
            return Some(self.bindings[lo + 1]);
        }

        None
    }

    fn upper(&self) -> Option<Timestamp> {
        self.bindings.last().map(|(time, _)| *time)
    }
}

/// This struct holds per-source timestamp state in a way that can be shared across
/// different source instances and allow different source instances to indicate
/// how far they have read up to.
///
/// This type is almost never meant to be used directly, and you probably want to
/// use `TimestampBindingRc` instead.
pub struct TimestampBindingBox {
    /// List of timestamp bindings per independent partition. This vector is sorted
    /// by timestamp and offset and each `(time, offset)` entry indicates that offsets <=
    /// `offset` should be assigned `time` as their timestamp. Consecutive entries form
    /// an interval of offsets.
    partitions: HashMap<PartitionId, PartitionTimestamps>,
    /// Indicates the lowest timestamp across all partitions that we retain bindings for.
    /// This frontier can be held back by other entities holding the shared
    /// `TimestampBindingRc`.
    compaction_frontier: MutableAntichain<Timestamp>,
}

impl TimestampBindingBox {
    fn new() -> Self {
        Self {
            partitions: HashMap::new(),
            compaction_frontier: MutableAntichain::new(),
        }
    }

    fn adjust_compaction_frontier(
        &mut self,
        remove: AntichainRef<Timestamp>,
        add: AntichainRef<Timestamp>,
    ) {
        self.compaction_frontier
            .update_iter(remove.iter().map(|t| (*t, -1)));
        self.compaction_frontier
            .update_iter(add.iter().map(|t| (*t, 1)));
    }

    fn compact(&mut self) {
        let frontier = self.compaction_frontier.frontier();

        // Don't compact up to the empty frontier as it would mean there were no
        // timestamp bindings available
        // TODO(rkhaitan): is there a more sensible approach here?
        if frontier.is_empty() {
            return;
        }

        for (_, partition) in self.partitions.iter_mut() {
            partition.compact(frontier);
        }
    }

    fn add_binding(&mut self, partition: PartitionId, timestamp: Timestamp, offset: MzOffset) {
        let partition = self
            .partitions
            .entry(partition)
            .or_insert_with(PartitionTimestamps::new);
        partition.add_binding(timestamp, offset);
    }

    fn get_binding(
        &self,
        partition: &PartitionId,
        offset: MzOffset,
    ) -> Option<(Timestamp, MzOffset)> {
        if !self.partitions.contains_key(partition) {
            return None;
        }

        let partition = self.partitions.get(partition).expect("known to exist");
        partition.get_binding(offset)
    }

    fn read_upper(&self, target: &mut Antichain<Timestamp>) {
        target.clear();

        for (_, partition) in self.partitions.iter() {
            if let Some(timestamp) = partition.upper() {
                target.insert(timestamp);
            }
        }
    }

    fn partitions(&self) -> Vec<PartitionId> {
        self.partitions
            .iter()
            .map(|(pid, _)| pid)
            .cloned()
            .collect()
    }
}

/// A wrapper that allows multiple source instances to share a `TimestampBindingBox`
/// and hold back its compaction.
pub struct TimestampBindingRc {
    wrapper: Rc<RefCell<TimestampBindingBox>>,
    compaction_frontier: Antichain<Timestamp>,
}

impl TimestampBindingRc {
    /// Create a new instance of `TimestampBindingRc`.
    pub fn new() -> Self {
        let wrapper = Rc::new(RefCell::new(TimestampBindingBox::new()));

        let ret = Self {
            wrapper: wrapper.clone(),
            compaction_frontier: wrapper.borrow().compaction_frontier.frontier().to_owned(),
        };

        ret
    }

    /// Set the compaction frontier to `new_frontier` and compact all timestamp bindings at
    /// timestamps less than the compaction frontier.
    ///
    /// Note that `new_frontier` must be in advance of the current compaction
    /// frontier. The source can be correctly replayed from any `as_of` in advance of
    /// the compaction frontier after this operation.
    pub fn set_compaction_frontier(&mut self, new_frontier: AntichainRef<Timestamp>) {
        self.wrapper
            .borrow_mut()
            .adjust_compaction_frontier(self.compaction_frontier.borrow(), new_frontier);
        self.compaction_frontier = new_frontier.to_owned();
        self.wrapper.borrow_mut().compact();
    }

    /// Add a new mapping from `(partition, offset) -> timestamp`.
    ///
    /// Note that the `timestamp` greater than the largest previously bound
    /// timestamp for that partition, and `offset` has to be greater than or equal to
    /// the largest previously bound offset for that partition.
    pub fn add_binding(&self, partition: PartitionId, timestamp: Timestamp, offset: MzOffset) {
        self.wrapper
            .borrow_mut()
            .add_binding(partition, timestamp, offset);
    }

    /// Get the timestamp assignment for `(partition, offset)` if it is known.
    ///
    /// This function returns the timestamp and the maximum offset for which it is
    /// valid.
    pub fn get_binding(
        &self,
        partition: &PartitionId,
        offset: MzOffset,
    ) -> Option<(Timestamp, MzOffset)> {
        self.wrapper.borrow().get_binding(partition, offset)
    }

    /// Returns the lower bound across every partition's most recent timestamp.
    ///
    /// All subsequent updates will either be at or in advance of this frontier.
    pub fn read_upper(&self, target: &mut Antichain<Timestamp>) {
        self.wrapper.borrow().read_upper(target)
    }

    /// Returns the list of partitions this source knows about.
    ///
    /// TODO(rkhaitan): this function feels like a hack, both in the API of having
    /// the source instances ask for the list of known partitions and in allocating
    /// a vector to answer that question.
    pub fn partitions(&self) -> Vec<PartitionId> {
        self.wrapper.borrow().partitions()
    }
}

impl Clone for TimestampBindingRc {
    fn clone(&self) -> Self {
        // Bump the reference count for the current frontier
        self.wrapper.borrow_mut().adjust_compaction_frontier(
            Antichain::new().borrow(),
            self.compaction_frontier.borrow(),
        );

        Self {
            wrapper: self.wrapper.clone(),
            compaction_frontier: self.compaction_frontier.clone(),
        }
    }
}

impl Drop for TimestampBindingRc {
    fn drop(&mut self) {
        // Decrement the reference count for the current frontier
        self.wrapper.borrow_mut().adjust_compaction_frontier(
            self.compaction_frontier.borrow(),
            Antichain::new().borrow(),
        );

        self.compaction_frontier = Antichain::new();
    }
}

/// A type wrapper for a timestamp update
pub enum TimestampDataUpdate {
    /// RT sources see the current set of partitions known to the source.
    RealTime(HashSet<PartitionId>),
    /// BYO sources see a list of (Timestamp, MzOffset) timestamp updates
    BringYourOwn(TimestampBindingRc),
}

/// Map of source ID to timestamp data updates (RT or BYO).
pub type TimestampDataUpdates = Rc<RefCell<HashMap<GlobalId, TimestampDataUpdate>>>;
