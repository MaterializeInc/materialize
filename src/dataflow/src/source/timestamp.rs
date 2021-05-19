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
use std::mem::discriminant;
use std::rc::Rc;

use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};

use dataflow_types::{DebeziumTimestampBinding, DebeziumTransactionEntry};
use expr::{GlobalId, PartitionId};
use repr::Timestamp;

struct DebeziumTransactionCompletions {
    binding: DebeziumTimestampBinding,
    completions: HashMap<i64, bool>,
}
/// This struct holds per partition timestamp binding state.
pub struct PartitionTimestamps {
    bindings: Vec<DebeziumTransactionCompletions>,
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
        for DebeziumTransactionCompletions { binding, .. } in self.bindings.iter_mut() {
            if !frontier.less_equal(&binding.timestamp) {
                binding.timestamp = *frontier.first().expect("known to exist");
                // debug_assert!(binding.is_complete());
            }
        }

        // TODO (cirego): Actually remove entries that have been completed
        // Entries are considered completed when every `<_, bool>` entry in the completions struct is set to True
        // Starting from the earliest entry, remove entries until we encounter the first entry that still has
        // some completions with a value of `false`.
    }

    fn add_binding(&mut self, binding: DebeziumTimestampBinding) {
        if let Some(DebeziumTransactionCompletions { binding: last, .. }) = self.bindings.last() {
            assert!(
                discriminant(&binding.transaction) == discriminant(&last.transaction),
                "transactions should be of the same type, but {:?} is a different type from {:?}",
                binding.transaction,
                last.transaction
            );
            assert!(
                binding.transaction > last.transaction,
                "transaction should only ever increase, but {:?} <= {:?}",
                binding.transaction,
                last.transaction
            );
            assert!(
                binding.timestamp > last.timestamp,
                "timestamp should move forwards, but {} <= {}",
                binding.timestamp,
                last.timestamp
            );
        }
        let mut completions = HashMap::new();
        for i in 0..binding.transaction.event_count {
            completions.insert(i, false);
        }
        self.bindings.push(DebeziumTransactionCompletions {
            binding,
            completions,
        });
    }

    // TODO (cirego) This is the part with the bitmap and it returns None if no timestamps can be closed
    fn get_binding(&self, entry: DebeziumTransactionEntry) -> Option<DebeziumTimestampBinding> {
        // Rust's binary search is inconvenient so let's roll our own.
        // Maintain the invariants that the offset at lo (entries[lo].1) is always less
        // than the requested offset, and n is > 1. Check for violations of that before we
        // start the main loop.
        if self.bindings.is_empty() {
            return None;
        }

        // TODO (cirego): make this greater than
        if self.bindings[0].binding.transaction.transaction_id >= entry.transaction_id {
            return Some(self.bindings[0].binding);
        }

        let mut n = self.bindings.len();
        let mut lo = 0;

        while n > 1 {
            let half = n / 2;

            // Advance lo if a later element has an offset lower than the one requested.
            if self.bindings[lo + half].binding.transaction.transaction_id < entry.transaction_id {
                lo += half;
            }

            n -= half;
        }

        if lo + 1 < self.bindings.len() {
            return Some(self.bindings[lo + 1].binding);
        }

        None
    }

    fn upper(&self) -> Option<Timestamp> {
        self.bindings.last().map(|b| b.binding.timestamp)
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

    fn add_binding(&mut self, partition: PartitionId, binding: DebeziumTimestampBinding) {
        let partition = self
            .partitions
            .entry(partition)
            .or_insert_with(PartitionTimestamps::new);
        partition.add_binding(binding);
    }

    fn get_binding(
        &self,
        partition: &PartitionId,
        entry: DebeziumTransactionEntry,
    ) -> Option<DebeziumTimestampBinding> {
        if !self.partitions.contains_key(partition) {
            return None;
        }

        let partition = self.partitions.get(partition).expect("known to exist");
        partition.get_binding(entry)
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

    /// Add a new mapping from `(partition, transaction_id) -> timestamp`.
    ///
    /// Note that the `timestamp` greater than the largest previously bound
    /// timestamp for that partition, and `transaction_id` has to be greater than or equal to
    /// the largest previously bound offset for that partition.
    pub fn add_binding(&self, partition: PartitionId, binding: DebeziumTimestampBinding) {
        self.wrapper.borrow_mut().add_binding(partition, binding);
    }

    /// Get the timestamp assignment for `(partition, offset)` if it is known.
    ///
    /// This function returns the timestamp and the maximum offset for which it is
    /// valid.
    pub fn get_binding(
        &self,
        partition: &PartitionId,
        entry: DebeziumTransactionEntry,
    ) -> Option<DebeziumTimestampBinding> {
        self.wrapper.borrow().get_binding(partition, entry)
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
    /// BYO sources see a list of (Timestamp, DebeziumTransaction) timestamp updates
    Debezium(TimestampBindingRc),
}

/// Map of source ID to timestamp data updates (RT or BYO).
pub type TimestampDataUpdates = Rc<RefCell<HashMap<GlobalId, TimestampDataUpdate>>>;
