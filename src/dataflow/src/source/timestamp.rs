// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and methods for managing timestamp assignment and invention in sources

//! External users will interact primarily with instances of a `TimestampBindingRc` object
//! which lets various source instances reading on the same worker coordinate about the
//! underlying `TimestampBindingBox` and give readers that are lagging behind the ability
//! to delay compaction.

//! Besides that, the only other bit of complexity in this code is the `TimestampProposer` object
//! which manages the collaborative invention of timestamps by several source instances all reading
//! from the same worker. The key idea is that since all source readers are assigned to the same
//! worker, only one of them will be reading at a given time, and that reader can either consult
//! the timestamp bindings generated by its peers if it is not the furthest ahead, or if it is
//! the furthest ahead, it can propose a new assingment of `(partition, offset) -> timestamp` that
//! its peers will respect.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;

use bytes::BufMut;
use mz_persist_types::Codec;
use prost::Message;
use timely::dataflow::operators::Capability;
use timely::order::PartialOrder;
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::{ChangeBatch, Timestamp as TimelyTimestamp};

use mz_dataflow_types::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_repr::Timestamp;

use crate::source::gen::source::{
    proto_source_timestamp, ProtoAssignedTimestamp, ProtoSourceTimestamp,
};

/// This struct holds state for proposed timestamps and
/// proposed bindings from offsets to timestamps.
#[derive(Debug)]
pub struct TimestampProposer {
    /// Working set of proposed offsets to assign to a new timestamp.
    bindings: HashMap<PartitionId, MzOffset>,
    /// Current timestamp we are assigning new data to.
    timestamp: Timestamp,
    /// Last time we updated the timestamp.
    last_update_time: Instant,
    /// Interval at which we are updating the timestamp.
    update_interval: u64,
    now: NowFn,
}

impl TimestampProposer {
    fn new(update_interval: u64, now: NowFn) -> Self {
        let timestamp = now();
        Self {
            bindings: HashMap::new(),
            timestamp,
            last_update_time: Instant::now(),
            update_interval,
            now,
        }
    }

    /// Propose that `(partition, offset)` be bound to whatever timestamp we are
    /// currently assigning data to.
    ///
    /// This proposal is ignored if there is already a proposed binding for this
    /// partition to an offset > 'offset'.
    fn propose_binding(&mut self, partition: PartitionId, offset: MzOffset) -> Timestamp {
        // Propose one past the current offset, as bindings store one past the
        // maximum offset bound to that time..
        let next_offset = offset + 1;

        // Only use the proposal if it further ahead than any existing proposals
        // for that partition.
        let current_proposal = self.bindings.entry(partition).or_insert(next_offset);
        if next_offset > *current_proposal {
            *current_proposal = next_offset;
        }
        self.timestamp
    }

    /// Attempt to mint the currently proposed timestamp bindings, and open up for
    /// proposals on a new timestamp.
    ///
    /// This function needs to be called periodically in order for RT sources to
    /// make progress.
    fn update_timestamp(&mut self) -> Option<(Timestamp, Vec<(PartitionId, MzOffset)>)> {
        if self.last_update_time.elapsed().as_millis() < self.update_interval.into() {
            return None;
        }

        // We need to determine the new timestamp
        let mut new_ts = (self.now)();
        new_ts += self.update_interval - (new_ts % self.update_interval);

        if self.timestamp < new_ts {
            // Now we need to fetch all of the existing bindings
            let bindings: Vec<_> = self.bindings.iter().map(|(p, o)| (p.clone(), *o)).collect();
            let old_timestamp = self.timestamp;

            self.timestamp = new_ts;
            self.last_update_time = Instant::now();
            Some((old_timestamp, bindings))
        } else {
            // We could not determine a suitable new timestamp, and so we
            // cannot finalize any current proposals.
            None
        }
    }

    /// Returns the current upper frontier (timestamp at which all future updates
    /// will occur).
    fn upper(&self) -> Timestamp {
        self.timestamp
    }
}

/// This struct holds per partition timestamp binding state, as an ordered list
/// of pairs (time, offset). Each pair indicates "all offsets < offset must be
/// bound to time".
///
/// Adjacent pairs indicate half-open intervals of offsets that are bound to
/// various timestamps. There can be duplicate offsets, which denote timestamps
/// that are closed but did not have any timestamps assigned to them.
///
/// As an example, the sequence of pairs (t1, o1), (t2, o2), (t3, o2) indicates
/// that:
/// - offsets in [0, o1) are bound to t1.
/// - offsets in [o1, o2) are bound to t2.
/// - offsets in [o2, inf) have not been assigned a timestamp yet.
/// - no offsets are bound to t3, and no offsets will be bound to t3.
#[derive(Debug)]
pub struct PartitionTimestamps {
    id: PartitionId,
    bindings: Vec<(Timestamp, MzOffset)>,
}

impl PartitionTimestamps {
    fn new(id: PartitionId) -> Self {
        Self {
            id,
            bindings: Vec::new(),
        }
    }

    /// Advance all timestamp bindings to the frontier, and then
    /// combine overlapping offset ranges bound to the same timestamp.
    fn compact(&mut self, frontier: AntichainRef<Timestamp>) {
        if self.bindings.is_empty() {
            return;
        }

        // First, let's advance all times not in advance of the frontier to the frontier
        for (time, _) in self.bindings.iter_mut() {
            if !frontier.less_equal(time) {
                *time = *frontier.first().expect("known to exist");
            }
        }

        let mut new_bindings = Vec::with_capacity(self.bindings.len());
        // Now let's only keep the largest binding for each timestamp.
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
        if let Some((last_ts, last_offset)) = self.bindings.last() {
            // TODO(rkhaitan): remove this error log and change the assertion
            // below to be strictly greater than once we fix 10742.
            if timestamp == *last_ts {
                log::error!(
                    "newly added timestamps should go forwards but {} == {}. Continuing",
                    timestamp,
                    last_ts
                );
            }
            assert!(
                offset >= *last_offset,
                "offset should not go backwards, but {} < {}",
                offset,
                last_offset
            );
            assert!(
                timestamp >= *last_ts,
                "timestamp should not go backwards, but {} < {}",
                timestamp,
                last_ts
            );
        }
        self.bindings.push((timestamp, offset));
    }

    /// Gets the timestamp binding for `offset`.
    ///
    /// The timestamp binding is the minimum binding_time such that:
    /// - (binding_time, binding_offset) exists in the list of bindings
    /// - binding_offset > `offset`
    ///
    /// Returns None if no such binding exists.
    fn get_binding(&self, offset: MzOffset) -> Option<Timestamp> {
        // Rust's binary search is inconvenient so let's roll our own.
        if self.bindings.is_empty() {
            return None;
        }

        let mut remaining = self.bindings.len();
        let mut lo = 0;
        if self.bindings[lo].1 > offset {
            return Some(self.bindings[lo].0);
        }

        // Invariants:
        // - The offset at lo is always <= requested offset.
        // - remaining > 1
        // - lo < bindings.len()
        while remaining > 1 {
            let half = remaining / 2;

            // Advance lo if a later element has an offset <= equal to the one requested.
            if self.bindings[lo + half].1 <= offset {
                lo += half;
            }

            remaining -= half;
        }

        // lo points to the max offset <= the requested offset, so lo + 1
        // points to the minimum offset > requested offset.
        if lo + 1 < self.bindings.len() {
            Some(self.bindings[lo + 1].0)
        } else {
            None
        }
    }

    fn get_bindings_in_range(
        &self,
        lower: AntichainRef<Timestamp>,
        upper: AntichainRef<Timestamp>,
        bindings: &mut Vec<(PartitionId, Timestamp, MzOffset)>,
    ) {
        for (time, offset) in self.bindings.iter() {
            if lower.less_equal(time) && !upper.less_equal(time) {
                bindings.push((self.id.clone(), *time, *offset));
            }
        }
    }
}

/// This struct holds per-source timestamp state in a way that can be shared across
/// different source instances and allow different source instances to indicate
/// how far they have read up to.
///
/// This type is almost never meant to be used directly, and you probably want to
/// use `TimestampBindingRc` instead.
#[derive(Debug)]
pub struct TimestampBindingBox {
    /// List of timestamp bindings per independent partition.
    partitions: HashMap<PartitionId, PartitionTimestamps>,
    /// Indicates the lowest timestamp across all partitions that we retain bindings for.
    /// This frontier can be held back by other entities holding the shared
    /// `TimestampBindingRc`.
    compaction_frontier: MutableAntichain<Timestamp>,
    /// Indicates the lowest timestamp across all partititions and across all workers that has
    /// been durably persisted.
    durability_frontier: Antichain<Timestamp>,
    /// Generates new timestamps for RT sources
    proposer: TimestampProposer,
    /// Source operators that should be activated on durability changes.
    pub activators: Vec<timely::scheduling::Activator>,
}

impl TimestampBindingBox {
    fn new(timestamp_update_interval: u64, now: NowFn) -> Self {
        Self {
            partitions: HashMap::new(),
            compaction_frontier: MutableAntichain::new_bottom(TimelyTimestamp::minimum()),
            durability_frontier: Antichain::from_elem(TimelyTimestamp::minimum()),
            proposer: TimestampProposer::new(timestamp_update_interval, now),
            activators: Vec::new(),
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

    fn set_durability_frontier(&mut self, new_frontier: AntichainRef<Timestamp>) {
        assert!(
            <_ as PartialOrder>::less_equal(&self.durability_frontier.borrow(), &new_frontier),
            "Durability frontier regression: {:?} to {:?}",
            self.durability_frontier.borrow(),
            new_frontier
        );
        self.durability_frontier = new_frontier.to_owned();
        for activator in self.activators.iter() {
            activator.activate();
        }
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

    fn add_partition(&mut self, partition: PartitionId) {
        // Update our internal state to also keep track of the new partition.
        self.partitions
            .entry(partition.clone())
            .or_insert_with(|| PartitionTimestamps::new(partition));
    }

    fn add_binding(&mut self, partition: PartitionId, timestamp: Timestamp, offset: MzOffset) {
        if !self.partitions.contains_key(&partition) {
            panic!("missing partition {:?} when adding binding", partition);
        }

        let partition = self.partitions.get_mut(&partition).expect("known to exist");
        partition.add_binding(timestamp, offset);
    }

    fn downgrade(&self, cap: &mut Capability<Timestamp>, cursors: &HashMap<PartitionId, MzOffset>) {
        let mut ts = self.upper();
        for (pid, timestamps) in self.partitions.iter() {
            let offset = match cursors.get(pid).cloned() {
                Some(offset) => MzOffset {
                    // The cursors store the offset of the last read message while we want to now
                    // the potential binding for future messages, so we add 1.
                    offset: offset.offset + 1,
                },
                None => MzOffset { offset: 1 },
            };
            if let Some(partition_ts) = timestamps.get_binding(offset) {
                ts = std::cmp::min(ts, partition_ts);
            }
        }
        match cap.try_downgrade(&ts) {
            Ok(_) => (),
            Err(e) => {
                panic!(
                    "error trying to downgrade {:?} to {}; cursors: {:?}, bindings: {:?}, upper: {}, compaction: {:?}, error: {}",
                    cap,
                    ts,
                    cursors,
                    self.partitions,
                    self.upper(),
                    self.compaction_frontier,
                    e
                );
            }
        }
    }

    fn get_or_propose_binding(&mut self, partition: &PartitionId, offset: MzOffset) -> Timestamp {
        if !self.partitions.contains_key(partition) {
            self.add_partition(partition.clone());
        }
        let partition_timestamps = self.partitions.get(partition).expect("known to exist");
        if let Some(time) = partition_timestamps.get_binding(offset) {
            time
        } else {
            self.proposer.propose_binding(partition.clone(), offset)
        }
    }

    fn get_bindings_in_range(
        &self,
        lower: AntichainRef<Timestamp>,
        upper: AntichainRef<Timestamp>,
    ) -> Vec<(PartitionId, Timestamp, MzOffset)> {
        let mut ret = Vec::new();

        for (_, partition) in self.partitions.iter() {
            partition.get_bindings_in_range(lower, upper, &mut ret);
        }

        ret
    }

    fn upper(&self) -> Timestamp {
        self.proposer.upper()
    }

    fn read_upper(&self, target: &mut Antichain<Timestamp>) {
        target.clear();
        target.insert(self.proposer.upper());

        use timely::progress::Timestamp;
        if target.elements().is_empty() {
            target.insert(Timestamp::minimum());
        }
    }

    fn update_timestamp(&mut self) {
        let result = self.proposer.update_timestamp();
        if let Some((time, bindings)) = result {
            for (partition, offset) in bindings {
                self.add_binding(partition, time, offset);
            }
        }
    }
}

/// A wrapper that allows multiple source instances to share a `TimestampBindingBox`
/// and hold back its compaction.
#[derive(Debug)]
pub struct TimestampBindingRc {
    /// The wrapped shared state.
    pub wrapper: Rc<RefCell<TimestampBindingBox>>,
    compaction_frontier: Antichain<Timestamp>,
}

impl TimestampBindingRc {
    /// Create a new instance of `TimestampBindingRc`.
    pub fn new(timestamp_update_interval: u64, now: NowFn) -> Self {
        let wrapper = Rc::new(RefCell::new(TimestampBindingBox::new(
            timestamp_update_interval,
            now,
        )));

        let ret = Self {
            wrapper: Rc::clone(&wrapper),
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
        assert!(
            self.compaction_frontier.borrow().is_empty()
                || <_ as PartialOrder>::less_equal(
                    &self.compaction_frontier.borrow(),
                    &new_frontier
                )
        );
        self.wrapper
            .borrow_mut()
            .adjust_compaction_frontier(self.compaction_frontier.borrow(), new_frontier);
        self.compaction_frontier = new_frontier.to_owned();
        self.wrapper.borrow_mut().compact();
    }

    /// Sets the durability frontier, aka, the frontier before which all updates can be
    /// replayed across restarts.
    pub fn set_durability_frontier(&self, new_frontier: AntichainRef<Timestamp>) {
        self.wrapper
            .borrow_mut()
            .set_durability_frontier(new_frontier);
    }

    /// Add a new mapping from `(partition, offset) -> timestamp`.
    ///
    /// Note that the `timestamp` has to be greater than the largest previously bound
    /// timestamp for that partition, and `offset` has to be greater than or equal to
    /// the largest previously bound offset for that partition. If `proposed` is true,
    /// the binding is treated as tentative and may be overwritten by other, overlapping
    /// bindings
    pub fn add_binding(&self, partition: PartitionId, timestamp: Timestamp, offset: MzOffset) {
        self.wrapper
            .borrow_mut()
            .add_binding(partition, timestamp, offset);
    }

    /// Tell timestamping machinery to look out for `partition`
    pub fn add_partition(&self, partition: PartitionId) {
        self.wrapper.borrow_mut().add_partition(partition);
    }

    /// Get the timestamp assignment for `(partition, offset)` if it is known.
    ///
    /// This function returns the timestamp and the maximum offset for which it is
    /// valid.
    pub fn get_or_propose_binding(&self, partition: &PartitionId, offset: MzOffset) -> Timestamp {
        self.wrapper
            .borrow_mut()
            .get_or_propose_binding(partition, offset)
    }

    /// Get the timestamp that all messages beyond the minted bindings will be assigned to. This is
    /// equal to the proposer's current timestamp
    pub fn upper(&self) -> Timestamp {
        self.wrapper.borrow().upper()
    }

    /// Returns the frontier of timestamps that have not been bound to any
    /// incoming data, or in other words, all data has been assigned timestamps
    /// less than some element in the returned frontier.
    ///
    /// All subsequent updates will either be at or in advance of this frontier.
    pub fn read_upper(&self, target: &mut Antichain<Timestamp>) {
        self.wrapper.borrow().read_upper(target)
    }

    /// Attempt to downgrade the given capability by consulting the currently known bindings and
    /// the per partition cursors of the caller.
    pub fn downgrade(
        &self,
        cap: &mut Capability<Timestamp>,
        cursors: &HashMap<PartitionId, MzOffset>,
    ) {
        self.wrapper.borrow().downgrade(cap, cursors)
    }

    /// Instructs RT sources to try and move forward to the next timestamp if
    /// possible
    pub fn update_timestamp(&self) {
        self.wrapper.borrow_mut().update_timestamp()
    }

    /// Return all timestamp bindings at or in advance of lower and not at or in advance of upper
    pub fn get_bindings_in_range(
        &self,
        lower: AntichainRef<Timestamp>,
        upper: AntichainRef<Timestamp>,
    ) -> Vec<(PartitionId, Timestamp, MzOffset)> {
        self.wrapper.borrow().get_bindings_in_range(lower, upper)
    }

    /// Returns the current durability frontier
    pub fn durability_frontier(&self) -> Antichain<Timestamp> {
        self.wrapper.borrow().durability_frontier.clone()
    }
}

impl Clone for TimestampBindingRc {
    fn clone(&self) -> Self {
        // Bump the reference count for the current shared frontier
        let frontier = self
            .wrapper
            .borrow()
            .compaction_frontier
            .frontier()
            .to_owned();
        self.wrapper
            .borrow_mut()
            .adjust_compaction_frontier(Antichain::new().borrow(), frontier.borrow());
        self.wrapper.borrow_mut().compact();

        Self {
            wrapper: Rc::clone(&self.wrapper),
            compaction_frontier: frontier,
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
        self.wrapper.borrow_mut().compact();

        self.compaction_frontier = Antichain::new();
    }
}

/// Source-agnostic timestamp for [`SourceMessages`](crate::source::SourceMessage). Admittedly,
/// this is quite Kafka-centric.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceTimestamp {
    /// Partition from which this message originates
    pub partition: PartitionId,
    /// Materialize offset of the message (1-indexed)
    pub offset: MzOffset,
}

// TODO: See comment on Ord below.
impl PartialOrd for SourceTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let result = match (&self.partition, &other.partition) {
            (PartitionId::Kafka(a), PartitionId::Kafka(b)) if a == b => {
                self.offset.offset.cmp(&other.offset.offset)
            }
            (PartitionId::Kafka(a), PartitionId::Kafka(b)) => a.cmp(b),
            (PartitionId::None, PartitionId::None) => self.offset.offset.cmp(&other.offset.offset),
            // We're not using a wildcard pattern here, to make sure this fails when someone adds
            // new types of partition ID.
            (PartitionId::None, PartitionId::Kafka(_)) => {
                unreachable!("PartitionId types must match")
            }
            (PartitionId::Kafka(_), PartitionId::None) => {
                unreachable!("PartitionId types must match")
            }
        };
        Some(result)
    }
}

// TODO: We have `Ord` only because `ChangeBatch` requires `Ord`. Maybe there's a better
// alternative.  We use ChangeBatch to maintain a view of the current timestamp bindings in
// `TimestampBindingUpdater`.
impl Ord for SourceTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let result = match (&self.partition, &other.partition) {
            (PartitionId::Kafka(a), PartitionId::Kafka(b)) if a == b => {
                self.offset.offset.cmp(&other.offset.offset)
            }
            (PartitionId::Kafka(a), PartitionId::Kafka(b)) => a.cmp(b),
            (PartitionId::None, PartitionId::None) => self.offset.offset.cmp(&other.offset.offset),
            // We're not using a wildcard pattern here, to make sure this fails when someone adds
            // new types of partition ID.
            (PartitionId::None, PartitionId::Kafka(_)) => {
                unreachable!("PartitionId types must match")
            }
            (PartitionId::Kafka(_), PartitionId::None) => {
                unreachable!("PartitionId types must match")
            }
        };
        result
    }
}

/// Timestamp that was assigned to a source message.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Default)]
pub struct AssignedTimestamp(pub(crate) u64);

impl From<&SourceTimestamp> for ProtoSourceTimestamp {
    fn from(x: &SourceTimestamp) -> Self {
        ProtoSourceTimestamp {
            partition_id: Some(match &x.partition {
                PartitionId::Kafka(x) => proto_source_timestamp::PartitionId::Kafka(*x),
                PartitionId::None => proto_source_timestamp::PartitionId::None(()),
            }),
            mz_offset: x.offset.offset,
        }
    }
}

impl TryFrom<ProtoSourceTimestamp> for SourceTimestamp {
    type Error = String;

    fn try_from(x: ProtoSourceTimestamp) -> Result<Self, Self::Error> {
        let partition = match x.partition_id {
            Some(proto_source_timestamp::PartitionId::Kafka(x)) => PartitionId::Kafka(x),
            Some(proto_source_timestamp::PartitionId::None(_)) => PartitionId::None,
            None => return Err("unknown partition_id".into()),
        };
        Ok(SourceTimestamp {
            partition,
            offset: MzOffset {
                offset: x.mz_offset,
            },
        })
    }
}

impl Codec for SourceTimestamp {
    fn codec_name() -> String {
        "protobuf[SourceTimestamp]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        ProtoSourceTimestamp::from(self)
            .encode(buf)
            .expect("provided buffer had sufficient capacity")
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        ProtoSourceTimestamp::decode(buf)
            .map_err(|err| err.to_string())?
            .try_into()
    }
}

impl From<&AssignedTimestamp> for ProtoAssignedTimestamp {
    fn from(x: &AssignedTimestamp) -> Self {
        ProtoAssignedTimestamp { ts: x.0 }
    }
}

impl TryFrom<ProtoAssignedTimestamp> for AssignedTimestamp {
    type Error = String;

    fn try_from(x: ProtoAssignedTimestamp) -> Result<Self, Self::Error> {
        Ok(AssignedTimestamp(x.ts))
    }
}

impl Codec for AssignedTimestamp {
    fn codec_name() -> String {
        "protobuf[AssignedTimestamp]".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        ProtoAssignedTimestamp::from(self)
            .encode(buf)
            .expect("provided buffer had sufficient capacity")
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        ProtoAssignedTimestamp::decode(buf)
            .map_err(|err| err.to_string())?
            .try_into()
    }
}

/// Helper that can track offset cursors and emit differential updates that can be used to
/// reconstruct the offsets as they were at a given time.
///
/// This can be used to tee off a "stream" of differential updates that can be used to maintain a
/// copy of the current state of the offsets. For example, to persist them.
pub struct OffsetsUpdater {
    /// Consolidated view of the changes that we have emitted up to the latest invocation of
    /// `update`.
    current_offsets: ChangeBatch<SourceTimestamp>,
}

impl OffsetsUpdater {
    /// Creates a new [`OffsetsUpdater`]. We need the `initial_offsets` to bootstrap the internal
    /// view with the current state of the offsets that the outside consumer of the updates has.
    pub fn new<'a>(initial_offsets: impl Iterator<Item = (&'a PartitionId, &'a MzOffset)>) -> Self {
        let mut current_offsets = ChangeBatch::new();

        // NOTE: The cursor points to the _next_ message while we need to record the
        // last read message.
        let initial_offsets = initial_offsets.map(|(partition, offset)| {
            (
                SourceTimestamp {
                    partition: partition.clone(),
                    offset: *offset,
                },
                1,
            )
        });

        current_offsets.extend(initial_offsets);

        Self { current_offsets }
    }

    /// Brings the internal view of the offsets up to date with the given offsets and returns any
    /// changes as differential updates.
    pub fn update<'a>(
        &mut self,
        offsets: impl Iterator<Item = (&'a PartitionId, &'a MzOffset)>,
    ) -> impl Iterator<Item = (SourceTimestamp, i64)> {
        // We either have a binding or we don't. There can never be other multiplicities.
        mz_ore::soft_assert!(self
            .current_offsets
            .iter()
            .all(|(_binding, diff)| *diff == 1 || *diff == 0));

        // Determine what changes we have to apply (both to the output stream and our internal
        // view) to bring us in sync with the current state of offsets as presented.
        //
        // We do this by first inverting all of the updates that we had previously and then
        // applying the new state to that. Updates that are in the previous state and the new state
        // will cancel out, while updates that are no longer in the current state will remain as
        // `-1`s and new updates will remain as `1`s. If there are no changes since the last
        // invocation, the negated changes and the newly presented updates will cancel out and we
        // don't emit anything.
        let inverted_current_offsets = self
            .current_offsets
            .iter()
            .cloned()
            .map(|(offset, diff)| (offset, -diff));
        let mut offsets_change = ChangeBatch::new();
        offsets_change.extend(inverted_current_offsets);

        let offsets = offsets.map(|(partition, offset)| {
            (
                SourceTimestamp {
                    partition: partition.clone(),
                    offset: *offset,
                },
                1,
            )
        });

        offsets_change.extend(offsets);

        self.current_offsets.extend(offsets_change.iter().cloned());

        // We either have a binding or we don't. There can never be other multiplicities.
        mz_ore::soft_assert!(self
            .current_offsets
            .iter()
            .all(|(_offsets, diff)| *diff == 1 || *diff == 0));

        offsets_change.into_inner().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use mz_dataflow_types::sources::MzOffset;
    use mz_expr::PartitionId;
    use mz_persist_types::Codec;

    use super::*;

    #[test]
    fn source_timestamp_roundtrip() -> Result<(), String> {
        let partition = PartitionId::Kafka(42);
        let offset = MzOffset { offset: 17 };
        let original = SourceTimestamp { partition, offset };
        let mut encoded = Vec::new();
        original.encode(&mut encoded);
        let decoded = SourceTimestamp::decode(&encoded)?;

        assert_eq!(decoded, original);

        Ok(())
    }

    #[test]
    fn assigned_timestamp_roundtrip() -> Result<(), String> {
        let original = AssignedTimestamp(3);
        let mut encoded = Vec::new();
        original.encode(&mut encoded);
        let decoded = AssignedTimestamp::decode(&encoded)?;

        assert_eq!(decoded, original);

        Ok(())
    }

    #[test]
    fn offsets_updater_simple_updates() {
        let mut offsets = HashMap::new();
        let mut offsets_updater = OffsetsUpdater::new(offsets.iter());

        offsets.insert(PartitionId::Kafka(0), MzOffset { offset: 0 });

        let actual_updates = offsets_updater.update(offsets.iter()).collect::<Vec<_>>();
        let expected_updates = vec![(
            SourceTimestamp {
                partition: PartitionId::Kafka(0),
                offset: MzOffset { offset: 0 },
            },
            1,
        )];
        assert_eq!(actual_updates, expected_updates);

        offsets.insert(PartitionId::Kafka(0), MzOffset { offset: 5 });

        let actual_updates = offsets_updater.update(offsets.iter()).collect::<Vec<_>>();
        let expected_updates = vec![
            (
                SourceTimestamp {
                    partition: PartitionId::Kafka(0),
                    offset: MzOffset { offset: 0 },
                },
                -1,
            ),
            (
                SourceTimestamp {
                    partition: PartitionId::Kafka(0),
                    offset: MzOffset { offset: 5 },
                },
                1,
            ),
        ];
        assert_eq!(actual_updates, expected_updates);
    }

    // Verify that we don't emit new updates when repeatedly calling `update()` with unchanged
    // offsets.
    #[test]
    fn offsets_updater_repeated_update() {
        let mut offsets = HashMap::new();
        let mut offsets_updater = OffsetsUpdater::new(offsets.iter());

        offsets.insert(PartitionId::Kafka(0), MzOffset { offset: 4 });

        let actual_updates = offsets_updater.update(offsets.iter()).collect::<Vec<_>>();
        let expected_updates = vec![(
            SourceTimestamp {
                partition: PartitionId::Kafka(0),
                offset: MzOffset { offset: 4 },
            },
            1,
        )];
        assert_eq!(actual_updates, expected_updates);

        let actual_updates = offsets_updater.update(offsets.iter()).collect::<Vec<_>>();
        assert_eq!(actual_updates, vec![]);
    }

    #[test]
    fn partition_timestamps() {
        let mut pt = PartitionTimestamps::new(PartitionId::Kafka(1));
        let bindings = vec![
            (1, MzOffset { offset: 2 }),
            (2, MzOffset { offset: 5 }),
            (3, MzOffset { offset: 5 }),
            (5, MzOffset { offset: 6 }),
            (6, MzOffset { offset: 6 }),
        ];

        for (time, offset) in bindings.iter() {
            pt.add_binding(*time, offset.clone());
        }

        let test_cases = vec![
            ((0, 2), Some(1)),
            ((2, 5), Some(2)),
            ((5, 6), Some(5)),
            ((6, 10), None),
        ];

        for ((test_start, test_end), expected_binding) in test_cases {
            for offset in test_start..test_end {
                let mz_offset = MzOffset { offset };
                let binding = pt.get_binding(mz_offset);
                assert_eq!(binding, expected_binding);
            }
        }
    }
}
