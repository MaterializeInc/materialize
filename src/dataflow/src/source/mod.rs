// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow sources.

use dataflow_types::{DataflowError, SourceErrorDetails};
use mz_avro::types::Value;
use persist::indexed::runtime::{StreamReadHandle, StreamWriteHandle};
use persist::indexed::Snapshot;
use persist::operators::stream::Persist;
use persist_types::Codec;
use repr::MessagePayload;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use timely::dataflow::operators::{Concat, Map, ToStream};
use timely::dataflow::{
    channels::pact::{Exchange, ParallelizationContract},
    operators::{Capability, Event},
};

use anyhow::anyhow;
use async_trait::async_trait;
use dataflow_types::{
    Consistency, ExternalSourceConnector, MzOffset, SourceDataEncoding, SourceError,
};
use expr::{PartitionId, SourceInstanceId};
use log::error;
use ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use ore::now::NowFn;
use prometheus::core::{AtomicI64, AtomicU64};

use repr::{Diff, Row, Timestamp};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::{operator, OutputHandle};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::scheduling::activate::{Activator, SyncActivator};
use timely::Data;
use tokio::sync::{mpsc, RwLock, RwLockReadGuard};

use self::metrics::SourceBaseMetrics;

use super::source::util::source;
use crate::logging::materialized::{Logger, MaterializedEvent};
use crate::operator::StreamExt;
use crate::source::timestamp::TimestampBindingRc;
use crate::source::timestamp::TimestampBindingUpdater;
use crate::source::timestamp::{AssignedTimestamp, SourceTimestamp};

mod file;
mod kafka;
mod kinesis;
pub(super) mod metrics;
mod postgres;
mod pubnub;
mod s3;
mod util;

pub mod timestamp;

use differential_dataflow::Hashable;
pub use file::read_file_task;
pub use file::FileReadStyle;
pub use file::FileSourceReader;
pub use kafka::KafkaSourceReader;
pub use kinesis::KinesisSourceReader;
pub use postgres::PostgresSourceReader;
pub use pubnub::PubNubSourceReader;
pub use s3::S3SourceReader;

// Interval after which the source operator will yield control.
static YIELD_INTERVAL_MS: u128 = 10;

/// Shared configuration information for all source types.
pub struct SourceConfig<'a, G> {
    /// The name to attach to the underlying timely operator.
    pub name: String,
    /// The name of the SQL object this source corresponds to
    pub sql_name: String,
    /// The name of the upstream resource this source corresponds to
    /// (For example, a Kafka topic)
    pub upstream_name: Option<String>,
    /// The ID of this instantiation of this source.
    pub id: SourceInstanceId,
    /// The timely scope in which to build the source.
    pub scope: &'a G,
    /// The ID of the worker on which this operator is executing
    pub worker_id: usize,
    /// The total count of workers
    pub worker_count: usize,
    // Timestamping fields.
    /// Data-timestamping updates: information about (timestamp, source offset)
    pub timestamp_histories: Option<TimestampBindingRc>,
    /// A source can use Real-Time consistency timestamping or BYO consistency information.
    pub consistency: Consistency,
    /// Source Type
    /// Timestamp Frequency: frequency at which timestamps should be closed (and capabilities
    /// downgraded)
    pub timestamp_frequency: Duration,
    /// Whether this worker has been chosen to actually receive data.
    pub active: bool,
    /// Data encoding
    pub encoding: SourceDataEncoding,
    /// Timely worker logger for source events
    pub logger: Option<Logger>,
    /// The function to return a now time.
    pub now: NowFn,
    /// The metrics & registry that each source instantiates.
    pub base_metrics: &'a SourceBaseMetrics,
}

#[derive(Clone, Serialize, Debug, Deserialize)]
/// A record produced by a source
pub struct SourceOutput<K, V>
where
    K: Data,
    V: Data,
{
    /// The record's key (or some empty/default value for sources without the concept of key)
    pub key: K,
    /// The record's value
    pub value: V,
    /// The position in the source, if such a concept exists (e.g., Kafka offset, file line number)
    pub position: Option<i64>,
    /// The time the record was created in the upstream systsem, as milliseconds since the epoch
    pub upstream_time_millis: Option<i64>,
}

/// The data that we send from sources to the decode process
#[derive(Debug, Default, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub(crate) struct SourceData {
    /// The actual value
    pub(crate) value: Option<Result<Row, DataflowError>>,
    /// The source's reported position for this record
    ///
    /// e.g. kafka offset or file location
    pub(crate) position: Option<i64>,

    /// The time that the upstream source believes that the message was created
    ///
    /// Currently only applies to Kafka
    pub(crate) upstream_time_millis: Option<i64>,
}

#[derive(Debug, Default, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
/// The output of the decoding operator
pub struct DecodeResult {
    /// The decoded key
    pub key: Option<Result<Row, DataflowError>>,
    /// The decoded value
    pub value: Option<Result<Row, DataflowError>>,
    /// The index of the decoded value in the stream
    pub position: Option<i64>,
}

impl<K, V> SourceOutput<K, V>
where
    K: Data,
    V: Data,
{
    /// Build a new SourceOutput
    pub fn new(
        key: K,
        value: V,
        position: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> SourceOutput<K, V> {
        SourceOutput {
            key,
            value,
            position,
            upstream_time_millis,
        }
    }
}
impl<K, V> SourceOutput<K, V>
where
    K: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
    V: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
{
    /// A parallelization contract that hashes by keys.
    ///
    /// It would not be ideal to use this in a case where the keys might be
    /// empty (or for any other reason too often the same),
    /// as that would lead to serial execution of whatever operator
    /// is being "parallelized" by this contract.
    ///
    /// One good reason to use this (and as of this writing, apparently the only reason)
    /// is for operators that need to do some dedpulication logic based on keys:
    /// For example, upserts, or Debezium dedpulication based on MySQL binlog coordinates.
    pub fn key_contract() -> impl ParallelizationContract<Timestamp, Self>
    where
        K: Hashable<Output = u64>,
    {
        Exchange::new(|x: &Self| x.key.hashed())
    }
    /// A parallelization contract that hashes by positions (if available)
    /// and otherwise falls back to hashing by value. Values can be just as
    /// skewed as keys, whereas positions are generally known to be unique or
    /// close to unique in a source. For example, Kafka offsets are unique per-partition.
    /// Most decode logic should use this instead of `key_contract`.
    pub fn position_value_contract() -> impl ParallelizationContract<Timestamp, Self>
    where
        V: Hashable<Output = u64>,
    {
        Exchange::new(|x: &Self| {
            if let Some(position) = x.position {
                position.hashed()
            } else {
                x.value.hashed()
            }
        })
    }
}

/// A `SourceToken` manages interest in a source.
///
/// When the `SourceToken` is dropped the associated source will be stopped.
pub struct SourceToken {
    capabilities: Rc<RefCell<Option<(Capability<Timestamp>, Capability<Timestamp>)>>>,
    activator: Activator,
}

impl SourceToken {
    /// Re-activates the associated timely source operator.
    pub fn activate(&self) {
        self.activator.activate();
    }
}

impl Drop for SourceToken {
    fn drop(&mut self) {
        *self.capabilities.borrow_mut() = None;
        self.activator.activate();
    }
}

/// The status of a source.
pub enum SourceStatus {
    /// The source is still alive.
    Alive,
    /// The source is complete.
    Done,
}

/// Types that implement this trait expose a length function
pub trait MaybeLength {
    /// Returns the size of the object
    fn len(&self) -> Option<usize>;
    /// Returns true if the object is empty
    fn is_empty(&self) -> bool;
}

impl MaybeLength for Vec<u8> {
    fn len(&self) -> Option<usize> {
        Some(self.len())
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl MaybeLength for Value {
    // Not possible to compute a size in bytes without recursively traversing the entire tree.
    fn len(&self) -> Option<usize> {
        None
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl MaybeLength for MessagePayload {
    fn len(&self) -> Option<usize> {
        match self {
            MessagePayload::Data(data) => Some(data.len()),
            MessagePayload::EOF => None,
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<T: MaybeLength> MaybeLength for Option<T> {
    fn len(&self) -> Option<usize> {
        self.as_ref().and_then(|v| v.len())
    }

    fn is_empty(&self) -> bool {
        self.as_ref().map(|v| v.is_empty()).unwrap_or_default()
    }
}

/// This trait defines the interface between Materialize and external sources, and
/// must be implemented for every new kind of source.
///
/// TODO: this trait is still a little too Kafka-centric, specifically the concept of
/// a "partition" is baked into this trait and introduces some cognitive overhead as
/// we are forced to treat things like file sources as "single-partition"
pub(crate) trait SourceReader {
    type Payload: timely::Data + MaybeLength;

    /// Create a new source reader.
    ///
    /// This function returns the source reader and optionally, any "partition" it's
    /// already reading. In practice, the partition is only non-None for static sources
    /// that either don't truly have partitions or have a fixed number of partitions.
    fn new(
        source_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        encoding: SourceDataEncoding,
        logger: Option<Logger>,
        metrics: crate::source::metrics::SourceBaseMetrics,
    ) -> Result<(Self, Option<PartitionId>), anyhow::Error>
    where
        Self: Sized;

    /// Instruct the source to start subscribing to `pid`.
    ///
    /// Many (most?) sources do not actually have partitions and for those kinds
    /// of sources the default implementation is a panic (to make sure we don't
    /// inadvertently call this at runtime).
    ///
    /// The optional `restored_offset` can be used to give an explicit offset that should be used when
    /// starting to read from the given partition.
    fn add_partition(&mut self, _pid: PartitionId, _restored_offset: Option<MzOffset>) {
        panic!("add partiton not supported for source!");
    }

    /// Returns the next message available from the source.
    ///
    /// Note that implementers are required to present messages in strictly ascending\
    /// offset order within each partition.
    fn get_next_message(&mut self) -> Result<NextMessage<Self::Payload>, anyhow::Error>;
}

#[derive(Debug)]
pub(crate) enum NextMessage<Payload> {
    Ready(SourceMessage<Payload>),
    Pending,
    TransientDelay,
    Finished,
}

/// Source-agnostic wrapper for messages. Each source must implement a
/// conversion to Message.
pub struct SourceMessage<Payload> {
    /// Partition from which this message originates
    pub partition: PartitionId,
    /// Materialize offset of the message (1-indexed)
    pub offset: MzOffset,
    /// The time that an external system first observed the message
    ///
    /// Milliseconds since the unix epoch
    pub upstream_time_millis: Option<i64>,
    /// Optional key
    pub key: Option<Vec<u8>>,
    /// The payload
    pub payload: Payload,
}

impl<Payload> fmt::Debug for SourceMessage<Payload> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceMessage")
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("upstream_time_millis", &self.upstream_time_millis)
            .field("key[present]", &self.key.is_some())
            .finish()
    }
}

/// Distinguish between offset bindings that are final and agreed upon (Fixed)
/// and still being negotiated (Tentative).
#[derive(Copy, Clone, Debug)]
enum TimestampUpperBound {
    Fixed(MzOffset),
    Tentative(MzOffset),
}

impl TimestampUpperBound {
    fn inner(&self) -> MzOffset {
        match self {
            TimestampUpperBound::Fixed(o) => *o,
            TimestampUpperBound::Tentative(o) => *o,
        }
    }

    fn is_fixed(&self) -> bool {
        match self {
            TimestampUpperBound::Fixed(_) => true,
            TimestampUpperBound::Tentative(_) => false,
        }
    }
}

/// Per-partition consistency information. This effectively acts a cache of
/// timestamp binding information that needs to be refreshed every time
/// the source operator is invoked.
#[derive(Copy, Clone, Debug)]
struct ConsInfo {
    /// The timestamp we are currently aware of. This timestamp is open iff
    /// offset < current_upper_bound  - 1 (aka there are more offsets we need to find)
    /// and closed otherwise.
    current_ts: Timestamp,
    /// Current upper bound for the current timestamp. All offsets < upper_bound
    /// are assigned to current_ts.
    current_upper_bound: TimestampUpperBound,
    /// The last processed offset
    offset: MzOffset,
}

impl ConsInfo {
    fn new(timestamp: Timestamp, offset: Option<MzOffset>) -> Self {
        let offset = offset.unwrap_or_else(|| MzOffset { offset: 0 });

        Self {
            current_ts: timestamp,
            current_upper_bound: TimestampUpperBound::Fixed(MzOffset { offset: 0 }),
            offset,
        }
    }

    /// Checks if the current binding is sufficient for this offset.
    ///
    /// This code assumes we will be ingesting offsets in order.
    fn is_valid(&self, offset: MzOffset) -> bool {
        if !self.current_upper_bound.is_fixed() || self.current_upper_bound.inner() > offset {
            true
        } else {
            false
        }
    }

    fn update_timestamp_fixed(&mut self, timestamp: Timestamp, upper: MzOffset) {
        assert!(timestamp >= self.current_ts);
        assert!(upper >= self.current_upper_bound.inner());

        self.current_upper_bound = TimestampUpperBound::Fixed(upper);
        self.current_ts = timestamp;
    }

    /// Store a new (time, offset) binding in the cache. If no such binding is
    /// available, we will start proposing our own.
    fn update_timestamp(&mut self, timestamp: Timestamp, offset: Option<MzOffset>) {
        if offset.is_some() {
            self.update_timestamp_fixed(timestamp, offset.expect("known to exist"));
        } else {
            // We only got a new timestamp, and no new upper bound. Let's declare the
            // upper bound to be equal to the current maximum processed offset and go
            // from there.
            assert!(timestamp >= self.current_ts);
            let old_upper = self.current_upper_bound.inner();
            self.current_upper_bound = TimestampUpperBound::Tentative(old_upper);
            self.current_ts = timestamp;
        }
    }

    /// Update the last read offset.
    fn update_offset(&mut self, offset: MzOffset) {
        assert!(offset >= self.offset);

        if self.current_upper_bound.is_fixed() {
            assert!(offset < self.current_upper_bound.inner())
        } else if offset > self.current_upper_bound.inner() {
            self.current_upper_bound = TimestampUpperBound::Tentative(offset);
        }

        self.offset = offset;
    }

    /// Return the maximum timestamp for which we will not receive any more updates.
    fn get_closed_timestamp(&self) -> Timestamp {
        if self.current_ts == 0
            || (self.current_upper_bound.is_fixed()
                && self.current_upper_bound.inner().offset - 1 == self.offset.offset)
        {
            return self.current_ts;
        }

        self.current_ts - 1
    }

    /// The most recently read offset.
    fn offset(&self) -> MzOffset {
        self.offset
    }

    /// Return a (time, offset) binding if we think we have a new one to contribute.
    fn proposal(&self) -> Option<(Timestamp, MzOffset)> {
        // Need to be careful here to make sure we are not echoing a prior proposal.
        if !self.current_upper_bound.is_fixed() && self.current_upper_bound.inner() == self.offset {
            Some((self.current_ts, self.current_upper_bound.inner()))
        } else {
            None
        }
    }
}

/// Contains all necessary information that relates to consistency and timestamping.
/// This information is (and should remain) source independent. This covers consistency
/// information for sources that follow RT consistency and BYO consistency.
pub struct ConsistencyInfo {
    /// Last closed timestamp
    last_closed_ts: u64,
    /// Frequency at which we should downgrade capability (in milliseconds)
    downgrade_capability_frequency: u64,
    /// Per partition (a partition ID in Kafka is an i32), keep track of the last closed offset
    /// and the last closed timestamp.
    /// Note that we only keep track of partitions this worker is responsible for in this
    /// hashmap.
    partition_metadata: HashMap<PartitionId, ConsInfo>,
    /// Source Type (Real-time or BYO)
    source_type: Consistency,
    /// Per-source Prometheus metrics.
    source_metrics: SourceMetrics,
    /// source global id
    source_id: SourceInstanceId,
    /// True if this worker is the active reader
    active: bool,
    /// id of worker
    worker_id: usize,
    /// total number of workers
    worker_count: usize,
}

impl ConsistencyInfo {
    fn new(
        active: bool,
        metrics_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        worker_count: usize,
        consistency: Consistency,
        timestamp_frequency: Duration,
        logger: Option<Logger>,
        base_metrics: &SourceBaseMetrics,
    ) -> ConsistencyInfo {
        ConsistencyInfo {
            last_closed_ts: 0,
            // Safe conversion: statement.rs checks that value specified fits in u64
            downgrade_capability_frequency: timestamp_frequency.as_millis().try_into().unwrap(),
            partition_metadata: HashMap::new(),
            source_type: consistency,
            source_metrics: SourceMetrics::new(
                &base_metrics,
                &metrics_name,
                source_id,
                &worker_id.to_string(),
                logger,
            ),
            source_id,
            active,
            worker_id,
            worker_count,
        }
    }

    /// Returns true if this worker is responsible for handling this partition
    fn responsible_for(&self, pid: &PartitionId) -> bool {
        match pid {
            PartitionId::None => self.active,
            PartitionId::Kafka(p) => {
                // We want to distribute partitions across workers evenly, such that
                // - different partitions for the same source are uniformly distributed across workers
                // - the same partition id across different sources are uniformly distributed across workers
                // - the same partition id across different instances of the same source is sent to
                //   the same worker.
                // We achieve this by taking a hash of the `source_id` (not the source instance id) and using
                // that to offset distributing partitions round robin across workers.

                // We keep only 32 bits of randomness from `hashed` to prevent 64 bit
                // overflow.
                let hash = (self.source_id.source_id.hashed() >> 32) + *p as u64;
                (hash % self.worker_count as u64) == self.worker_id as u64
            }
        }
    }

    /// Returns true if we currently know of particular partition. We know (and have updated the
    /// metadata for this partition) if there is an entry for it
    fn knows_of(&self, pid: &PartitionId) -> bool {
        self.partition_metadata.contains_key(pid)
    }

    /// Start tracking consistency information and metrics for `pid`. The optional `offset` will be
    /// used to correctly set the internal read offset, though sources are responsible for correctly
    /// starting to read only from that offset.
    ///
    /// Need to call this together with `SourceReader::add_partition` before we
    /// ingest from `pid`.
    fn add_partition(&mut self, pid: &PartitionId, offset: Option<MzOffset>) {
        if self.partition_metadata.contains_key(pid) {
            error!("Incorrectly attempting to add a partition twice for source: {} partition: {}. Ignoring",
                   self.source_id,
                   pid
            );

            return;
        }

        self.partition_metadata
            .insert(pid.clone(), ConsInfo::new(self.last_closed_ts, offset));
        self.source_metrics.add_partition(pid);
    }

    /// Refreshes the source instance's knowledge of timestamp bindings.
    ///
    /// This function needs to be called at the start of every source operator
    /// execution to ensure all source instances assign the same timestamps.
    fn refresh<S: SourceReader>(
        &mut self,
        source: &mut S,
        timestamp_bindings: &mut TimestampBindingRc,
    ) {
        // Pick up any new partitions that we don't know about but should.
        for (pid, offset) in timestamp_bindings.partitions() {
            if !self.knows_of(&pid) && self.responsible_for(&pid) {
                source.add_partition(pid.clone(), offset.clone());
                self.add_partition(&pid, offset);
            }

            if !self.responsible_for(&pid) {
                continue;
            }
            let cons_info = self
                .partition_metadata
                .get_mut(&pid)
                .expect("partition known to exist");

            if let Some((timestamp, max)) =
                timestamp_bindings.get_binding(&pid, cons_info.offset() + 1)
            {
                cons_info.update_timestamp(timestamp, max);
            }
        }
    }

    fn downgrade_capability(
        &mut self,
        cap: &mut Capability<Timestamp>,
        timestamp_bindings: &mut TimestampBindingRc,
    ) {
        //  We need to determine the maximum timestamp that is fully closed. This corresponds to the minimum of
        //  * closed timestamps across all partitions we own
        //  * maximum bound timestamps across all partitions we don't own (the `upper`)
        let mut upper = Antichain::new();
        timestamp_bindings.read_upper(&mut upper);

        let mut min: Option<Timestamp> = if let Some(time) = upper.elements().get(0) {
            Some(time.saturating_sub(1))
        } else {
            None
        };
        // Determine which timestamps have been closed. A timestamp is closed once we have processed
        // all messages that we are going to process for this timestamp across all partitions that the
        // worker knows about (i.e. the ones the worker has been assigned to read from).
        // In practice, the following happens:
        // Per partition, we iterate over the data structure to remove (ts,offset) mappings for which
        // we have seen all records <= offset. We keep track of the last "closed" timestamp in that partition
        // in next_partition_ts

        for (pid, cons_info) in self.partition_metadata.iter_mut() {
            let closed_ts = cons_info.get_closed_timestamp();
            let metrics = self
                .source_metrics
                .partition_metrics
                .get_mut(&pid)
                .expect("partition known to exist");

            metrics.closed_ts.set(closed_ts);

            min = match min.as_mut() {
                Some(min) => Some(std::cmp::min(closed_ts, *min)),
                None => Some(closed_ts),
            };
        }
        let min = min.unwrap_or(0);

        // Downgrade capability to new minimum open timestamp (which corresponds to min + 1).
        if min > self.last_closed_ts {
            self.source_metrics.capability.set(min);
            cap.downgrade(&(&min + 1));
            self.last_closed_ts = min;

            let new_compaction_frontier = Antichain::from_elem(min + 1);
            timestamp_bindings.set_compaction_frontier(new_compaction_frontier.borrow());
        }
    }

    /// Potentially propose new timestamp bindings to peers.
    ///
    /// Does nothing for BYO sources. This function needs to be called
    /// at the end of source operator execution to make sure all source
    /// instances assign the same timestamps.
    fn propose(&self, timestamp_bindings: &mut TimestampBindingRc) {
        if self.source_type == Consistency::RealTime {
            for (pid, cons_info) in self.partition_metadata.iter() {
                if let Some((time, offset)) = cons_info.proposal() {
                    timestamp_bindings.add_binding(
                        pid.clone(),
                        time,
                        MzOffset {
                            offset: offset.offset + 1,
                        },
                        true,
                    );
                }
            }
        }
    }

    /// For a given offset, returns an option type returning the matching timestamp or None
    /// if no timestamp can be assigned.
    ///
    /// The timestamp history contains a sequence of
    /// (partition_count, timestamp, offset) tuples. A message with offset x will be assigned the first timestamp
    /// for which offset>=x.
    fn find_matching_timestamp(
        &mut self,
        partition: &PartitionId,
        offset: MzOffset,
        timestamp_bindings: &TimestampBindingRc,
    ) -> Option<Timestamp> {
        // We either can take a fast path, where we simply re-use the currently
        // available timestamp binding, or if one isn't available for `offset` in `partition`, we have to
        // look it up from the set of timestamp histories

        // We know we will only read from partitions already assigned to this
        // worker.
        let cons_info = self
            .partition_metadata
            .get_mut(partition)
            .expect("known to exist");

        if cons_info.is_valid(offset) {
            // This is the fast path - we can reuse a timestamp binding
            // we already know about.
            cons_info.update_offset(offset);
            Some(cons_info.current_ts)
        } else {
            if let Some((timestamp, max_offset)) = timestamp_bindings.get_binding(partition, offset)
            {
                cons_info.update_timestamp(timestamp, max_offset);
                cons_info.update_offset(offset);
                Some(timestamp)
            } else {
                None
            }
        }
    }
}

/// Source-specific Prometheus metrics
pub struct SourceMetrics {
    /// Number of times an operator gets scheduled
    operator_scheduled_counter: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
    /// Value of the capability associated with this source
    capability: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Per-partition Prometheus metrics.
    pub partition_metrics: HashMap<PartitionId, PartitionMetrics>,
    logger: Option<Logger>,
    source_name: String,
    source_id: SourceInstanceId,
    base_metrics: SourceBaseMetrics,
}

impl SourceMetrics {
    /// Initialises source metrics for a given (source_id, worker_id)
    pub fn new(
        base: &SourceBaseMetrics,
        source_name: &str,
        source_id: SourceInstanceId,
        worker_id: &str,
        logger: Option<Logger>,
    ) -> SourceMetrics {
        let labels = &[
            source_name.to_string(),
            source_id.to_string(),
            worker_id.to_string(),
        ];
        SourceMetrics {
            operator_scheduled_counter: base
                .source_specific
                .operator_scheduled_counter
                .get_delete_on_drop_counter(labels.to_vec()),
            capability: base
                .source_specific
                .capability
                .get_delete_on_drop_gauge(labels.to_vec()),
            partition_metrics: Default::default(),
            logger,
            source_name: source_name.to_string(),
            source_id,
            base_metrics: base.clone(),
        }
    }

    /// Add metrics for `partition_id`
    pub fn add_partition(&mut self, partition_id: &PartitionId) {
        if self.partition_metrics.contains_key(partition_id) {
            error!(
                "incorrectly adding a duplicate partition metric in source: {} partition: {}",
                self.source_id, partition_id
            );
            return;
        }

        let metric = PartitionMetrics::new(
            &self.base_metrics,
            &self.source_name,
            self.source_id,
            partition_id,
        );
        self.partition_metrics.insert(partition_id.clone(), metric);
    }

    /// Log updates to which offsets / timestamps read up to.
    pub fn record_partition_offsets(&mut self, offsets: HashMap<PartitionId, (MzOffset, u64)>) {
        if self.logger.is_none() {
            return;
        }

        for (partition, (offset, timestamp)) in offsets {
            if let Some(metric) = self.partition_metrics.get_mut(&partition) {
                metric.record_offset(
                    &mut self.logger.as_mut().unwrap(),
                    &self.source_name,
                    self.source_id,
                    &partition,
                    offset.offset,
                    timestamp as i64,
                );
            }
        }
    }
}

impl Drop for SourceMetrics {
    fn drop(&mut self) {
        // retract our partition from logging
        if let Some(logger) = self.logger.as_mut() {
            for (partition, metric) in self.partition_metrics.iter() {
                logger.log(MaterializedEvent::SourceInfo {
                    source_name: self.source_name.clone(),
                    source_id: self.source_id,
                    partition_id: partition.into(),
                    offset: -metric.last_offset,
                    timestamp: -metric.last_timestamp,
                });
            }
        }
    }
}

/// Partition-specific metrics, recorded to both Prometheus and a system table
pub struct PartitionMetrics {
    /// Highest offset that has been received by the source and timestamped
    offset_ingested: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// Highest offset that has been received by the source
    offset_received: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// Value of the highest timestamp that is closed (for which all messages have been ingested)
    closed_ts: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Total number of messages that have been received by the source and timestamped
    messages_ingested: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
    last_offset: i64,
    last_timestamp: i64,
}

impl PartitionMetrics {
    /// Record the latest offset ingested high-water mark
    fn record_offset(
        &mut self,
        logger: &mut Logger,
        source_name: &str,
        source_id: SourceInstanceId,
        partition_id: &PartitionId,
        offset: i64,
        timestamp: i64,
    ) {
        logger.log(MaterializedEvent::SourceInfo {
            source_name: source_name.to_string(),
            source_id,
            partition_id: partition_id.into(),
            offset: offset - self.last_offset,
            timestamp: timestamp - self.last_timestamp,
        });
        self.last_offset = offset;
        self.last_timestamp = timestamp;
    }

    /// Initialises partition metrics for a given (source_id, partition_id)
    pub fn new(
        base_metrics: &SourceBaseMetrics,
        source_name: &str,
        source_id: SourceInstanceId,
        partition_id: &PartitionId,
    ) -> PartitionMetrics {
        let labels = &[
            source_name.to_string(),
            source_id.source_id.to_string(),
            source_id.dataflow_id.to_string(),
            partition_id.to_string(),
        ];
        let base = &base_metrics.partition_specific;
        PartitionMetrics {
            offset_ingested: base
                .offset_ingested
                .get_delete_on_drop_gauge(labels.to_vec()),
            offset_received: base
                .offset_received
                .get_delete_on_drop_gauge(labels.to_vec()),
            closed_ts: base.closed_ts.get_delete_on_drop_gauge(labels.to_vec()),
            messages_ingested: base
                .messages_ingested
                .get_delete_on_drop_counter(labels.to_vec()),
            last_offset: 0,
            last_timestamp: 0,
        }
    }
}

enum MessageProcessing {
    Stopped,
    Active,
    Yielded,
    YieldedWithDelay,
}

type EventSender =
    mpsc::Sender<Event<Option<Timestamp>, Result<(Row, Timestamp, Diff), SourceError>>>;

/// An active transaction at a particular point in time. An instance of this struct is provided to
/// a source when calling start_tx() on its timestamper. This has the effect of freezing the
/// timestamper clock while the data for the transaction is sent.
///
/// The transaction is automatically committed on Drop at which point the timestamper will continue
/// ticking its internal clock.
pub struct SourceTransaction<'a> {
    timestamp: RwLockReadGuard<'a, Timestamp>,
    sender: &'a EventSender,
}

impl SourceTransaction<'_> {
    /// Record an insertion of a row in the current transaction
    pub async fn insert(&self, row: Row) -> anyhow::Result<()> {
        let msg = Ok((row, *self.timestamp, 1));
        self.sender
            .send(Event::Message(*self.timestamp, msg))
            .await
            .or_else(|_| Err(anyhow!("channel closed")))
    }

    /// Record a deletion of a row in the current transaction
    pub async fn delete(&self, row: Row) -> anyhow::Result<()> {
        let msg = Ok((row, *self.timestamp, -1));
        self.sender
            .send(Event::Message(*self.timestamp, msg))
            .await
            .or_else(|_| Err(anyhow!("channel closed")))
    }
}

/// A thread-safe transaction manager that is responsible for assigning timestamps to the
/// transactions submitted to the system. Rows can be inserted individually using the
/// [insert](Timestamper::insert) and [delete](Timestamper::delete) methods or as part of a bigger
/// transaction.
///
/// When a transaction is started using [start_tx](Timestamper::start_tx) the internal clock will be
/// frozen and any subsequent rows will be timestamped with the exact same timestamp. The
/// transaction is committed automatically as soon as the transaction object gets dropped.
pub struct Timestamper {
    inner: Arc<RwLock<Timestamp>>,
    sender: EventSender,
    tick_duration: Duration,
    now: NowFn,
}

impl Timestamper {
    fn new(sender: EventSender, tick_duration: Duration, now: NowFn) -> Self {
        let ts = now();
        Self {
            inner: Arc::new(RwLock::new(ts)),
            sender,
            tick_duration,
            now,
        }
    }

    /// Start a transaction at a particular point in time. The timestamper will freeze its internal
    /// clock while a transaction is active.
    pub async fn start_tx<'a>(&'a self) -> SourceTransaction<'a> {
        SourceTransaction {
            timestamp: self.inner.read().await,
            sender: &self.sender,
        }
    }

    /// Record an insertion of a row
    pub async fn insert(&self, row: Row) -> anyhow::Result<()> {
        self.start_tx().await.insert(row).await
    }

    /// Record a deletion of a row
    pub async fn delete(&self, row: Row) -> anyhow::Result<()> {
        self.start_tx().await.delete(row).await
    }

    /// Records an error. After this method is called the source will permanently be in an errored
    /// state
    async fn error(&self, err: SourceError) -> anyhow::Result<()> {
        let timestamp = self.inner.read().await;
        self.sender
            .send(Event::Message(*timestamp, Err(err)))
            .await
            .or_else(|_| Err(anyhow!("channel closed")))
    }

    /// Attempts to monotonically increase the current timestamp and provides a Progress message to
    /// timely.
    ///
    /// This method will wait for all current transactions to commit before advancing the clock and
    /// will cause any new requests for transactions to wait for the tick to complete before
    /// starting.  This is due to the write-preferring behaviour of the tokio RwLock.
    async fn tick(&self) -> anyhow::Result<()> {
        tokio::time::sleep(self.tick_duration).await;
        let mut timestamp = self.inner.write().await;
        let mut now: u128 = (self.now)().into();

        // Round to the next greatest self.tick_duration increment.
        // This is to guarantee that different workers downgrade (without coordination) to the
        // "same" next time
        now += self.tick_duration.as_millis() - (now % self.tick_duration.as_millis());

        let now: u64 = now
            .try_into()
            .expect("materialize has existed for more than 500M years");

        if *timestamp < now {
            *timestamp = now;
            self.sender
                .send(Event::Progress(Some(*timestamp)))
                .await
                .or_else(|_| Err(anyhow!("channel closed")))?;
        }
        Ok(())
    }
}

/// Simple sources must implement this trait. Sources will then get created as part of the
/// [`create_source_simple`] function.
///
/// Each simple source is given access to a timestamper instance that can be used to insert or
/// retract rows for this source. See the API of the [Timestamper](Timestamper) for more details.
#[async_trait]
pub trait SimpleSource {
    /// Consumes the instance of this SimpleSource and converts it into an async state machine that
    /// submits rows using the provided timestamper.
    ///
    /// Implementors should return an Err(_) if an unrecoverable error is encountered or Ok(()) when
    /// they have finished consuming the upstream data.
    async fn start(self, timestamper: &Timestamper) -> Result<(), SourceError>;
}

/// Creates a source dataflow operator from a connector implementing [SimpleSource](SimpleSource)
pub fn create_source_simple<G, C>(
    config: SourceConfig<G>,
    connector: C,
) -> (
    (
        timely::dataflow::Stream<G, (Row, Timestamp, Diff)>,
        timely::dataflow::Stream<G, SourceError>,
    ),
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
    C: SimpleSource + Send + 'static,
{
    let SourceConfig {
        id,
        name,
        upstream_name,
        scope,
        active,
        worker_id,
        timestamp_frequency,
        logger,
        now,
        base_metrics,
        ..
    } = config;

    let (tx, mut rx) = mpsc::channel(64);

    if active {
        tokio::spawn(async move {
            let timestamper = Timestamper::new(tx, timestamp_frequency, now);
            let source = connector.start(&timestamper);
            tokio::pin!(source);

            loop {
                tokio::select! {
                    res = timestamper.tick() => {
                        if res.is_err() {
                            break;
                        }
                    }
                    res = &mut source => {
                        if let Err(err) = res {
                            let _ = timestamper.error(err).await;
                        }
                        break;
                    }
                }
            }
        });
    }

    let (stream, _secondary_stream, capability) = source(scope, name.clone(), move |info| {
        let activator = Arc::new(scope.sync_activator_for(&info.address[..]));

        let metrics_name = upstream_name.unwrap_or(name);
        let mut metrics = SourceMetrics::new(
            &base_metrics,
            &metrics_name,
            id,
            &worker_id.to_string(),
            logger,
        );

        if active {
            metrics.add_partition(&PartitionId::None);
        }

        move |cap,
              _secondary_cap: &mut Capability<Timestamp>,
              output,
              _secondary_output: &mut OutputHandle<Timestamp, (), _>| {
            if !active {
                return SourceStatus::Done;
            }

            let waker = futures::task::waker_ref(&activator);
            let mut context = Context::from_waker(&waker);

            while let Poll::Ready(item) = rx.poll_recv(&mut context) {
                match item {
                    Some(Event::Progress(None)) => unreachable!(),
                    Some(Event::Progress(Some(time))) => {
                        let mut metric_updates = HashMap::new();
                        metric_updates.insert(
                            PartitionId::None,
                            (
                                MzOffset {
                                    offset: time as i64,
                                },
                                time,
                            ),
                        );
                        metrics.record_partition_offsets(metric_updates);
                        cap.downgrade(&time);
                    }
                    Some(Event::Message(time, data)) => {
                        output.session(&cap.delayed(&time)).give(data);
                    }
                    None => {
                        return SourceStatus::Done;
                    }
                }
            }

            return SourceStatus::Alive;
        }
    });

    (
        stream.map_fallible("SimpleSourceErrorDemux", |r| r),
        Some(capability),
    )
}

/// Creates a source dataflow operator. The type of ExternalSourceConnector determines the type of
/// source that should be created
///
/// If `persist_config` is `Some`, this will emit and persist a stream of timestamp bindings and
/// use the persisted bindings on startup to seed initial source offsets and timestamp bindings.
///
/// The returned `Stream` of persisted timestamp bindings can be used to track the persistence
/// frontier and should be used to seal up the backing collection to that frontier. This function
/// does not do any sealing and it is the responsibility of the caller to eventually do that, for
/// example using [`conditional_seal`](persist::operators::stream::Seal::conditional_seal).
pub(crate) fn create_source<G, S: 'static>(
    config: SourceConfig<G>,
    source_connector: &ExternalSourceConnector,
    persist_config: Option<PersistentTimestampBindingsConfig<SourceTimestamp, AssignedTimestamp>>,
) -> (
    (
        timely::dataflow::Stream<G, SourceOutput<Vec<u8>, S::Payload>>,
        timely::dataflow::Stream<G, ((SourceTimestamp, AssignedTimestamp), Timestamp, Diff)>,
        timely::dataflow::Stream<G, SourceError>,
    ),
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader,
{
    let SourceConfig {
        name,
        sql_name,
        upstream_name,
        id,
        scope,
        mut timestamp_histories,
        worker_id,
        worker_count,
        consistency,
        timestamp_frequency,
        active,
        encoding,
        logger,
        base_metrics,
        ..
    } = config;

    // Errors that come up during the construction of the source operators. We cannot bubble them
    // up via a Result because dataflow installation currently has to be infallible on the workers.
    //
    // We therefore collect errors in a vec and create a stream that we combine with the other
    // error output streams.
    //
    // TODO: This is not ideal, especially because these system errors should not make it into the
    // relational error collections. But we don't have another way of presenting them to users
    // right now. We should fix this once we have clearer understanding of different types of
    // errors. The errors here would likely be considered transitive or indefinite errors.
    let mut construction_errs = Vec::new();

    let (source_persist, restored_bindings) = match persist_config {
        Some(persist_config) => {
            let source_persist = SourceReaderPersistence::new(name.clone(), persist_config);

            let timestamp_histories = timestamp_histories.as_mut().ok_or_else(|| {
                SourceError::new(
                    sql_name.clone(),
                    SourceErrorDetails::Persistence("missing timestamp histories".to_owned()),
                )
            });

            let result = timestamp_histories.and_then(|timestamp_histories| {
                let (offsets, mut bindings) = source_persist.restore().map_err(|e| {
                    SourceError::new(
                        sql_name.clone(),
                        SourceErrorDetails::Persistence(format!(
                            "restoring timestamp bindings: {}",
                            e
                        )),
                    )
                })?;

                for (pid, offset) in offsets {
                    timestamp_histories.add_partition(pid, Some(offset));
                }

                // We need to sort by offset and then timestamp because `add_binding()` will not allow
                // adding bindings that go "backwards".
                bindings.sort_by(|a, b| (a.0.offset.offset, a.1).cmp(&(b.0.offset.offset, b.1)));

                for (source_ts, assigned_ts) in bindings.iter() {
                    // The timestamp bindings are potentially pre-seeded by bindings that we
                    // restored from the coordinator, if/when coordinator based timestamp
                    // persistence is active.
                    //
                    // The bindings from persistence and the bindings from the coordinator are not
                    // 100 % in sync because it can happen that one of them succeeds in writing
                    // bindings right before a crash that prevents the other from writing. This is
                    // ok, though, because we can just take the bindings which are "further in the
                    // future".
                    //
                    // Side note: with persistence enabled for the source, we will anyways never
                    // re-emit old data, because we start reading from the persisted offsets.
                    let current_binding = timestamp_histories
                        .get_binding(&source_ts.partition, source_ts.offset)
                        .map(|(binding, _offset)| binding)
                        .unwrap_or(0);
                    if current_binding < assigned_ts.0 {
                        timestamp_histories.add_binding(
                            source_ts.partition.clone(),
                            assigned_ts.0,
                            source_ts.offset,
                            false,
                        );
                    } else {
                        log::debug!(
                            "Filtered out timestamp binding {:?} from persistence because we already have {}.",
                            (source_ts, assigned_ts),
                            current_binding
                        );
                    }
                }

                Ok((Some(source_persist), Some(bindings)))
            });

            match result {
                Ok(result) => result,
                Err(e) => {
                    construction_errs.push(e);
                    (None, None)
                }
            }
        }
        None => (None, None),
    };

    let bytes_read_counter = base_metrics.bytes_read.clone();

    let emit_timestamp_bindings = source_persist.is_some();

    let (stream, ts_bindings_stream, capability) = source(scope, name.clone(), move |info| {
        // Create activator for source
        let activator = scope.activator_for(&info.address[..]);

        // Create control plane information (Consistency-related information)
        let mut consistency_info = ConsistencyInfo::new(
            active,
            upstream_name.unwrap_or_else(|| name.clone()),
            id,
            worker_id,
            worker_count,
            consistency,
            timestamp_frequency,
            logger.clone(),
            &base_metrics,
        );

        // Create source information (this function is specific to a specific
        // source

        let mut source_reader: Option<S> = if !active {
            None
        } else {
            match SourceReader::new(
                name.clone(),
                id,
                worker_id,
                scope.sync_activator_for(&info.address[..]),
                source_connector.clone(),
                encoding,
                logger,
                base_metrics.clone(),
            ) {
                Ok((source_reader, partition)) => {
                    if let Some(pid) = partition {
                        consistency_info.add_partition(&pid, None);
                    }

                    Some(source_reader)
                }

                Err(e) => {
                    error!("Failed to create source: {}", e);
                    None
                }
            }
        };

        // Stash messages we cannot yet timestamp here.
        let mut buffer = None;

        let mut timestamp_bindings_updater = if emit_timestamp_bindings {
            let restored_bindings = restored_bindings.expect("missing restored bindings");
            Some(TimestampBindingUpdater::new(restored_bindings))
        } else {
            None
        };

        move |cap, bindings_cap, output, bindings_output| {
            // First check that the source was successfully created
            let source_reader = match &mut source_reader {
                Some(source_reader) => source_reader,
                None => {
                    return SourceStatus::Done;
                }
            };

            // Check that we have a valid list of timestamp bindings.
            let mut timestamp_histories = match &mut timestamp_histories {
                Some(histories) => histories,
                None => {
                    error!("Source missing list of timestamp bindings");
                    return SourceStatus::Done;
                }
            };

            // Refresh any consistency info from the worker that we need to
            consistency_info.refresh(source_reader, &mut timestamp_histories);
            // Downgrade capability (if possible)
            consistency_info.downgrade_capability(cap, &mut timestamp_histories);
            bindings_cap.downgrade(cap.time());
            // Bound execution of operator to prevent a single operator from hogging
            // the CPU if there are many messages to process
            let timer = Instant::now();
            // Accumulate updates to bytes_read for Prometheus metrics collection
            let mut bytes_read = 0;
            // Accumulate updates to offsets for system table metrics collection
            let mut metric_updates = HashMap::new();

            // Record operator has been scheduled
            consistency_info
                .source_metrics
                .operator_scheduled_counter
                .inc();

            let mut source_state = (SourceStatus::Alive, MessageProcessing::Active);
            while let (_, MessageProcessing::Active) = source_state {
                // If we previously buffered something, try to ingest that first.
                // Otherwise, try to pull a new message from the source.
                source_state = if buffer.is_some() {
                    let message = buffer.take().unwrap();
                    handle_message::<S>(
                        message,
                        &mut consistency_info,
                        &mut bytes_read,
                        &cap,
                        output,
                        &mut metric_updates,
                        &timer,
                        &mut buffer,
                        &timestamp_histories,
                    )
                } else {
                    match source_reader.get_next_message() {
                        Ok(NextMessage::Ready(message)) => handle_message::<S>(
                            message,
                            &mut consistency_info,
                            &mut bytes_read,
                            &cap,
                            output,
                            &mut metric_updates,
                            &timer,
                            &mut buffer,
                            &timestamp_histories,
                        ),
                        Ok(NextMessage::TransientDelay) => {
                            // There was a temporary hiccup in getting messages, check again asap.
                            (SourceStatus::Alive, MessageProcessing::Yielded)
                        }
                        Ok(NextMessage::Pending) => {
                            // There were no new messages, check again after a delay
                            (SourceStatus::Alive, MessageProcessing::YieldedWithDelay)
                        }
                        Ok(NextMessage::Finished) => {
                            if let Consistency::RealTime = consistency_info.source_type {
                                (SourceStatus::Done, MessageProcessing::Stopped)
                            } else {
                                // The coord drives Doneness decisions for BYO, so we must still return Alive
                                // on EOF
                                (SourceStatus::Alive, MessageProcessing::YieldedWithDelay)
                            }
                        }
                        Err(e) => {
                            output.session(&cap).give(Err(e.to_string()));
                            (SourceStatus::Done, MessageProcessing::Stopped)
                        }
                    }
                }
            }

            bytes_read_counter.inc_by(bytes_read as u64);
            consistency_info
                .source_metrics
                .record_partition_offsets(metric_updates);

            // Propose any new timestamp bindings we need to.
            consistency_info.propose(&mut timestamp_histories);

            // Emit any new timestamp bindings that we might have since we last emitted.
            if let Some(timestamp_bindings_updater) = timestamp_bindings_updater.as_mut() {
                let changes = timestamp_bindings_updater.update(timestamp_histories);

                // Emit required changes downstream.
                let to_emit = changes
                    .into_iter()
                    .filter(|((source_ts, _assigned_ts), _diff)| {
                        consistency_info.responsible_for(&source_ts.partition)
                    })
                    .map(|(binding, diff)| {
                        (
                            binding,
                            bindings_cap.time().clone(),
                            diff.try_into()
                                .expect("could not convert i64 diff to isize"),
                        )
                    });

                // We're collecting into a Vec because we want to log and emit. This is a bit
                // wasteful but we don't expect large numbers of bindings.
                let mut to_emit = to_emit.collect::<Vec<_>>();

                log::trace!(
                    "In {} (worker {}), emitting new timestamp bindings: {:?}, cap: {:?}",
                    name.clone(),
                    worker_id,
                    to_emit,
                    bindings_cap
                );

                let mut session = bindings_output.session(bindings_cap);
                session.give_vec(&mut to_emit);
            }

            // Downgrade capability (if possible) before exiting
            consistency_info.downgrade_capability(cap, &mut timestamp_histories);
            bindings_cap.downgrade(cap.time());

            let (source_status, processing_status) = source_state;
            // Schedule our next activation
            match processing_status {
                MessageProcessing::Yielded => activator.activate(),
                MessageProcessing::YieldedWithDelay => activator.activate_after(
                    Duration::from_millis(consistency_info.downgrade_capability_frequency),
                ),
                _ => (),
            }

            source_status
        }
    });

    let sql_name_for_error = sql_name.clone();
    let (ok_stream, err_stream) = stream.map_fallible("SourceErrorDemux", move |r| {
        r.map_err(|e| SourceError::new(sql_name_for_error.clone(), SourceErrorDetails::FileIO(e)))
    });

    let (ts_bindings_stream, ts_bindings_err_stream) = if let Some(source_persist) = source_persist
    {
        source_persist.render_persistence_operators(sql_name, ts_bindings_stream)
    } else {
        (ts_bindings_stream, operator::empty(scope))
    };

    // Work around `to_stream()` requiring a `&mut scope`.
    let mut scope = scope.clone();
    let construction_errs_stream = construction_errs.to_stream(&mut scope);

    // TODO: It is not ideal that persistence errors end up in the same differential error
    // collection as other errors because they are transient/indefinite errors that we should be
    // treating differently. We do not, however, at the current time have to infrastructure for
    // treating these errors differently, so we're adding them to the same error collections.
    let err_stream = err_stream
        .concat(&construction_errs_stream)
        .concat(&ts_bindings_err_stream);

    if active {
        (
            (ok_stream, ts_bindings_stream, err_stream),
            Some(capability),
        )
    } else {
        // Immediately drop the capability if worker is not an active reader for source
        ((ok_stream, ts_bindings_stream, err_stream), None)
    }
}

/// Util for restoring persisted timestamps and rendering persistence operators.
struct SourceReaderPersistence {
    source_name: String,
    config: PersistentTimestampBindingsConfig<SourceTimestamp, AssignedTimestamp>,
}

impl SourceReaderPersistence {
    /// Creates a new [`SourceReaderPersistence`] from the given configuration. The configuration
    /// determines the persistent collection that will be used by [`restore`](Self::restore) and
    /// [`render_persistence_operators`](Self::render_persistence_operators), respectively.
    fn new(
        source_name: String,
        config: PersistentTimestampBindingsConfig<SourceTimestamp, AssignedTimestamp>,
    ) -> Self {
        Self {
            source_name,
            config,
        }
    }

    /// Restores the known source partitions (along with their latest read offset) and the known
    /// timestamp bindings.
    fn restore(
        &self,
    ) -> Result<
        (
            HashMap<PartitionId, MzOffset>,
            Vec<(SourceTimestamp, AssignedTimestamp)>,
        ),
        persist::error::Error,
    > {
        let (offsets, bindings) = self.internal_restore(
            self.config.read_handle.clone(),
            self.config.upper_bindings_seal_ts,
            self.config.upper_data_seal_ts,
        )?;

        log::trace!(
            "In {}, initial (restored) source offsets: {:?}",
            self.source_name,
            offsets,
        );

        log::trace!(
            "In {}, initial (restored) timestamp bindings: {:?}",
            self.source_name,
            bindings,
        );

        Ok((offsets, bindings))
    }

    /// Restores the latest partition offsets and timestamp bindings from the given `StreamReadHandle`.
    ///
    /// This restores partition offsets that are not beyond the given `upper_data_seal_ts`, to ensure
    /// that the updates we emit from the source are consistent with the state that we have in the
    /// persistent data collection. We do restore bindings up to `upper_bindings_seal_ts`, which is
    /// potentially beyond `upper_data_seal_ts` because we seal the latter before sealing the former.
    /// This means we will emit source data with previously persisted bindings, which is a valid thing
    /// to do.
    ///
    /// When `write` is `Some(StreamWriteHandle)`, this will also emit retractions for updates that are
    /// beyond the `upper_bindings_seal_ts`. This should be used when restoring to clean up unsealed
    /// updates from previous attempts.
    fn internal_restore(
        &self,
        read: StreamReadHandle<SourceTimestamp, AssignedTimestamp>,
        upper_bindings_seal_ts: u64,
        upper_data_seal_ts: u64,
    ) -> Result<
        (
            HashMap<PartitionId, MzOffset>,
            Vec<(SourceTimestamp, AssignedTimestamp)>,
        ),
        persist::error::Error,
    > {
        assert!(upper_bindings_seal_ts >= upper_data_seal_ts);

        let mut bindings: HashMap<_, isize> = HashMap::new();
        let mut starting_offsets = HashMap::new();

        let snapshot = read.snapshot()?;

        let buf = snapshot.into_iter().collect::<Result<Vec<_>, _>>()?;

        for ((source_timestamp, assigned_timestamp), ts, diff) in buf.into_iter() {
            // Only restore starting offsets that are not beyond the uppser_data_seal_ts. This is the
            // timestamp up to which we have sealed the persistent collection storing the actual source
            // data/updates.
            if ts < upper_data_seal_ts {
                starting_offsets
                    .entry(source_timestamp.partition.clone())
                    .and_modify(|current_offset| {
                        if source_timestamp.offset > *current_offset {
                            *current_offset = source_timestamp.offset;
                        }
                    })
                    .or_insert_with(|| source_timestamp.offset);
            }

            // Collect all the bindings. The bindings are potentially beyond the starting offsets, but
            // that is ok. This means we will emit source data with previously persisted bindings.
            //
            // We consolidate all the diffs as we go. Below we collect all positive updates and
            // return them.
            *bindings
                .entry((source_timestamp, assigned_timestamp))
                .or_default() += diff;
        }

        let bindings: Vec<_> = bindings
            .drain()
            .filter(|(binding, diff)| {
                if *diff < 0 || *diff > 1 {
                    panic!(
                        "Binding with invalid diff. Binding {:?}, diff: {}.",
                        binding, diff
                    );
                }
                *diff == 1
            })
            .map(|(binding, _diff)| binding)
            .collect();

        Ok((starting_offsets, bindings))
    }

    /// Renders operators that persist the given `ts_bindings_stream` to the configured persistent
    /// collection.
    ///
    /// This does not seal the persistent collection, calling code must ensure that this happens
    /// eventually.
    fn render_persistence_operators<G>(
        &self,
        source_name: String,
        ts_bindings_stream: Stream<G, ((SourceTimestamp, AssignedTimestamp), Timestamp, Diff)>,
    ) -> (
        Stream<G, ((SourceTimestamp, AssignedTimestamp), Timestamp, Diff)>,
        Stream<G, SourceError>,
    )
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let persist_operator_name = format!("{}-timestamp-bindings", self.source_name);

        let (ts_bindings_stream, ts_bindings_persist_err) =
            ts_bindings_stream.persist(&persist_operator_name, self.config.write_handle.clone());

        // We're throwing away the differential information that we theoretically get from
        // `persist()`. We have to do this because sources currently only emit a `Stream<_,
        // SourceError>`. In practice, persist errors are non-retractable, currently.
        let ts_bindings_persist_err = ts_bindings_persist_err.map(move |(error, _ts, _diff)| {
            SourceError::new(source_name.clone(), SourceErrorDetails::Persistence(error))
        });

        (ts_bindings_stream, ts_bindings_persist_err)
    }
}

/// Configuration for persistent timestamp bindings.
///
/// `ST` is the source timestamp, while `AT` is the timestamp that is assigned based on timestamp
/// bindings.
#[derive(Clone)]
pub struct PersistentTimestampBindingsConfig<ST: Codec, AT: Codec> {
    /// The timestamp up to which which timestamp bindings have been sealed.
    upper_bindings_seal_ts: u64,

    /// The timestamp up to which which data (the updates read from the source) has been sealed.
    ///
    /// This can be different from `upper_bindings_seal_ts` because we seal bindings before data,
    /// and the latter can fail after we succesfully sealed the bindings.
    upper_data_seal_ts: u64,

    /// [`StreamReadHandle`] for the collection that we should persist to.
    read_handle: StreamReadHandle<ST, AT>,

    /// [`StreamWriteHandle`] for the collection that we should persist to.
    pub write_handle: StreamWriteHandle<ST, AT>,
}

impl<K: Codec, V: Codec> PersistentTimestampBindingsConfig<K, V> {
    /// Creates a new [`PersistentTimestampBindingsConfig`] from the given parts.
    pub fn new(
        upper_bindings_seal_ts: u64,
        upper_data_seal_ts: u64,
        read_handle: StreamReadHandle<K, V>,
        write_handle: StreamWriteHandle<K, V>,
    ) -> Self {
        // We always seal bindings before data.
        assert!(upper_bindings_seal_ts >= upper_data_seal_ts);

        PersistentTimestampBindingsConfig {
            upper_bindings_seal_ts,
            upper_data_seal_ts,
            read_handle,
            write_handle,
        }
    }
}

/// Take `message` and assign it the appropriate timestamps and push it into the
/// dataflow layer, if possible.
///
/// TODO: This function is a bit of a mess rn but hopefully this function makes the
/// existing mess more obvious and points towards ways to improve it.
fn handle_message<S: SourceReader>(
    message: SourceMessage<S::Payload>,
    consistency_info: &mut ConsistencyInfo,
    bytes_read: &mut usize,
    cap: &Capability<Timestamp>,
    output: &mut OutputHandle<
        Timestamp,
        Result<SourceOutput<Vec<u8>, S::Payload>, String>,
        Tee<Timestamp, Result<SourceOutput<Vec<u8>, S::Payload>, String>>,
    >,
    metric_updates: &mut HashMap<PartitionId, (MzOffset, Timestamp)>,
    timer: &std::time::Instant,
    buffer: &mut Option<SourceMessage<S::Payload>>,
    timestamp_bindings: &TimestampBindingRc,
) -> (SourceStatus, MessageProcessing) {
    let partition = message.partition.clone();
    let offset = message.offset;

    // Update ingestion metrics. Guaranteed to exist as the appropriate
    // entry gets created in SourceReader or when a new partition
    // is discovered
    consistency_info
        .source_metrics
        .partition_metrics
        .get_mut(&partition)
        .unwrap_or_else(|| panic!("partition metrics do not exist for partition {}", partition))
        .offset_received
        .set(offset.offset);

    // Determine the timestamp to which we need to assign this message
    let ts = consistency_info.find_matching_timestamp(&partition, offset, timestamp_bindings);
    match ts {
        None => {
            // We have not yet decided on a timestamp for this message,
            // we need to buffer the message
            *buffer = Some(message);
            (SourceStatus::Alive, MessageProcessing::Yielded)
        }
        Some(ts) => {
            // Note: empty and null payload/keys are currently
            // treated as the same thing.
            let key = message.key.unwrap_or_default();
            let out = message.payload;
            // Entry for partition_metadata is guaranteed to exist as messages
            // are only processed after we have updated the partition_metadata for a
            // partition and created a partition queue for it.
            *bytes_read += key.len();
            if let Some(len) = out.len() {
                *bytes_read += len;
            }
            let ts_cap = cap.delayed(&ts);

            output.session(&ts_cap).give(Ok(SourceOutput::new(
                key,
                out,
                Some(offset.offset),
                message.upstream_time_millis,
            )));

            // Update ingestion metrics
            // Entry is guaranteed to exist as it gets created when we initialise the partition
            let partition_metrics = consistency_info
                .source_metrics
                .partition_metrics
                .get_mut(&partition)
                .unwrap();
            partition_metrics.offset_ingested.set(offset.offset);
            partition_metrics.messages_ingested.inc();

            metric_updates.insert(partition, (offset, ts));

            if timer.elapsed().as_millis() > YIELD_INTERVAL_MS {
                // We didn't drain the entire queue, so indicate that we
                // should run again but yield the CPU to other operators.
                (SourceStatus::Alive, MessageProcessing::Yielded)
            } else {
                (SourceStatus::Alive, MessageProcessing::Active)
            }
        }
    }
}
