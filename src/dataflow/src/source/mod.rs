// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow sources.

use mz_avro::types::Value;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use timely::dataflow::{
    channels::pact::{Exchange, ParallelizationContract},
    operators::{Capability, Event},
};

use anyhow::anyhow;
use async_trait::async_trait;
use dataflow_types::{Consistency, DataEncoding, ExternalSourceConnector, MzOffset, SourceError};
use expr::{PartitionId, SourceInstanceId};
use lazy_static::lazy_static;
use log::{debug, error, trace};
use prometheus::core::{AtomicI64, AtomicU64};
use prometheus::{
    register_int_counter, register_int_counter_vec, register_int_gauge_vec,
    register_uint_gauge_vec, DeleteOnDropCounter, DeleteOnDropGauge, IntCounter, IntCounterVec,
    IntGaugeVec, UIntGauge, UIntGaugeVec,
};
use repr::{CachedRecord, CachedRecordIter, Diff, Row, Timestamp};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::scheduling::activate::{Activator, SyncActivator};
use timely::Data;
use tokio::sync::{mpsc, RwLock, RwLockReadGuard};

use super::source::util::source;
use crate::logging::materialized::{Logger, MaterializedEvent};
use crate::operator::StreamExt;
use crate::source::cache::WorkerCacheData;
use crate::source::timestamp::{TimestampBindingRc, TimestampDataUpdate, TimestampDataUpdates};
use crate::CacheMessage;

mod file;
mod kafka;
mod kinesis;
mod postgres;
mod pubnub;
mod s3;
mod util;

pub mod cache;
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
    pub timestamp_histories: TimestampDataUpdates,
    /// A source can use Real-Time consistency timestamping or BYO consistency information.
    pub consistency: Consistency,
    /// Source Type
    /// Timestamp Frequency: frequency at which timestamps should be closed (and capabilities
    /// downgraded)
    pub timestamp_frequency: Duration,
    /// Whether this worker has been chosen to actually receive data.
    pub active: bool,
    /// Data encoding
    pub encoding: DataEncoding,
    /// Channel to send source caching information to cacher thread
    pub caching_tx: Option<mpsc::UnboundedSender<CacheMessage>>,
    /// Timely worker logger for source events
    pub logger: Option<Logger>,
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
    pub(crate) value: Vec<u8>,
    /// The source's reported position for this record
    ///
    /// e.g. kafka offset or file location
    pub(crate) position: Option<i64>,

    /// The time that the upstream source believes that the message was created
    ///
    /// Currently only applies to Kafka
    pub(crate) upstream_time_millis: Option<i64>,
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
    capability: Rc<RefCell<Option<Capability<Timestamp>>>>,
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
        *self.capability.borrow_mut() = None;
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

// Global Prometheus metrics
lazy_static! {
    static ref BYTES_READ_COUNTER: IntCounter =
        register_int_counter!("mz_bytes_read_total", "Count of bytes read from sources").unwrap();
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

/// This trait defines the interface between Materialize and external sources, and
/// must be implemented for every new kind of source.
///
/// TODO: this trait is still a little too Kafka-centric, specifically the concept of
/// a "partition" is baked into this trait and introduces some cognitive overhead as
/// we are forced to treat things like file sources as "single-partition"
pub(crate) trait SourceReader<Out> {
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
        encoding: DataEncoding,
        logger: Option<Logger>,
    ) -> Result<(Self, Option<PartitionId>), anyhow::Error>
    where
        Self: Sized;

    /// Instruct the source to start subscribing to `pid`.
    ///
    /// Many (most?) sources do not actually have partitions and for those kinds
    /// of sources the default implementation is a panic (to make sure we don't
    /// inadvertently call this at runtime).
    fn add_partition(&mut self, _pid: PartitionId) {
        panic!("add partiton not supported for source!");
    }

    /// Returns the next message available from the source.
    ///
    /// Note that implementers are required to present messages in strictly ascending\
    /// offset order within each partition.
    fn get_next_message(&mut self) -> Result<NextMessage<Out>, anyhow::Error>;
}

/// This trait defines the interface to cache incoming messages.
///
/// Every output message type must implement this trait.
pub trait SourceCache {
    /// Cache a message
    fn cache_message(
        message: &SourceMessage<Self>,
        source_id: SourceInstanceId,
        caching_tx: &mut Option<mpsc::UnboundedSender<CacheMessage>>,
        timestamp: Timestamp,
        offset: Option<MzOffset>,
    ) where
        Self: Sized;

    /// Read back data from a previously cached file.
    /// Reads messages back from files in offset order, and returns None when there is
    /// no more data left to process
    /// TODO(rkhaitan): clean this up to return a proper type and potentially a iterator.
    fn read_file(path: PathBuf) -> Vec<SourceMessage<Self>>
    where
        Self: Sized;
}

impl SourceCache for Vec<u8> {
    fn cache_message(
        message: &SourceMessage<Vec<u8>>,
        source_id: SourceInstanceId,
        caching_tx: &mut Option<mpsc::UnboundedSender<CacheMessage>>,
        timestamp: Timestamp,
        predecessor: Option<MzOffset>,
    ) {
        // Send this record to be cached
        if let Some(caching_tx) = caching_tx {
            let partition_id = match message.partition {
                PartitionId::Kafka(p) => p,
                _ => unreachable!(),
            };

            // TODO(rkhaitan): let's experiment with wrapping these in a
            // Arc so we don't have to clone.
            let key = message.key.clone().unwrap_or_default();
            let value = message.payload.clone().unwrap_or_default();

            let cache_data = CacheMessage::Data(WorkerCacheData {
                source_id: source_id.source_id,
                partition_id,
                record: CachedRecord {
                    predecessor: predecessor.map(|p| p.offset),
                    offset: message.offset.offset,
                    timestamp,
                    key,
                    value,
                },
            });

            // TODO(benesch): the lack of backpressure here can result in
            // unbounded memory usage.
            caching_tx
                .send(cache_data)
                .expect("caching receiver should never drop first");
        }
    }

    fn read_file(path: PathBuf) -> Vec<SourceMessage<Vec<u8>>> {
        debug!("reading cached data from {}", path.display());
        let data = ::std::fs::read(&path).unwrap_or_else(|e| {
            error!("failed to read source cache file {}: {}", path.display(), e);
            vec![]
        });

        let partition =
            crate::source::cache::cached_file_partition(&path).expect("partition known to exist");
        CachedRecordIter::new(data)
            .map(move |r| SourceMessage {
                key: Some(r.key),
                payload: Some(r.value),
                offset: MzOffset { offset: r.offset },
                upstream_time_millis: None,
                partition: partition.clone(),
            })
            .collect()
    }
}

impl SourceCache for mz_avro::types::Value {
    fn cache_message(
        _message: &SourceMessage<Self>,
        _source_id: SourceInstanceId,
        _caching_tx: &mut Option<mpsc::UnboundedSender<CacheMessage>>,
        _timestamp: Timestamp,
        _offset: Option<MzOffset>,
    ) {
        // Just no-op for OCF sources
        trace!("source caching is not supported for Avro OCF sources");
    }

    fn read_file(_path: PathBuf) -> Vec<SourceMessage<Self>> {
        panic!("source caching is not supported for Avro OCF sources");
    }
}

#[derive(Debug)]
pub(crate) enum NextMessage<Out> {
    Ready(SourceMessage<Out>),
    Pending,
    TransientDelay,
    Finished,
}

/// Source-agnostic wrapper for messages. Each source must implement a
/// conversion to Message.
pub struct SourceMessage<Out> {
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
    /// Optional payload
    pub payload: Option<Out>,
}

impl<Out> fmt::Debug for SourceMessage<Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceMessage")
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("upstream_time_millis", &self.upstream_time_millis)
            .field("key[present]", &self.key.is_some())
            .field("payload[present]", &self.payload.is_some())
            .finish()
    }
}

/// Per-partition consistency information.
#[derive(Copy, Clone)]
struct ConsInfo {
    /// The timestamp we are currently aware of. This timestamp is open iff
    /// offset < current_upper_bound and closed otherwise.
    current_ts: Timestamp,
    /// Current upper bound for the current timestamp. All offsets <= upper_bound
    /// are assigned to current_ts.
    current_upper_bound: MzOffset,
    /// the last processed offset
    offset: MzOffset,
}

impl ConsInfo {
    fn new(timestamp: Timestamp) -> Self {
        Self {
            current_ts: timestamp,
            current_upper_bound: MzOffset { offset: 0 },
            offset: MzOffset { offset: 0 },
        }
    }

    fn update_timestamp(&mut self, timestamp: Timestamp, upper: MzOffset) {
        assert!(timestamp >= self.current_ts);
        assert!(upper >= self.current_upper_bound);

        self.current_upper_bound = upper;
        self.current_ts = timestamp;
    }

    fn update_offset(&mut self, offset: MzOffset) {
        assert!(offset >= self.offset);
        assert!(offset <= self.current_upper_bound);
        self.offset = offset;
    }

    fn get_closed_timestamp(&self) -> Timestamp {
        if self.current_ts == 0 || self.current_upper_bound == self.offset {
            return self.current_ts;
        }

        self.current_ts - 1
    }
}

/// Contains all necessary information that relates to consistency and timestamping.
/// This information is (and should remain) source independent. This covers consistency
/// information for sources that follow RT consistency and BYO consistency.
pub struct ConsistencyInfo {
    /// Last closed timestamp
    last_closed_ts: u64,
    /// Time since last capability downgrade
    time_since_downgrade: Instant,
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
        source_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        worker_count: usize,
        consistency: Consistency,
        timestamp_frequency: Duration,
        logger: Option<Logger>,
    ) -> ConsistencyInfo {
        ConsistencyInfo {
            last_closed_ts: 0,
            // Safe conversion: statement.rs checks that value specified fits in u64
            downgrade_capability_frequency: timestamp_frequency.as_millis().try_into().unwrap(),
            partition_metadata: HashMap::new(),
            source_type: consistency,
            source_metrics: SourceMetrics::new(
                &source_name,
                source_id,
                &worker_id.to_string(),
                logger,
            ),
            // we have never downgraded, so make sure the initial value is outside of our frequency
            time_since_downgrade: Instant::now() - timestamp_frequency - Duration::from_secs(1),
            source_id,
            active,
            worker_id,
            worker_count,
        }
    }

    /// Returns true if this worker is responsible for handling this partition
    fn responsible_for(&self, pid: &PartitionId) -> bool {
        match pid {
            PartitionId::File | PartitionId::S3 | PartitionId::Kinesis => self.active,
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

    /// Start tracking consistency information and metrics for `pid`.
    ///
    /// Need to call this together with `SourceReader::add_partition` before we
    /// ingest from `pid`.
    fn add_partition(&mut self, pid: &PartitionId) {
        if self.partition_metadata.contains_key(pid) {
            error!("Incorrectly attempting to add a partition twice for source: {} partion: {}. Ignoring",
                   self.source_id,
                   pid
            );

            return;
        }

        self.partition_metadata
            .insert(pid.clone(), ConsInfo::new(self.last_closed_ts));
        self.source_metrics.add_partition(pid);
    }

    /// Generates a timestamp that is guaranteed to be monotonically increasing.
    /// This may require multiple calls to the underlying now() system method, which is not
    /// guaranteed to increase monotonically
    fn generate_next_timestamp(&mut self) -> Option<u64> {
        let mut new_ts = 0;
        while new_ts <= self.last_closed_ts {
            let start = SystemTime::now();
            new_ts = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64;
            if new_ts < self.last_closed_ts && self.last_closed_ts - new_ts > 1000 {
                // If someone resets their system clock to be too far in the past, this could cause
                // Materialize to block and not give control to other operators. We thus give up
                // on timestamping this message
                error!("The new timestamp is more than 1 second behind the last assigned timestamp. To \
                avoid unnecessary blocking, Materialize will not attempt to downgrade the capability. Please \
                consider resetting your system time.");
                return None;
            }
        }
        assert!(new_ts > self.last_closed_ts);
        // Round new_ts to the next greatest self.downgrade_capability_frequency increment.
        // This is to guarantee that different workers downgrade (without coordination) to the
        // "same" next time
        new_ts = new_ts
            + (self.downgrade_capability_frequency
                - (new_ts % self.downgrade_capability_frequency));
        self.last_closed_ts = new_ts;
        Some(self.last_closed_ts)
    }

    /// Timestamp history map is of format [pid1: (p_ct, ts1, offset1), (p_ct, ts2, offset2), pid2: (p_ct, ts1, offset)...].
    /// For a given partition pid, messages in interval \[0,offset1\] get assigned ts1, all messages in interval \[offset1+1,offset2\]
    /// get assigned ts2, etc.
    /// When receive message with offset1, it is safe to downgrade the capability to the next
    /// timestamp, which is either
    /// 1) the timestamp associated with the next highest offset if it exists
    /// 2) max(timestamp, offset1) + 1. The timestamp_history map can contain multiple timestamps for
    /// the same offset. We pick the greatest one + 1
    /// (the next message we generate will necessarily have timestamp timestamp + 1)
    ///
    /// This method assumes that timestamps are inserted in increasing order in the hashmap
    /// (even across partitions). This means that once we see a timestamp with ts x, no entry with
    /// ts (x-1) will ever be inserted. Entries with timestamp x might still be inserted in different
    /// partitions. This is guaranteed by the `coord::timestamp::is_ts_valid` method.
    ///
    fn downgrade_capability<Out: Send + Clone + 'static>(
        &mut self,
        id: &SourceInstanceId,
        cap: &mut Capability<Timestamp>,
        source: &mut dyn SourceReader<Out>,
        timestamp_histories: &TimestampDataUpdates,
        timestamp_bindings: &mut TimestampBindingRc,
    ) {
        //  We need to determine the maximum timestamp that is fully closed. This corresponds to the minimum of
        //  * closed timestamps across all partitions we own
        //  * maximum bound timestamps across all partitions we don't own (the `upper`)
        let mut upper = Antichain::new();
        timestamp_bindings.read_upper(&mut upper);
        let mut min: Option<Timestamp> = if let Some(time) = upper.elements().get(0) {
            Some(*time)
        } else {
            None
        };

        if let Consistency::BringYourOwn(_) = self.source_type {
            // Determine which timestamps have been closed. A timestamp is closed once we have processed
            // all messages that we are going to process for this timestamp across all partitions that the
            // worker knows about (i.e. the ones the worker has been assigned to read from).
            // In practice, the following happens:
            // Per partition, we iterate over the data structure to remove (ts,offset) mappings for which
            // we have seen all records <= offset. We keep track of the last "closed" timestamp in that partition
            // in next_partition_ts

            for pid in timestamp_bindings.partitions() {
                if !self.knows_of(&pid) && self.responsible_for(&pid) {
                    source.add_partition(pid.clone());
                    self.add_partition(&pid);
                }

                if self.knows_of(&pid) {
                    let closed_ts = self
                        .partition_metadata
                        .get(&pid)
                        .expect("partition known to exist")
                        .get_closed_timestamp();
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
            }
            let min = min.unwrap_or(0);

            // Downgrade capability to new minimum open timestamp (which corresponds to min + 1).
            if min > self.last_closed_ts {
                self.source_metrics.capability.set(min);
                cap.downgrade(&(&min + 1));
                self.last_closed_ts = min;

                let new_compaction_frontier = Antichain::from_elem(min);
                timestamp_bindings.set_compaction_frontier(new_compaction_frontier.borrow());
            }
        } else {
            // This a RT source. It is always possible to close the timestamp and downgrade the
            // capability
            if let Some(entries) = timestamp_histories.borrow().get(&id.source_id) {
                match entries {
                    TimestampDataUpdate::RealTime(partitions) => {
                        for pid in partitions {
                            if !self.knows_of(pid) && self.responsible_for(pid) {
                                source.add_partition(pid.clone());
                                self.add_partition(pid);
                            }
                        }
                    }
                    _ => panic!("Unexpected timestamp message. Expected RT update."),
                }
            }

            if self.time_since_downgrade.elapsed().as_millis()
                > self.downgrade_capability_frequency.try_into().unwrap()
            {
                let ts = self.generate_next_timestamp();
                if let Some(ts) = ts {
                    self.source_metrics.capability.set(ts);
                    cap.downgrade(&(&ts + 1));
                }
                self.time_since_downgrade = Instant::now();
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
        if let Consistency::RealTime = self.source_type {
            // Simply assign to this message the next timestamp that is not closed
            Some(self.find_matching_rt_timestamp())
        } else {
            // The source is a BYO source. We either can take a fast path, where we simply re-use the currently
            // available timestamp binding, or if one isn't available for `offset` in `partition`, we have to
            // look it up from the set of timestamp histories

            // We know we will only read from partitions already assigned to this
            // worker.
            let cons_info = self
                .partition_metadata
                .get_mut(partition)
                .expect("known to exist");

            if cons_info.current_upper_bound >= offset {
                // This is the fast path - we can reuse a timestamp binding
                // we already know about.
                cons_info.update_offset(offset);
                Some(cons_info.current_ts)
            } else {
                if let Some((timestamp, max_offset)) =
                    timestamp_bindings.get_binding(partition, offset)
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

    /// For a given record from a RT source, find the timestamp that is not closed
    fn find_matching_rt_timestamp(&self) -> Timestamp {
        self.last_closed_ts + 1
    }
}

/// Source-specific Prometheus metrics
pub struct SourceMetrics {
    /// Number of times an operator gets scheduled
    operator_scheduled_counter: IntCounter,
    /// Value of the capability associated with this source
    capability: UIntGauge,
    /// Per-partition Prometheus metrics.
    pub partition_metrics: HashMap<PartitionId, PartitionMetrics>,
    logger: Option<Logger>,
    source_name: String,
    source_id: SourceInstanceId,
}

impl SourceMetrics {
    /// Initialises source metrics for a given (source_id, worker_id)
    pub fn new(
        source_name: &str,
        source_id: SourceInstanceId,
        worker_id: &str,
        logger: Option<Logger>,
    ) -> SourceMetrics {
        let source_id_string = source_id.to_string();
        lazy_static! {
            static ref OPERATOR_SCHEDULED_COUNTER: IntCounterVec = register_int_counter_vec!(
                "mz_operator_scheduled_total",
                "The number of times the kafka client got invoked for this source",
                &["topic", "source_id", "worker_id"]
            )
            .unwrap();
            static ref CAPABILITY: UIntGaugeVec = register_uint_gauge_vec!(
                "mz_capability",
                "The current capability for this dataflow. This corresponds to min(mz_partition_closed_ts)",
                &["topic", "source_id", "worker_id"]
            )
            .unwrap();
        }
        let labels = &[source_name, &source_id_string, worker_id];
        SourceMetrics {
            operator_scheduled_counter: OPERATOR_SCHEDULED_COUNTER.with_label_values(labels),
            capability: CAPABILITY.with_label_values(labels),
            partition_metrics: Default::default(),
            logger,
            source_name: source_name.to_string(),
            source_id,
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

        let metric = PartitionMetrics::new(&self.source_name, self.source_id, partition_id);
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
                    partition_id: partition.to_string(),
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
    offset_ingested: DeleteOnDropGauge<'static, AtomicI64>,
    /// Highest offset that has been received by the source
    offset_received: DeleteOnDropGauge<'static, AtomicI64>,
    /// Value of the highest timestamp that is closed (for which all messages have been ingested)
    closed_ts: DeleteOnDropGauge<'static, AtomicU64>,
    /// Total number of messages that have been received by the source and timestamped
    messages_ingested: DeleteOnDropCounter<'static, AtomicI64>,
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
            partition_id: partition_id.to_string(),
            offset: offset - self.last_offset,
            timestamp: timestamp - self.last_timestamp,
        });
        self.last_offset = offset;
        self.last_timestamp = timestamp;
    }

    /// Initialises partition metrics for a given (source_id, partition_id)
    pub fn new(
        source_name: &str,
        source_id: SourceInstanceId,
        partition_id: &PartitionId,
    ) -> PartitionMetrics {
        const LABELS: &[&str] = &["topic", "source_id", "source_instance", "partition_id"];
        lazy_static! {
            static ref OFFSET_INGESTED: IntGaugeVec = register_int_gauge_vec!(
                "mz_partition_offset_ingested",
                "The most recent offset that we have ingested into a dataflow. This correspond to \
                data that we have 1)ingested 2) assigned a timestamp",
                LABELS
            )
            .unwrap();
            static ref OFFSET_RECEIVED: IntGaugeVec = register_int_gauge_vec!(
                "mz_partition_offset_received",
                "The most recent offset that we have been received by this source.",
                LABELS
            )
            .unwrap();
            static ref CLOSED_TS: UIntGaugeVec = register_uint_gauge_vec!(
                "mz_partition_closed_ts",
                "The highest closed timestamp for each partition in this dataflow",
                LABELS
            )
            .unwrap();
            static ref MESSAGES_INGESTED: IntCounterVec = register_int_counter_vec!(
                "mz_messages_ingested",
                "The number of messages ingested per partition.",
                LABELS
            )
            .unwrap();
        }
        let labels = &[
            source_name,
            &source_id.source_id.to_string(),
            &source_id.dataflow_id.to_string(),
            &partition_id.to_string(),
        ];
        PartitionMetrics {
            offset_ingested: DeleteOnDropGauge::new_with_error_handler(
                OFFSET_INGESTED.with_label_values(labels),
                &OFFSET_INGESTED,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            offset_received: DeleteOnDropGauge::new_with_error_handler(
                OFFSET_RECEIVED.with_label_values(labels),
                &OFFSET_RECEIVED,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            closed_ts: DeleteOnDropGauge::new_with_error_handler(
                CLOSED_TS.with_label_values(labels),
                &CLOSED_TS,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            messages_ingested: DeleteOnDropCounter::new_with_error_handler(
                MESSAGES_INGESTED.with_label_values(labels),
                &MESSAGES_INGESTED,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
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
}

impl Timestamper {
    fn new(sender: EventSender, tick_duration: Duration) -> Self {
        let now = UNIX_EPOCH
            .elapsed()
            .expect("system clock before 1970")
            .as_millis()
            .try_into()
            .expect("materialize has existed for more than 500M years");
        Self {
            inner: Arc::new(RwLock::new(now)),
            sender,
            tick_duration,
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
        let mut now = UNIX_EPOCH
            .elapsed()
            .expect("system clock before 1970")
            .as_millis();

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
        name,
        scope,
        active,
        timestamp_frequency,
        ..
    } = config;

    let (tx, mut rx) = mpsc::channel(64);

    if active {
        tokio::spawn(async move {
            let timestamper = Timestamper::new(tx, timestamp_frequency);
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

    let (stream, capability) = source(scope, name, move |info| {
        let activator = Arc::new(scope.sync_activator_for(&info.address[..]));

        move |cap, output| {
            if !active {
                return SourceStatus::Done;
            }

            let waker = futures::task::waker_ref(&activator);
            let mut context = Context::from_waker(&waker);

            while let Poll::Ready(item) = rx.poll_recv(&mut context) {
                match item {
                    Some(Event::Progress(None)) => unreachable!(),
                    Some(Event::Progress(Some(time))) => {
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

    (stream.map_fallible(|r| r), Some(capability))
}

/// Creates a source dataflow operator. The type of ExternalSourceConnector determines the
/// type of source that should be created
pub(crate) fn create_source<G, S: 'static, Out>(
    config: SourceConfig<G>,
    source_connector: &ExternalSourceConnector,
) -> (
    (
        timely::dataflow::Stream<G, SourceOutput<Vec<u8>, Out>>,
        timely::dataflow::Stream<G, SourceError>,
    ),
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader<Out>,
    Out: Debug + Clone + Send + Default + MaybeLength + SourceCache + 'static,
{
    let SourceConfig {
        name,
        id,
        scope,
        timestamp_histories,
        worker_id,
        worker_count,
        consistency,
        timestamp_frequency,
        active,
        encoding,
        mut caching_tx,
        logger,
        ..
    } = config;
    let (stream, capability) = source(scope, name.clone(), move |info| {
        // Create activator for source
        let activator = scope.activator_for(&info.address[..]);

        // Create control plane information (Consistency-related information)
        let mut consistency_info = ConsistencyInfo::new(
            active,
            name.clone(),
            id,
            worker_id,
            worker_count,
            consistency,
            timestamp_frequency,
            logger.clone(),
        );

        // Create source information (this function is specific to a specific
        // source

        let mut source_reader: Option<S> = if !active {
            None
        } else {
            match SourceReader::<Out>::new(
                name.clone(),
                id,
                worker_id,
                scope.sync_activator_for(&info.address[..]),
                source_connector.clone(),
                encoding,
                logger,
            ) {
                Ok((source_reader, partition)) => {
                    if let Some(pid) = partition {
                        consistency_info.add_partition(&pid);
                    }

                    Some(source_reader)
                }

                Err(e) => {
                    error!("Failed to create source: {}", e);
                    None
                }
            }
        };

        let cached_files = dataflow_types::cached_files(source_connector);
        let mut cached_files = crate::source::cache::cached_files_for_worker(
            id.source_id,
            cached_files,
            &consistency_info,
        );

        let mut timestamp_bindings = if let TimestampDataUpdate::BringYourOwn(history) =
            timestamp_histories
                .borrow_mut()
                .get(&id.source_id)
                .expect("known to exist")
        {
            history.clone()
        } else {
            TimestampBindingRc::new()
        };

        let mut predecessor = None;
        // Stash messages we cannot yet timestamp here.
        let mut buffer = None;

        move |cap, output| {
            // First check that the source was successfully created
            let source_reader = match &mut source_reader {
                Some(source_reader) => source_reader,
                None => {
                    return SourceStatus::Done;
                }
            };

            // Downgrade capability (if possible)
            consistency_info.downgrade_capability(
                &id,
                cap,
                source_reader,
                &timestamp_histories,
                &mut timestamp_bindings,
            );

            if let Some(file) = cached_files.pop() {
                // TODO(rkhaitan) change this to properly re-use old timestamps.
                // Currently this is hard to do because there can be arbitrary delays between
                // different workers being scheduled, and this means that all cached state
                // can potentially get pulled into memory without being able to close timestamps
                // which causes the system to go out of memory.
                // For now, constrain we constrain caching to RT sources, and we re-assign
                // timestamps to cached messages on startup.
                let ts = consistency_info.find_matching_rt_timestamp();
                let ts_cap = cap.delayed(&ts);

                let messages = SourceCache::read_file(file);

                for message in messages {
                    let key = message.key.unwrap_or_default();
                    let out = message.payload.unwrap_or_default();
                    output.session(&ts_cap).give(Ok(SourceOutput::new(
                        key,
                        out,
                        Some(message.offset.offset),
                        message.upstream_time_millis,
                    )));
                }
                // Yield to give downstream operators time to handle this data.
                activator.activate_after(Duration::from_millis(10));
                return SourceStatus::Alive;
            }

            // Bound execution of operator to prevent a single operator from hogging
            // the CPU if there are many messages to process
            let timer = Instant::now();
            // Accumulate updates to BYTES_READ_COUNTER for Prometheus metrics collection
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
                    handle_message(
                        message,
                        &mut predecessor,
                        &mut consistency_info,
                        &id,
                        &mut bytes_read,
                        &mut caching_tx,
                        &cap,
                        output,
                        &mut metric_updates,
                        &timer,
                        &mut buffer,
                        &timestamp_bindings,
                    )
                } else {
                    match source_reader.get_next_message() {
                        Ok(NextMessage::Ready(message)) => handle_message(
                            message,
                            &mut predecessor,
                            &mut consistency_info,
                            &id,
                            &mut bytes_read,
                            &mut caching_tx,
                            &cap,
                            output,
                            &mut metric_updates,
                            &timer,
                            &mut buffer,
                            &timestamp_bindings,
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

            BYTES_READ_COUNTER.inc_by(bytes_read as i64);
            consistency_info
                .source_metrics
                .record_partition_offsets(metric_updates);

            // Downgrade capability (if possible) before exiting
            consistency_info.downgrade_capability(
                &id,
                cap,
                source_reader,
                &timestamp_histories,
                &mut timestamp_bindings,
            );

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

    let (ok_stream, err_stream) = stream.map_fallible(|r| r.map_err(SourceError::FileIO));

    if active {
        ((ok_stream, err_stream), Some(capability))
    } else {
        // Immediately drop the capability if worker is not an active reader for source
        ((ok_stream, err_stream), None)
    }
}

/// Take `message` and assign it the appropriate timestamps and push it into the
/// dataflow layer, if possible.
///
/// TODO: This function is a bit of a mess rn but hopefully this function makes the
/// existing mess more obvious and points towards ways to improve it.
fn handle_message<Out>(
    message: SourceMessage<Out>,
    predecessor: &mut Option<MzOffset>,
    consistency_info: &mut ConsistencyInfo,
    id: &SourceInstanceId,
    bytes_read: &mut usize,
    caching_tx: &mut Option<mpsc::UnboundedSender<CacheMessage>>,
    cap: &Capability<Timestamp>,
    output: &mut OutputHandle<
        Timestamp,
        Result<SourceOutput<Vec<u8>, Out>, String>,
        Tee<Timestamp, Result<SourceOutput<Vec<u8>, Out>, String>>,
    >,
    metric_updates: &mut HashMap<PartitionId, (MzOffset, Timestamp)>,
    timer: &std::time::Instant,
    buffer: &mut Option<SourceMessage<Out>>,
    timestamp_bindings: &TimestampBindingRc,
) -> (SourceStatus, MessageProcessing)
where
    Out: Debug + Clone + Send + Default + MaybeLength + SourceCache + 'static,
{
    let partition = message.partition.clone();
    let offset = message.offset;
    let msg_predecessor = *predecessor;
    *predecessor = Some(offset);

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
            SourceCache::cache_message(&message, *id, caching_tx, ts, msg_predecessor);
            // Note: empty and null payload/keys are currently
            // treated as the same thing.
            let key = message.key.unwrap_or_default();
            let out = message.payload.unwrap_or_default();
            // Entry for partition_metadata is guaranteed to exist as messages
            // are only processed after we have updated the partition_metadata for a
            // partition and created a partition queue for it.
            *bytes_read += key.len();
            *bytes_read += out.len().unwrap_or(0);
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
