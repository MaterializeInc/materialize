// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow sources.

use mz_avro::types::Value;
use mz_dataflow_types::sources::AwsExternalId;
use mz_dataflow_types::{DecodeError, SourceErrorDetails};
use mz_persist::client::{StreamReadHandle, StreamWriteHandle};
use mz_persist::operators::stream::Persist;
use mz_persist_types::Codec;
use mz_repr::MessagePayload;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::hash_map::Entry;
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
    operators::{Capability, CapabilitySet, Event},
};

use anyhow::anyhow;
use async_trait::async_trait;
use mz_dataflow_types::{
    sources::{encoding::SourceDataEncoding, ExternalSourceConnector, MzOffset},
    SourceError,
};
use mz_expr::{GlobalId, PartitionId, SourceInstanceId};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::now::NowFn;
use mz_ore::task;
use prometheus::core::{AtomicI64, AtomicU64};
use tracing::{debug, error, trace};

use mz_repr::{Diff, Row, Timestamp};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::{operator, OutputHandle};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::scheduling::activate::{Activator, SyncActivator};
use timely::Data;
use tokio::sync::{mpsc, RwLock, RwLockReadGuard};

use self::metrics::SourceBaseMetrics;

use super::source::util::source;
use crate::common::operator::StreamExt;
use crate::storage::source::timestamp::TimestampBindingRc;
use crate::storage::source::timestamp::TimestampBindingUpdater;
use crate::storage::source::timestamp::{AssignedTimestamp, SourceTimestamp};
use crate::storage::{Logger, StorageEvent};

mod file;
mod gen;
mod kafka;
mod kinesis;
pub mod metrics;
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
const YIELD_INTERVAL: Duration = Duration::from_millis(10);

/// Shared configuration information for all source types.
pub struct SourceConfig<'a, G> {
    /// The name to attach to the underlying timely operator.
    pub name: String,
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
    /// An external ID to use for all AWS AssumeRole operations.
    pub aws_external_id: AwsExternalId,
}

/// A record produced by a source
#[derive(Clone, Serialize, Debug, Deserialize)]
pub struct SourceOutput<K, V>
where
    K: Data,
    V: Data,
{
    /// The record's key (or some empty/default value for sources without the concept of key)
    pub key: K,
    /// The record's value
    pub value: V,
    /// The position in the partition described by the `partition` in the source
    /// (e.g., Kafka offset, file line number, monotonic increasing
    /// number, etc.)
    pub position: i64,
    /// The time the record was created in the upstream systsem, as milliseconds since the epoch
    pub upstream_time_millis: Option<i64>,
    /// The partition of this message, present iff the partition comes from Kafka
    pub partition: PartitionId,
    /// Headers, if the source is configured to pass them along. If it is, but there are none, it
    /// passes `Some([])`
    pub headers: Option<Vec<(String, Option<Vec<u8>>)>>,
}

/// The output of the decoding operator
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct DecodeResult {
    /// The decoded key
    pub key: Option<Result<Row, DecodeError>>,
    /// The decoded value
    pub value: Option<Result<Row, DecodeError>>,
    /// The index of the decoded value in the stream
    pub position: i64,
    /// The time the record was created in the upstream systsem, as milliseconds since the epoch
    pub upstream_time_millis: Option<i64>,
    /// The partition this record came from
    pub partition: PartitionId,
    /// If this is a Kafka stream, the appropriate metadata
    // TODO(bwm): This should probably be statically different for different streams, or we should
    // propagate whether metadata is requested into the decoder
    pub metadata: Row,
}

/// Kafka-specific information about the event
#[derive(Debug, Default, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct KafkaMetadata {
    /// The Partition within the Kafka Topic
    pub partition: i32,
    /// The message offset within the given partition
    pub offset: i64,
    /// The message timestamp, expressed as a Unix Milliseconds Timestamp
    pub timestamp: i64,
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
        position: i64,
        upstream_time_millis: Option<i64>,
        partition: PartitionId,
        headers: Option<Vec<(String, Option<Vec<u8>>)>>,
    ) -> SourceOutput<K, V> {
        SourceOutput {
            key,
            value,
            position,
            upstream_time_millis,
            partition,
            headers,
        }
    }
}
impl<K, V> SourceOutput<K, V>
where
    K: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
    V: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
{
    /// A parallelization contract that hashes by positions (if available)
    /// and otherwise falls back to hashing by value. Values can be just as
    /// skewed as keys, whereas positions are generally known to be unique or
    /// close to unique in a source. For example, Kafka offsets are unique per-partition.
    /// Most decode logic should use this instead of `key_contract`.
    pub fn position_value_contract() -> impl ParallelizationContract<Timestamp, Self>
    where
        V: Hashable<Output = u64>,
    {
        Exchange::new(|x: &Self| x.position.hashed())
    }
}

/// A `SourceToken` manages interest in a source.
///
/// When the `SourceToken` is dropped the associated source will be stopped.
pub struct SourceToken {
    capabilities: Rc<
        RefCell<
            Option<(
                Capability<Timestamp>,
                Capability<Timestamp>,
                CapabilitySet<Timestamp>,
            )>,
        >,
    >,
    activator: Activator,
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

impl MaybeLength for () {
    fn len(&self) -> Option<usize> {
        None
    }

    fn is_empty(&self) -> bool {
        true
    }
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
    type Key: timely::Data + MaybeLength;
    type Value: timely::Data + MaybeLength;

    /// Create a new source reader.
    ///
    /// This function returns the source reader and optionally, any "partition" it's
    /// already reading. In practice, the partition is only non-None for static sources
    /// that either don't truly have partitions or have a fixed number of partitions.
    fn new(
        source_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        aws_external_id: AwsExternalId,
        restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        encoding: SourceDataEncoding,
        logger: Option<Logger>,
        metrics: crate::storage::source::metrics::SourceBaseMetrics,
    ) -> Result<Self, anyhow::Error>
    where
        Self: Sized;

    /// Returns the next message available from the source.
    ///
    /// Note that implementers are required to present messages in strictly ascending\
    /// offset order within each partition.
    fn get_next_message(&mut self) -> Result<NextMessage<Self::Key, Self::Value>, anyhow::Error>;
}

pub(crate) enum NextMessage<Key, Value> {
    Ready(SourceMessage<Key, Value>),
    Pending,
    TransientDelay,
    Finished,
}

/// Source-agnostic wrapper for messages. Each source must implement a
/// conversion to Message.
pub struct SourceMessage<Key, Value> {
    /// Partition from which this message originates
    pub partition: PartitionId,
    /// Materialize offset of the message (1-indexed)
    pub offset: MzOffset,
    /// The time that an external system first observed the message
    ///
    /// Milliseconds since the unix epoch
    pub upstream_time_millis: Option<i64>,
    /// The message key
    pub key: Key,
    /// The message value
    pub value: Value,
    /// Headers, if the source is configured to pass them along. If it is, but there are none, it
    /// passes `Some([])`
    pub headers: Option<Vec<(String, Option<Vec<u8>>)>>,
}

impl fmt::Debug for SourceMessage<(), MessagePayload> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceMessage")
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("upstream_time_millis", &self.upstream_time_millis)
            .finish()
    }
}

impl fmt::Debug for SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceMessage")
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("upstream_time_millis", &self.upstream_time_millis)
            .field("key[present]", &self.key.is_some())
            .field("value[present]", &self.value.is_some())
            .finish()
    }
}

/// Returns true if the given source id/worker id is responsible for handling the given
/// partition.
pub fn responsible_for(
    source_id: &GlobalId,
    worker_id: usize,
    worker_count: usize,
    pid: &PartitionId,
) -> bool {
    match pid {
        PartitionId::None => {
            // All workers are responsible for reading in Kafka sources. Other sources
            // support single-threaded ingestion only. Note that in all cases we want all
            // readers of the same source or same partition to reside on the same worker,
            // and only load-balance responsibility across distinct sources.
            (usize::cast_from(source_id.hashed()) % worker_count) == worker_id
        }
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
            let hash = (source_id.hashed() >> 32) + *p as u64;
            (hash % worker_count as u64) == worker_id as u64
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

    /// Log updates to which offsets / timestamps read up to.
    pub fn record_partition_offsets(
        &mut self,
        offsets: HashMap<PartitionId, (MzOffset, Timestamp, i64)>,
    ) {
        if self.logger.is_none() {
            return;
        }

        for (partition, (offset, timestamp, count)) in offsets {
            let metric = self
                .partition_metrics
                .entry(partition.clone())
                .or_insert_with(|| {
                    PartitionMetrics::new(
                        &self.base_metrics,
                        &self.source_name,
                        self.source_id,
                        &partition,
                    )
                });

            metric.messages_ingested.inc_by(count);

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

impl Drop for SourceMetrics {
    fn drop(&mut self) {
        // retract our partition from logging
        if let Some(logger) = self.logger.as_mut() {
            for (partition, metric) in self.partition_metrics.iter() {
                logger.log(StorageEvent::SourceInfo {
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
        logger.log(StorageEvent::SourceInfo {
            source_name: source_name.to_string(),
            source_id,
            partition_id: partition_id.into(),
            offset: offset - self.last_offset,
            timestamp: timestamp - self.last_timestamp,
        });

        self.offset_received.set(offset);
        self.offset_ingested.set(offset);
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
    #[allow(unused)]
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
        task::spawn(
            || format!("source_simple_timestamper:{}", id.source_id),
            async move {
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
            },
        );
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

        move |cap,
              _secondary_cap: &mut Capability<Timestamp>,
              durability_cap: &mut CapabilitySet<Timestamp>,
              output,
              _secondary_output: &mut OutputHandle<Timestamp, (), _>| {
            if !active {
                return SourceStatus::Done;
            }

            durability_cap.downgrade(Vec::<Timestamp>::new());

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
                                1,
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
/// example using [`seal`](mz_persist::operators::stream::Seal::seal).
pub(crate) fn create_source<G, S: 'static>(
    config: SourceConfig<G>,
    source_connector: &ExternalSourceConnector,
    persist_config: Option<PersistentTimestampBindingsConfig<SourceTimestamp, AssignedTimestamp>>,
    aws_external_id: AwsExternalId,
) -> (
    (
        timely::dataflow::Stream<G, SourceOutput<S::Key, S::Value>>,
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
        upstream_name,
        id,
        scope,
        mut timestamp_histories,
        worker_id,
        worker_count,
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

    // If we have persisted data, we initialize the starting timestamp to the latest upper seal
    // timestamp, such that we can start emitting data right at the time where it will become
    // sealed when we next downgrade the capability. We do this to ensure that necessary
    // retractions are considered valid on a future restore attempt.
    let (source_persist, restored_bindings, mut ts_bindings_retractions, starting_ts) =
        match persist_config {
            Some(persist_config) => {
                let source_persist =
                    SourceReaderPersistence::new(name.clone(), persist_config.clone());

                let timestamp_histories = timestamp_histories.as_mut().ok_or_else(|| {
                    SourceError::new(
                        id,
                        SourceErrorDetails::Persistence("missing timestamp histories".to_owned()),
                    )
                });

                let result = timestamp_histories.and_then(|timestamp_histories| {
                let (mut valid_bindings, mut retractions) = source_persist.restore().map_err(|e| {
                    SourceError::new(
                        id,
                        SourceErrorDetails::Persistence(format!(
                            "restoring timestamp bindings: {}",
                            e
                        )),
                    )
                })?;

                // Only retain bindings/retractions that this worker is responsible for. If we
                // weren't doing this, we would get a corrupted bindings updates in the output,
                // where diffs don't sum up to `1` or older bindings might overwrite newer ones.
                valid_bindings.retain(|(source_ts, _assigned_ts)| {
                    crate::storage::source::responsible_for(
                        &id.source_id,
                        worker_id,
                        worker_count,
                        &source_ts.partition)
                });
                retractions.retain(|((source_ts, _assigned_ts), _diff)| {
                    crate::storage::source::responsible_for(
                        &id.source_id,
                        worker_id,
                        worker_count,
                        &source_ts.partition)
                });

                let starting_offsets = source_persist.get_starting_offsets(valid_bindings.iter());

                debug!(
                    "In {}, initial (restored) source offsets: {:?}. upper_seal_ts = {}",
                    name,
                    starting_offsets,
                    source_persist.config.upper_seal_ts,
                );

                debug!(
                    "In {}, initial (restored) timestamp bindings: valid_bindings: {:?}, retractions: {:?}",
                    name,
                    valid_bindings, retractions
                );

                for (pid, offset) in starting_offsets {
                    timestamp_histories.add_partition(pid, Some(offset));
                }

                // We need to sort by offset and then timestamp because `add_binding()` will not allow
                // adding bindings that go "backwards".
                valid_bindings.sort_by(|a, b| (a.0.offset.offset, a.1).cmp(&(b.0.offset.offset, b.1)));

                for (source_ts, assigned_ts) in valid_bindings.iter() {

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
                    let lower = Antichain::from_elem(assigned_ts.0);
                    let upper = Antichain::new();
                    // NOTE: We use get_bindings_in_range() and not the seemingly better
                    // get_binding(). However, get_binding() does not only consult the actual
                    // stored bindings but also checks the Proposer, and returns its output as a
                    // binding if it doesn't have a stored binding. We only care about stored
                    // bindings here.
                    let existing_binding = timestamp_histories
                        .get_bindings_in_range(lower.borrow(), upper.borrow()).into_iter()
                        .filter(|(pid, _ts, offset)| *pid == source_ts.partition && *offset >= source_ts.offset).next();
                    match existing_binding {
                        None => {
                            timestamp_histories.add_binding(
                                source_ts.partition.clone(),
                                assigned_ts.0,
                                source_ts.offset,
                            );
                        },
                        Some(existing_binding) => {
                            debug!(
                                "Filtered out timestamp binding {:?} from persistence because we already have {:?}.",
                                (source_ts, assigned_ts),
                                existing_binding
                            );
                        }
                    }
                }

                Ok((Some(source_persist), Some(valid_bindings), Some(retractions), persist_config.upper_seal_ts))
            });

                match result {
                    Ok(result) => result,
                    Err(e) => {
                        construction_errs.push(e);
                        (None, None, None, 0)
                    }
                }
            }
            None => (None, None, None, 0),
        };

    let bytes_read_counter = base_metrics.bytes_read.clone();

    let should_emit_timestamp_bindings = source_persist.is_some();

    let (stream, ts_bindings_stream, capability) = source(scope, name.clone(), move |info| {
        // Create activator for source
        let activator = scope.activator_for(&info.address[..]);

        // This source will need to be activated when the durability frontier changes.
        if let Some(wrapper) = timestamp_histories.as_mut() {
            let durability_activator = scope.activator_for(&info.address[..]);
            wrapper
                .wrapper
                .borrow_mut()
                .activators
                .push(durability_activator);
        }

        let metrics_name = upstream_name.unwrap_or_else(|| name.clone());
        let mut source_metrics = SourceMetrics::new(
            base_metrics,
            &metrics_name,
            id,
            &worker_id.to_string(),
            logger.clone(),
        );
        let restored_offsets = timestamp_histories
            .as_mut()
            .map(|ts| ts.partitions())
            .unwrap_or_default();
        let mut partition_cursors: HashMap<_, _> = restored_offsets
            .iter()
            .cloned()
            .flat_map(|(pid, offset)| Some((pid, offset?)))
            .collect();

        let mut source_reader: Option<S> = if !active {
            None
        } else {
            match SourceReader::new(
                name.clone(),
                id,
                worker_id,
                worker_count,
                scope.sync_activator_for(&info.address[..]),
                source_connector.clone(),
                aws_external_id.clone(),
                restored_offsets,
                encoding,
                logger,
                base_metrics.clone(),
            ) {
                Ok(source_reader) => Some(source_reader),
                Err(e) => {
                    error!("Failed to create source: {}", e);
                    None
                }
            }
        };

        let mut timestamp_bindings_updater = if should_emit_timestamp_bindings {
            let restored_bindings = restored_bindings.expect("missing restored bindings");
            Some(TimestampBindingUpdater::new(restored_bindings))
        } else {
            None
        };

        move |cap, bindings_cap, durability_cap, output, bindings_output| {
            // First check that the source was successfully created
            let source_reader = match &mut source_reader {
                Some(source_reader) => source_reader,
                None => {
                    return SourceStatus::Done;
                }
            };

            // Check that we have a valid list of timestamp bindings.
            let timestamp_histories = match &mut timestamp_histories {
                Some(histories) => histories,
                None => {
                    error!("Source missing list of timestamp bindings");
                    return SourceStatus::Done;
                }
            };

            // Maintain a capability set that tracks the durability frontier.
            durability_cap.downgrade(timestamp_histories.durability_frontier());

            // NOTE: It's **very** important that we get out any necessary
            // retractions/additions to the timestamp bindings before we downgrade beyond the
            // previous upper seal frontier. Otherwise, it can happen that a needed retraction
            // is not considered valid on a future restart attempt, and we will get an
            // inconsistency between the persisted timestamp bindings and persisted data.
            if let Some(ts_bindings_retractions) = ts_bindings_retractions.take() {
                assert_eq!(
                    *bindings_cap.time(),
                    0,
                    "did not emit retractions at the earliest possible time: source capability is already at {}", bindings_cap.time()
                );

                bindings_cap.downgrade(&starting_ts);

                let mut session = bindings_output.session(&bindings_cap);

                let retraction_ts = starting_ts;
                let ts_bindings_retractions = ts_bindings_retractions
                    .into_iter()
                    .map(|(binding, diff)| (binding, retraction_ts, diff));

                session.give_iterator(ts_bindings_retractions);
            }

            // Bound execution of operator to prevent a single operator from hogging
            // the CPU if there are many messages to process
            let timer = Instant::now();
            // Accumulate updates to bytes_read for Prometheus metrics collection
            let mut bytes_read = 0;
            // Accumulate updates to offsets for system table metrics collection
            let mut metric_updates = HashMap::new();

            // Record operator has been scheduled
            source_metrics.operator_scheduled_counter.inc();

            let mut source_state = (SourceStatus::Alive, MessageProcessing::Active);
            while let (_, MessageProcessing::Active) = source_state {
                source_state = match source_reader.get_next_message() {
                    Ok(NextMessage::Ready(message)) => {
                        partition_cursors.insert(message.partition.clone(), message.offset + 1);
                        handle_message::<S>(
                            message,
                            &mut bytes_read,
                            &cap,
                            output,
                            &mut metric_updates,
                            &timer,
                            &timestamp_histories,
                        )
                    }
                    Ok(NextMessage::TransientDelay) => {
                        // There was a temporary hiccup in getting messages, check again asap.
                        (SourceStatus::Alive, MessageProcessing::Yielded)
                    }
                    Ok(NextMessage::Pending) => {
                        // There were no new messages, check again after a delay
                        (SourceStatus::Alive, MessageProcessing::YieldedWithDelay)
                    }
                    Ok(NextMessage::Finished) => (SourceStatus::Done, MessageProcessing::Stopped),
                    Err(e) => {
                        output.session(&cap).give(Err(e.to_string()));
                        (SourceStatus::Done, MessageProcessing::Stopped)
                    }
                };
            }

            bytes_read_counter.inc_by(bytes_read as u64);
            source_metrics.record_partition_offsets(metric_updates);

            // Attempt to update the timestamp and finalize the currently pending bindings
            let cur_ts = timestamp_histories.upper();
            timestamp_histories.update_timestamp();
            if timestamp_histories.upper() > cur_ts {
                for (_, partition_metrics) in source_metrics.partition_metrics.iter_mut() {
                    partition_metrics.closed_ts.set(cur_ts);
                }
            }

            // Emit any new timestamp bindings that we might have since we last emitted.
            maybe_emit_timestamp_bindings(
                &name,
                worker_id,
                &mut timestamp_bindings_updater,
                timestamp_histories,
                bindings_cap,
                bindings_output,
            );

            // Downgrade capability (if possible) before exiting
            timestamp_histories.downgrade(cap, &partition_cursors);
            bindings_cap.downgrade(cap.time());
            source_metrics.capability.set(*cap.time());
            // Downgrade compaction frontier to track the current time.
            timestamp_histories.set_compaction_frontier(Antichain::from_elem(*cap.time()).borrow());

            let (source_status, processing_status) = source_state;
            // Schedule our next activation
            match processing_status {
                MessageProcessing::Yielded => activator.activate(),
                MessageProcessing::YieldedWithDelay => {
                    activator.activate_after(timestamp_frequency)
                }
                _ => (),
            }

            source_status
        }
    });

    let (ok_stream, err_stream) = stream.map_fallible("SourceErrorDemux", move |r| {
        r.map_err(|e| SourceError::new(id, SourceErrorDetails::FileIO(e)))
    });

    let (ts_bindings_stream, ts_bindings_err_stream) = if let Some(source_persist) = source_persist
    {
        source_persist.render_persistence_operators(id, ts_bindings_stream)
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

    /// Restores timestamp bindings from the given `StreamReadHandle` by reading the differential
    /// timestamp updates and materializing/consolidating them into a `Vec`. This basically sums up
    /// the `diff` of the bindings and collects those bindings whose `diff` is `1`.
    ///
    /// This returns two `Vec`s. The first contains the valid bindings, the second one contains
    /// retractions  for the bindings that are beyond the `upper_seal_ts` which must be
    /// applied/emitted before emitting any new bindings updates.
    ///
    /// NOTE: Consolidated bindings with a `diff` other than `0` or `1` indicate a bug, and this
    /// method panics if that case happens.
    fn restore(
        &self,
    ) -> Result<
        (
            Vec<(SourceTimestamp, AssignedTimestamp)>,
            Vec<((SourceTimestamp, AssignedTimestamp), Diff)>,
        ),
        mz_persist::error::Error,
    > {
        // Materialized version of bindings updates that are not beyond the common seal timestamp.
        let mut valid_bindings: HashMap<_, Diff> = HashMap::new();

        let mut retractions: HashMap<_, Diff> = HashMap::new();

        let snapshot = self.config.read_handle.snapshot()?;
        let buf = snapshot.into_iter().collect::<Result<Vec<_>, _>>()?;

        // NOTE: We "compact" all time resolution, by ignorning the timestamp. We don't need
        // historical resolution and simply want the view as of the time at which the data was
        // sealed. Thus, it represents the content of the timestamp histories at exactly that
        // point.
        for ((source_timestamp, assigned_timestamp), ts, diff) in buf.into_iter() {
            if ts < self.config.upper_seal_ts {
                *valid_bindings
                    .entry((source_timestamp.clone(), assigned_timestamp))
                    .or_default() += diff;
            } else {
                *retractions
                    .entry((source_timestamp.clone(), assigned_timestamp))
                    .or_default() += diff;
            }
        }

        // We only want bindings that "exist". And panic on bindings that have a diff other than 0
        // or 1.
        let valid_bindings = valid_bindings
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
            .collect::<Vec<_>>();

        // For retractions, we just need the "consolidated" diff value for each binding/timestamp.
        let retractions = retractions
            .drain()
            .filter(|(binding, diff)| {
                if diff.abs() > 1 {
                    panic!(
                        "Binding with invalid diff. Binding {:?}, diff: {}.",
                        binding, diff
                    );
                }
                diff.abs() == 1
            })
            // retraction!
            .map(|(binding, diff)| (binding, -diff))
            .collect::<Vec<_>>();

        Ok((valid_bindings, retractions))
    }

    fn get_starting_offsets<'a>(
        &self,
        bindings: impl Iterator<Item = &'a (SourceTimestamp, AssignedTimestamp)>,
    ) -> HashMap<PartitionId, MzOffset> {
        let mut starting_offsets = HashMap::new();
        for (source_timestamp, _assigned_timestamp) in bindings {
            starting_offsets
                .entry(source_timestamp.partition.clone())
                .and_modify(|current_offset| {
                    if source_timestamp.offset > *current_offset {
                        *current_offset = source_timestamp.offset;
                    }
                })
                .or_insert_with(|| source_timestamp.offset);
        }

        starting_offsets
    }

    /// Renders operators that persist the given `ts_bindings_stream` to the configured persistent
    /// collection.
    ///
    /// This does not seal the persistent collection, calling code must ensure that this happens
    /// eventually.
    fn render_persistence_operators<G>(
        &self,
        source_id: SourceInstanceId,
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
            SourceError::new(source_id, SourceErrorDetails::Persistence(error))
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
    /// The timestamp up to which all involved streams have been sealed.
    upper_seal_ts: u64,

    /// [`StreamReadHandle`] for the collection that we should persist to.
    read_handle: StreamReadHandle<ST, AT>,

    /// [`StreamWriteHandle`] for the collection that we should persist to.
    pub write_handle: StreamWriteHandle<ST, AT>,
}

impl<K: Codec, V: Codec> PersistentTimestampBindingsConfig<K, V> {
    /// Creates a new [`PersistentTimestampBindingsConfig`] from the given parts.
    pub fn new(
        upper_seal_ts: u64,
        read_handle: StreamReadHandle<K, V>,
        write_handle: StreamWriteHandle<K, V>,
    ) -> Self {
        PersistentTimestampBindingsConfig {
            upper_seal_ts,
            read_handle,
            write_handle,
        }
    }
}

/// Updates the given `timestamp_bindings_updater` with any changes from the given
/// `timestamp_histories` and emits updates to `bindings_output` if there are in fact any changes.
///
/// The updates emitted from this can be used to re-construct the state of a `TimestampBindingRc`
/// at any given time by reading and applying (and consolidating, if neccessary) the stream of
/// changes.
fn maybe_emit_timestamp_bindings(
    source_name: &str,
    worker_id: usize,
    timestamp_bindings_updater: &mut Option<TimestampBindingUpdater>,
    timestamp_histories: &mut TimestampBindingRc,
    bindings_cap: &Capability<u64>,
    bindings_output: &mut OutputHandle<
        u64,
        ((SourceTimestamp, AssignedTimestamp), u64, Diff),
        Tee<u64, ((SourceTimestamp, AssignedTimestamp), u64, Diff)>,
    >,
) {
    let timestamp_bindings_updater = match timestamp_bindings_updater.as_mut() {
        Some(updater) => updater,
        None => {
            return;
        }
    };

    let changes = timestamp_bindings_updater.update(timestamp_histories);

    // Emit required changes downstream.
    let to_emit = changes
        .into_iter()
        .map(|(binding, diff)| (binding, bindings_cap.time().clone(), diff));

    // We're collecting into a Vec because we want to log and emit. This is a bit
    // wasteful but we don't expect large numbers of bindings.
    let mut to_emit = to_emit.collect::<Vec<_>>();

    trace!(
        "In {} (worker {}), emitting new timestamp bindings: {:?}, cap: {:?}",
        source_name,
        worker_id,
        to_emit,
        bindings_cap
    );

    let mut session = bindings_output.session(bindings_cap);
    session.give_vec(&mut to_emit);
}

/// Take `message` and assign it the appropriate timestamps and push it into the
/// dataflow layer, if possible.
///
/// TODO: This function is a bit of a mess rn but hopefully this function makes the
/// existing mess more obvious and points towards ways to improve it.
fn handle_message<S: SourceReader>(
    message: SourceMessage<S::Key, S::Value>,
    bytes_read: &mut usize,
    cap: &Capability<Timestamp>,
    output: &mut OutputHandle<
        Timestamp,
        Result<SourceOutput<S::Key, S::Value>, String>,
        Tee<Timestamp, Result<SourceOutput<S::Key, S::Value>, String>>,
    >,
    metric_updates: &mut HashMap<PartitionId, (MzOffset, Timestamp, i64)>,
    timer: &std::time::Instant,
    timestamp_bindings: &TimestampBindingRc,
) -> (SourceStatus, MessageProcessing) {
    let partition = message.partition.clone();
    let offset = message.offset;

    // Determine the timestamp to which we need to assign this message
    let ts = timestamp_bindings.get_or_propose_binding(&partition, offset);
    // Note: empty and null payload/keys are currently
    // treated as the same thing.
    let key = message.key;
    let out = message.value;
    // Entry for partition_metadata is guaranteed to exist as messages
    // are only processed after we have updated the partition_metadata for a
    // partition and created a partition queue for it.
    if let Some(len) = key.len() {
        *bytes_read += len;
    }
    if let Some(len) = out.len() {
        *bytes_read += len;
    }
    let ts_cap = cap.delayed(&ts);

    output.session(&ts_cap).give(Ok(SourceOutput::new(
        key,
        out,
        offset.offset,
        message.upstream_time_millis,
        message.partition,
        message.headers,
    )));

    match metric_updates.entry(partition) {
        Entry::Occupied(mut entry) => {
            entry.insert((offset, ts, entry.get().2 + 1));
        }
        Entry::Vacant(entry) => {
            entry.insert((offset, ts, 1));
        }
    }

    if timer.elapsed() > YIELD_INTERVAL {
        // We didn't drain the entire queue, so indicate that we
        // should run again but yield the CPU to other operators.
        (SourceStatus::Alive, MessageProcessing::Yielded)
    } else {
        (SourceStatus::Alive, MessageProcessing::Active)
    }
}
