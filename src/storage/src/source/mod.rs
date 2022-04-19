// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow sources.

use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use differential_dataflow::Hashable;
use prometheus::core::{AtomicI64, AtomicU64};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract};
use timely::dataflow::operators::{Capability, Event};
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::scheduling::activate::{Activator, SyncActivator};
use timely::Data;
use tokio::sync::{mpsc, RwLock, RwLockReadGuard};
use tracing::error;

use mz_avro::types::Value;
use mz_dataflow_types::sources::encoding::SourceDataEncoding;
use mz_dataflow_types::sources::{AwsExternalId, ExternalSourceConnector, MzOffset};
use mz_dataflow_types::{DecodeError, SourceError, SourceErrorDetails};
use mz_expr::{PartitionId, SourceInstanceId};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::now::NowFn;
use mz_ore::task;
use mz_repr::MessagePayload;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operator::StreamExt;

use crate::source::metrics::SourceBaseMetrics;
use crate::source::timestamp::TimestampBindingRc;
use crate::source::util::source;

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
    /// The time the record was created in the upstream system, as milliseconds since the epoch
    pub upstream_time_millis: Option<i64>,
    /// The partition of this message, present iff the partition comes from Kafka
    pub partition: PartitionId,
    /// Headers, if the source is configured to pass them along. If it is, but there are none, it
    /// passes `Some([])`
    pub headers: Option<Vec<(String, Option<Vec<u8>>)>>,
}

/// A wrapper that converts a delimited source reader that only provides
/// values into a key/value reader whose key is always None
pub struct DelimitedValueSource<S>(S);

impl<S> SourceReader for DelimitedValueSource<S>
where
    S: SourceReader<Key = (), Value = Option<Vec<u8>>>,
{
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;

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
        metrics: crate::source::metrics::SourceBaseMetrics,
    ) -> Result<Self, anyhow::Error> {
        S::new(
            source_name,
            source_id,
            worker_id,
            worker_count,
            consumer_activator,
            connector,
            aws_external_id,
            restored_offsets,
            encoding,
            metrics,
        )
        .map(Self)
    }

    fn get_next_message(&mut self) -> Result<NextMessage<Self::Key, Self::Value>, anyhow::Error> {
        match self.0.get_next_message()? {
            NextMessage::Ready(SourceMessage {
                key: _,
                value,
                partition,
                offset,
                upstream_time_millis,
                headers,
            }) => Ok(NextMessage::Ready(SourceMessage {
                key: None,
                value,
                partition,
                offset,
                upstream_time_millis,
                headers,
            })),
            NextMessage::Pending => Ok(NextMessage::Pending),
            NextMessage::TransientDelay => Ok(NextMessage::TransientDelay),
            NextMessage::Finished => Ok(NextMessage::Finished),
        }
    }
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
    /// The time the record was created in the upstream system, as milliseconds since the epoch
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
    capabilities: Rc<RefCell<Option<Capability<Timestamp>>>>,
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
        metrics: crate::source::metrics::SourceBaseMetrics,
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
    operator_scheduled_counter: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    /// Value of the capability associated with this source
    capability: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Per-partition Prometheus metrics.
    pub partition_metrics: HashMap<PartitionId, PartitionMetrics>,
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
                &self.source_name,
                self.source_id,
                &partition,
                offset.offset,
                timestamp as i64,
            );
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
        _source_name: &str,
        _source_id: SourceInstanceId,
        _partition_id: &PartitionId,
        offset: i64,
        timestamp: i64,
    ) {
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

    let (stream, capability) = source(scope, name.clone(), move |info| {
        let activator = Arc::new(scope.sync_activator_for(&info.address[..]));

        let metrics_name = upstream_name.unwrap_or(name);
        let mut metrics =
            SourceMetrics::new(&base_metrics, &metrics_name, id, &worker_id.to_string());

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
pub(crate) fn create_source<G, S: 'static>(
    config: SourceConfig<G>,
    source_connector: &ExternalSourceConnector,
    aws_external_id: AwsExternalId,
) -> (
    (
        timely::dataflow::Stream<G, SourceOutput<S::Key, S::Value>>,
        timely::dataflow::Stream<G, SourceError>,
    ),
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader,
{
    use std::collections::{BTreeMap, VecDeque};

    let SourceConfig {
        name,
        id,
        scope,
        mut timestamp_histories,
        worker_id,
        worker_count,
        timestamp_frequency,
        active,
        encoding,
        base_metrics,
        ..
    } = config;

    let (stream, capability) = source(scope, name.clone(), move |info| {
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
        // Pre-existing reclocking information.
        let restored_offsets = timestamp_histories
            .as_mut()
            .map(|ts| ts.partitions())
            .unwrap_or_default();
        // Records the *next* offset we expect to see for each part.
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
                base_metrics.clone(),
            ) {
                Ok(source_reader) => Some(source_reader),
                Err(e) => {
                    error!("Failed to create source: {}", e);
                    None
                }
            }
        };

        // Untransmitted messages arranged by part.
        // Messages are retained until their associated timestamp binding is durable,
        // at which point they (and other messages at their time) flow out.
        let mut messages_by_part = BTreeMap::new();

        move |cap, output| {
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

            // Bound execution of operator to prevent a single operator from hogging
            // the CPU if there are many messages to process
            let timer = Instant::now();

            let mut source_state = (SourceStatus::Alive, MessageProcessing::Active);
            while let (_, MessageProcessing::Active) = source_state {
                source_state = match source_reader.get_next_message() {
                    Ok(NextMessage::Ready(message)) => {
                        messages_by_part
                            .entry(message.partition.clone())
                            .or_insert_with(VecDeque::new)
                            .push_back(message);
                        if timer.elapsed() > YIELD_INTERVAL {
                            // We didn't drain the entire queue, so indicate that we
                            // should run again but yield the CPU to other operators.
                            (SourceStatus::Alive, MessageProcessing::Yielded)
                        } else {
                            (SourceStatus::Alive, MessageProcessing::Active)
                        }
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

            // TODO: This would be a great time to attempt to commit timestamp bindings.
            for (part, messages) in messages_by_part.iter() {
                if let Some(message) = messages.back() {
                    timestamp_histories.get_or_propose_binding(&part, message.offset);
                }
            }

            // Keyed by times, the messages to send at each time.
            let mut to_send = BTreeMap::new();

            // Times greater or equal to this frontier are not yet durable,
            // and we should not transmit output at these times.
            let durable_frontier = timestamp_histories.durability_frontier();
            for (part, messages) in messages_by_part.iter_mut() {
                while let Some(timestamp) = messages.front().map(|message| {
                    timestamp_histories.get_or_propose_binding(&message.partition, message.offset)
                }) {
                    if !durable_frontier.less_equal(&timestamp) {
                        let message = messages.pop_front().unwrap();
                        // Update the limits of offsets we have transmitted, for capability downgrading.
                        partition_cursors.insert(part.clone(), message.offset + 1);
                        // Form and enqueue the output.
                        let output = SourceOutput::new(
                            message.key,
                            message.value,
                            message.offset.offset,
                            message.upstream_time_millis,
                            message.partition,
                            message.headers,
                        );
                        to_send
                            .entry(timestamp)
                            .or_insert_with(Vec::new)
                            .push(Ok(output));
                    } else {
                        // We can stop processing now, as offsets only increase,
                        // and the offset to timestamp association increases monotonically.
                        break;
                    }
                }
            }
            // Transmit the data for each timestamp.
            for (timestamp, mut buffer) in to_send.into_iter() {
                let ts_cap = cap.delayed(&timestamp);
                output.session(&ts_cap).give_vec(&mut buffer);
            }

            // We should now downgrade our capability.
            // We must retain the ability to send any future data, which is determined by looking at
            // the *next* offsets for each part we manage, as this indicates data we might still receive.

            // Attempt to update the timestamp and finalize the currently pending bindings
            timestamp_histories.update_timestamp();

            // Downgrade capability (if possible) before exiting
            timestamp_histories.downgrade(cap, &partition_cursors);
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

    if active {
        ((ok_stream, err_stream), Some(capability))
    } else {
        // Immediately drop the capability if worker is not an active reader for source
        ((ok_stream, err_stream), None)
    }
}
