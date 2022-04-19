// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow raw sources.
//!
//! Raw sources are streams (currently, Timely streams) of data directly produced by the
//! upstream service. The main exports of this module are [`create_raw_source`]
//! and [`create_raw_source_simple`], which turns [`RawSourceCreationConfig`]'s,
//! [`ExternalSourceConnector`]'s, and [`SourceReader`]/[`SimpleSource`]
//! implementations into the aforementioned streams.
//!
//! The full source, which is the _differential_ stream that represents the actual object
//! created by a `CREATE SOURCE` statement, is created by composing
//! [`create_raw_source`] or [`create_raw_source_simple`] with
//! decoding, `SourceEnvelope` rendering, and more.
//! See the doc comment on [`rendering`](`crate::render::sources::render_source`)
//! for more details.

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context as _};
use async_trait::async_trait;
use differential_dataflow::Hashable;
use futures::task::Waker;
use futures::Stream;
use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_persist_types::Codec;
use prometheus::core::{AtomicI64, AtomicU64};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract};
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Capability, Event};
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp as _};
use timely::scheduling::activate::{Activator, SyncActivator};
use timely::Data;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, RwLock, RwLockReadGuard};
use tokio::time::error::Elapsed;
use tracing::{error, info};

use mz_avro::types::Value;
use mz_dataflow_types::sources::encoding::SourceDataEncoding;
use mz_dataflow_types::sources::{AwsExternalId, ExternalSourceConnector, MzOffset};
use mz_dataflow_types::{DecodeError, SourceError, SourceErrorDetails};
use mz_expr::PartitionId;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::now::NowFn;
use mz_ore::task;
use mz_persist_client::read::{Listen, ListenEvent};
use mz_persist_client::Upper;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operator::StreamExt;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::source::metrics::SourceBaseMetrics;
use crate::source::timestamp::TimestampBindingRc;
use crate::source::timestamper::CreateSourceTimestamper;
use crate::source::util::source;

mod kafka;
mod kinesis;
pub mod metrics;
pub mod persist_source;
mod postgres;
mod pubnub;
mod s3;
pub mod timestamp;
mod timestamper;
pub mod util;

pub use kafka::KafkaSourceReader;
pub use kinesis::KinesisSourceReader;
pub use postgres::PostgresSourceReader;
pub use pubnub::PubNubSourceReader;
pub use s3::S3SourceReader;

include!(concat!(env!("OUT_DIR"), "/mz_storage.source.rs"));

// Interval after which the source operator will yield control.
const YIELD_INTERVAL: Duration = Duration::from_millis(10);

/// Shared configuration information for all source types.
/// This is used in the `create_raw_source` functions, which
/// produce raw sources.
pub struct RawSourceCreationConfig<'a, G> {
    /// The name to attach to the underlying timely operator.
    pub name: String,
    /// The name of the upstream resource this source corresponds to
    /// (For example, a Kafka topic)
    pub upstream_name: Option<String>,
    /// The ID of this instantiation of this source.
    pub id: GlobalId,
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
        source_id: GlobalId,
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

    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value>, SourceReaderError> {
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

// TODO(guswynn): consider moving back to non-thread-safe `RC`'s if
// we end up with a boundary-per-worker
// TODO(guswynn): consider just using `SyncActivateOnDrop` if merged into timely
/// A `SourceToken` manages interest in a source, and is thread-safe.
///
/// When the `SourceToken` is dropped the associated source will be stopped.
pub struct SourceToken {
    pub activator: Arc<SyncActivator>,
}

impl Drop for SourceToken {
    fn drop(&mut self) {
        // Best effort: sync activation
        // failures are ignored
        let _ = self.activator.activate();
    }
}

/// The status of a source.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
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
}

impl MaybeLength for () {
    fn len(&self) -> Option<usize> {
        None
    }
}

impl MaybeLength for Vec<u8> {
    fn len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl MaybeLength for mz_repr::Row {
    fn len(&self) -> Option<usize> {
        Some(self.data().len())
    }
}

impl MaybeLength for Value {
    // Not possible to compute a size in bytes without recursively traversing the entire tree.
    fn len(&self) -> Option<usize> {
        None
    }
}

impl<T: MaybeLength> MaybeLength for Option<T> {
    fn len(&self) -> Option<usize> {
        self.as_ref().and_then(|v| v.len())
    }
}

/// A structured error for `SourceReader::get_next_message`
/// implementors. Also implements `From<anyhow::Error>`
/// for convenience.
pub struct SourceReaderError {
    pub inner: SourceErrorDetails,
}

impl From<anyhow::Error> for SourceReaderError {
    fn from(e: anyhow::Error) -> Self {
        SourceReaderError {
            inner: SourceErrorDetails::Other(format!("{}", e)),
        }
    }
}

pub(crate) type SourceReaderStream<S> = Pin<
    Box<
        dyn Stream<
            Item = Result<
                SourceMessage<<S as SourceReader>::Key, <S as SourceReader>::Value>,
                SourceReaderError,
            >,
        >,
    >,
>;

/// This trait defines the interface between Materialize and external sources, and
/// must be implemented for every new kind of source.
///
/// TODO: this trait is still a little too Kafka-centric, specifically the concept of
/// a "partition" is baked into this trait and introduces some cognitive overhead as
/// we are forced to treat things like file sources as "single-partition"
#[async_trait(?Send)]
pub trait SourceReader {
    type Key: timely::Data + MaybeLength;
    type Value: timely::Data + MaybeLength;

    /// Create a new source reader.
    ///
    /// This function returns the source reader and optionally, any "partition" it's
    /// already reading. In practice, the partition is only non-None for static sources
    /// that either don't truly have partitions or have a fixed number of partitions.
    fn new(
        source_name: String,
        source_id: GlobalId,
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
    /// Note that implementers are required to present messages in strictly ascending offset order
    /// within each partition.
    async fn next(
        &mut self,
        timestamp_frequency: Duration,
    ) -> Option<Result<SourceMessage<Self::Key, Self::Value>, SourceReaderError>> {
        // Compatiblity implementation that delegates to the deprecated [Self::get_next_method]
        // call. Once all source implementations have been transitioned to implement
        // [SourceReader::next] directly this provided implementation should be removed and the
        // method should become a required method.
        loop {
            match self.get_next_message() {
                Ok(NextMessage::Ready(msg)) => return Some(Ok(msg)),
                Err(err) => return Some(Err(err)),
                // There was a temporary hiccup in getting messages, check again asap.
                Ok(NextMessage::TransientDelay) => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                // There were no new messages, check again after a delay
                Ok(NextMessage::Pending) => tokio::time::sleep(timestamp_frequency).await,
                Ok(NextMessage::Finished) => return None,
            }
        }
    }

    /// Returns the next message available from the source.
    ///
    /// Note that implementers are required to present messages in strictly ascending offset order
    /// within each partition.
    ///
    /// # Deprecated
    ///
    /// Source implementation should implement the async [SourceReader::next] method instead.
    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value>, SourceReaderError> {
        Ok(NextMessage::Pending)
    }

    /// Returns an adapter that treats the source as a stream.
    ///
    /// The stream produces the messages that would be produced by repeated calls to `next`.
    fn into_stream(mut self, timestamp_frequency: Duration) -> SourceReaderStream<Self>
    where
        Self: Sized + 'static,
    {
        Box::pin(async_stream::stream! {
            while let Some(msg) = self.next(timestamp_frequency).await {
                yield msg;
            }
        })
    }
}

pub enum NextMessage<Key, Value> {
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

impl fmt::Debug for SourceMessage<(), Option<Vec<u8>>> {
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
    source_id: GlobalId,
    base_metrics: SourceBaseMetrics,
}

impl SourceMetrics {
    /// Initialises source metrics for a given (source_id, worker_id)
    pub fn new(
        base: &SourceBaseMetrics,
        source_name: &str,
        source_id: GlobalId,
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
        _source_id: GlobalId,
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
        source_id: GlobalId,
        partition_id: &PartitionId,
    ) -> PartitionMetrics {
        let labels = &[
            source_name.to_string(),
            source_id.to_string(),
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
    // Possibly used by no `SimpleSource` implementors
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

/// Simple sources must implement this trait. Raw source streams will then get created as part of the
/// [`create_raw_source_simple`] function.
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

/// Creates a raw source dataflow operator from a connector that has a corresponding [`SimpleSource`]
/// implentation. The type of ExternalSourceConnector determines the type of
/// connector that _should_ be created.
///
/// See the [`module` docs](self) for more details about how
/// raw sources are used.
pub fn create_raw_source_simple<G, C>(
    config: RawSourceCreationConfig<G>,
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
    let RawSourceCreationConfig {
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
        task::spawn(|| format!("source_simple_timestamper:{}", id), async move {
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

enum SourceMessageProcessingState<S: SourceReader> {
    Ready(SourceReaderStream<S>),
    Draining,
    Finished,
}
struct CreateSourceRunner<S: SourceReader> {
    name: String,
    processing_state: SourceMessageProcessingState<S>,
    activator: Activator,
    waker: Waker,
    messages_by_part: BTreeMap<
        PartitionId,
        VecDeque<SourceMessage<<S as SourceReader>::Key, <S as SourceReader>::Value>>,
    >,
    read_progress: Antichain<Timestamp>,
    persisted_timestamp_bindings: BTreeMap<PartitionId, VecDeque<(Timestamp, Diff)>>,
    partition_cursors: HashMap<PartitionId, MzOffset>,
    read_cursors: HashMap<PartitionId, MzOffset>,
    timestamp_bindings_listener: Listen<(), PartitionId, Timestamp, Diff>,
    timestamp_frequency: Duration,
    source_id: SourceInstanceId,
    bindings_channel: Sender<(PartitionId, MzOffset)>,
}

impl<S: SourceReader> CreateSourceRunner<S> {
    async fn initialize(
        name: String,
        processing_state: SourceMessageProcessingState<S>,
        activator: Activator,
        waker: Waker,
        CollectionMetadata {
            persist_location,
            timestamp_shard_id,
        }: CollectionMetadata,
        timestamp_frequency: Duration,
        source_id: SourceInstanceId,
        bindings_channel: Sender<(PartitionId, MzOffset)>,
    ) -> anyhow::Result<Option<Self>> {
        let persist_client = persist_location
            .open()
            .await
            .expect("error creating persist client");

        // Untransmitted messages arranged by part.
        // Messages are retained until their associated timestamp binding is durable,
        // at which point they (and other messages at their time) flow out.
        let messages_by_part = BTreeMap::new();

        let partition_cursors = HashMap::new();
        let read_cursors = HashMap::new();

        let (mut write_handle, read_handle) = persist_client
            .open(timestamp_shard_id)
            .await
            .expect("persist handles open err");

        // Collection is closed.  No source to run
        if read_handle.since().is_empty() || write_handle.upper().is_empty() {
            return Ok(None);
        }

        let read_progress = read_handle.since().clone();

        // If this is the initialization of the persist collection, allow a read at Timestamp::minimum().  Else fetch
        // the current upper.
        let min_plus_one = Antichain::from_elem(Timestamp::minimum() + 1);
        let empty: [(((), PartitionId), Timestamp, Diff); 0] = [];
        let write_upper = write_handle
            .compare_and_append(
                empty,
                Antichain::from_elem(Timestamp::minimum()),
                min_plus_one.clone(),
            )
            .await
            .expect("Initial CAA")
            .with_context(|| "Initial compare and append")?
            .err()
            .map(|Upper(actual_upper)| actual_upper)
            .unwrap_or(min_plus_one);

        assert!(
            timely::PartialOrder::less_equal(&read_progress, &write_upper),
            "{:?} PARTIAL ORDER: {:?} {:?}",
            name,
            read_progress,
            write_upper,
        );

        let mut snap_iter = read_handle
            .snapshot(read_progress.clone())
            .await
            .unwrap_or_else(|e| panic!("{:?} read snapshot at handle.since {:?}", name, e));

        let mut persisted_timestamp_bindings = BTreeMap::new();
        loop {
            match snap_iter.next().await {
                None => break,
                Some(v) => {
                    for ((key, value), timestamp, diff) in v {
                        let _: () = key.unwrap();
                        let partition = value.expect("Unable to decode partition id");
                        persisted_timestamp_bindings
                            .entry(partition)
                            .or_insert_with(VecDeque::new)
                            .push_back((timestamp, diff));
                    }
                }
            };
        }

        // TODO(#12267): Properly downgrade `since` of the timestamp collection based on the source data collection.
        assert!(persisted_timestamp_bindings.is_empty());

        let timestamp_bindings_listener = read_handle
            .listen(read_progress.clone())
            .await
            .expect("Initial listen at handle.since");

        Ok(Some(Self {
            name,
            processing_state,
            activator,
            waker,
            messages_by_part,
            read_progress,
            persisted_timestamp_bindings,
            partition_cursors,
            read_cursors,
            timestamp_bindings_listener,
            timestamp_frequency,
            source_id,
            bindings_channel,
        }))
    }

    async fn invoke<'a>(
        &mut self,
        mut output: OutputHandle<
            'a,
            Timestamp,
            Result<SourceOutput<<S as SourceReader>::Key, <S as SourceReader>::Value>, SourceError>,
            TeeCore<
                Timestamp,
                Vec<
                    Result<
                        SourceOutput<<S as SourceReader>::Key, <S as SourceReader>::Value>,
                        SourceError,
                    >,
                >,
            >,
        >,
        cap: &mut Capability<Timestamp>,
    ) -> anyhow::Result<SourceStatus> {
        // Bound execution of operator to prevent a single operator from hogging
        // the CPU if there are many messages to process
        let timer = Instant::now();

        let mut context = Context::from_waker(&self.waker);
        match self.processing_state {
            SourceMessageProcessingState::Ready(ref mut reader) => {
                while let Poll::Ready(item) = reader.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok(message)) => {
                            // N.B. Messages must arrive in increasing offset order
                            if let Some(old_offset) = self
                                .partition_cursors
                                .insert(message.partition.clone(), message.offset.clone())
                            {
                                assert!(
                                    message.offset >= old_offset,
                                    "Offsets must arrive in increasing order: {:?} -> {:?}",
                                    old_offset,
                                    message.offset
                                );
                            }
                            self.messages_by_part
                                .entry(message.partition.clone())
                                .or_insert_with(VecDeque::new)
                                .push_back(message);
                        }
                        Some(Err(e)) => {
                            output.session(&cap).give(Err(SourceError {
                                source_id: self.source_id,
                                error: e.inner,
                            }));
                            self.processing_state = SourceMessageProcessingState::Finished;
                            break;
                        }
                        None => {
                            self.processing_state = SourceMessageProcessingState::Draining;
                            break;
                        }
                    }
                    if timer.elapsed() > YIELD_INTERVAL {
                        // We didn't drain the entire queue, so indicate that we
                        // should run again but yield the CPU to other operators.
                        self.activator.activate();
                        break;
                    }
                }
            }
            SourceMessageProcessingState::Draining => (),
            SourceMessageProcessingState::Finished => return Ok(SourceStatus::Done),
        }

        if self.bindings_channel.is_closed() {
            info!("{:?} RECEIVER CLOSED; MARKING AS DONE", self.name);
            return Ok(SourceStatus::Done);
        }

        // Let the timestamper know about the new max bindings
        for (partition, offset) in self.partition_cursors.drain() {
            // If receiver drops, just shut down
            if let Err(_) = self.bindings_channel.send((partition, offset)).await {
                return Ok(SourceStatus::Done);
            }
        }

        // Read in any new bindings.  We use these events are the sole indication of whether a
        // timestamp can be emitted.  Unless we read it back out of the persistent collection,
        // assume it can be changed.
        //
        // XXX(chae): we'll need to change this unless we can have a nb poll (like we do for
        // reading snapshots).  I think we'll need a nb poll _anyway_ because we'll need to
        // be able to read in bindings when we lose a CAS write -- this will require a quick
        // listen OR the ability to read from a range.
        let listen_events = match tokio::time::timeout(
            Duration::from_secs(2),
            self.timestamp_bindings_listener.next(),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                // `Elapsed` has a private constructor so can't just match above
                let _: Elapsed = e;
                vec![]
            }
        };
        for event in listen_events {
            match event {
                ListenEvent::Progress(progress) => {
                    assert!(
                        timely::PartialOrder::less_equal(&self.read_progress, &progress),
                        "PARTIAL ORDER: {:?} {:?}",
                        self.read_progress,
                        progress
                    );
                    self.read_progress = progress;
                }
                ListenEvent::Updates(updates) => {
                    for ((_, value), timestamp, diff) in updates {
                        let partition = value.expect("Unable to decode partition id");
                        self.persisted_timestamp_bindings
                            .entry(partition)
                            .or_insert_with(VecDeque::new)
                            .push_back((timestamp, diff));
                    }
                }
            }
        }

        // TODO(#12267): Properly downgrade `since` of the timestamp collection based on the source data collection.

        let mut to_write = BTreeMap::new();
        for (partition, messages) in self.messages_by_part.iter_mut() {
            let bindings = match self.persisted_timestamp_bindings.get_mut(partition) {
                Some(bindings) => bindings,
                None => continue,
            };

            while let Some(message) = messages.front() {
                // Compact as necessary, relying on all messages being in ascending offset order
                while bindings.len() > 1
                    && bindings
                        .front()
                        .expect("always at least one binding per partition")
                        .1
                        < message.offset.offset
                {
                    let (_old_timestamp, old_max_offset) = bindings.pop_front().unwrap();
                    let (_timestamp, incremental_offset) = bindings.front_mut().unwrap();
                    *incremental_offset += old_max_offset;
                }

                let (timestamp, max_offset) = bindings
                    .front()
                    .expect("always at least one binding per partition");
                if message.offset.offset <= *max_offset {
                    self.read_cursors.insert(partition.clone(), message.offset);
                    to_write
                        .entry((*timestamp, partition.clone()))
                        .or_insert_with(Vec::new)
                        .push(messages.pop_front().unwrap());
                } else {
                    assert_eq!(bindings.len(), 1);
                    // Unable to match any more from this partition
                    break;
                }
            }
        }

        // Transmit the data for each durable timestamp.
        for ((timestamp, _partition), messages) in to_write {
            let mut msg_output = vec![];
            for message in messages {
                // Form and enqueue the output.
                msg_output.push(Ok(SourceOutput::new(
                    message.key,
                    message.value,
                    message.offset.offset,
                    message.upstream_time_millis,
                    message.partition,
                    message.headers,
                )));
            }
            let ts_cap = cap.delayed(&timestamp);
            output.session(&ts_cap).give_vec(&mut msg_output);
        }

        // We should now downgrade our capability.
        let new_cap = self
            .persisted_timestamp_bindings
            .iter()
            .filter_map(
                // If there's only one bindings, we may have written through that offset so check write_cursor. Else,
                // use the min timestamp because we could still write out a message then.
                |(partition, bindings)| {
                    let (lo_binding_ts, lo_binding_offset) = bindings
                        .get(0)
                        .expect("Always have at least one binding per existing partition");
                    if bindings.len() == 1 {
                        match self.read_cursors.get(partition) {
                            Some(cursor_offset) => {
                                if cursor_offset.offset >= *lo_binding_offset {
                                    None
                                } else {
                                    Some(*lo_binding_ts)
                                }
                            }
                            None => Some(*lo_binding_ts),
                        }
                    } else {
                        Some(*lo_binding_ts)
                    }
                },
            )
            .chain(std::iter::once(
                *self.read_progress.elements().first().unwrap_or(&u64::MAX),
            ))
            .min()
            .unwrap_or_else(Timestamp::minimum);
        let _ = cap.try_downgrade(&new_cap);

        let pending_messages = self.messages_by_part.values().any(|m| !m.is_empty())
            || self
                .persisted_timestamp_bindings
                .values()
                .any(|m| m.len() > 1);

        let source_status = match (pending_messages, &self.processing_state) {
            (_, SourceMessageProcessingState::Finished) => SourceStatus::Done,

            // No pending messages, Draining -> draining is complete
            (false, SourceMessageProcessingState::Draining) => {
                // TODO(#12267): Properly downgrade `since` of the timestamp collection based on the source data collection.

                self.processing_state = SourceMessageProcessingState::Finished;
                SourceStatus::Done
            }

            // No pending messages, not Draining
            (false, SourceMessageProcessingState::Ready(_)) => SourceStatus::Alive,

            // Pending messages: make sure source stays alive
            (
                true,
                SourceMessageProcessingState::Ready(_) | SourceMessageProcessingState::Draining,
            ) => {
                self.activator.activate_after(self.timestamp_frequency);
                SourceStatus::Alive
            }
        };
        Ok(source_status)
    }
}

/// Creates a raw source dataflow operator from a connector that has a corresponding [`SourceReader`]
/// implentation. The type of ExternalSourceConnector determines the type of
/// connector that _should_ be created.
///
/// This is also the place where _reclocking_
/// (<https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210714_reclocking.md>)
/// happens.
///
/// See the [`module` docs](self) for more details about how
/// raw sources are used.
pub fn create_raw_source<G, S: 'static>(
    config: RawSourceCreationConfig<G>,
    source_connector: &ExternalSourceConnector,
    aws_external_id: AwsExternalId,
    collection_metadata: CollectionMetadata,
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
    S::Key: Debug + Codec,
    S::Value: Debug + Codec,
{
    let RawSourceCreationConfig {
        name,
        id: source_id,
        scope,
        mut timestamp_histories,
        worker_id,
        worker_count,
        timestamp_frequency,
        active,
        encoding,
        base_metrics,
        now,
        ..
    } = config;

    let scope = scope.clone();

    let mut token = None;

    let mut builder = OperatorBuilder::new(name.clone(), scope.clone());
    let operator_info = builder.operator_info();

    let (mut data_output, data_stream) = builder.new_output();
    builder.set_notify(false);

    // Pre-existing reclocking information.
    let restored_offsets = timestamp_histories
        .as_mut()
        .map(|ts| ts.partitions())
        .unwrap_or_default();

    let source_reader = if !active {
        SourceMessageProcessingState::<S>::Finished
    } else {
        match S::new(
            name.clone(),
            source_id,
            worker_id,
            worker_count,
            scope.sync_activator_for(&operator_info.address[..]),
            source_connector.clone(),
            aws_external_id,
            restored_offsets,
            encoding,
            base_metrics.clone(),
        ) {
            Ok(source_reader) => {
                SourceMessageProcessingState::Ready(source_reader.into_stream(timestamp_frequency))
            }
            Err(e) => {
                error!("Failed to create source: {}", e);
                SourceMessageProcessingState::Finished
            }
        }
    };

    // Arbitrary size channel
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    // TODO make only one of these not one per worker
    let _timestamper_task = mz_ore::task::spawn(|| format!("timestamper_{}", name), {
        let collection_metadata = collection_metadata.clone();
        let name = name.clone();
        async move {
            let mut runner = match CreateSourceTimestamper::initialize(
                name.clone(),
                collection_metadata,
                now,
                timestamp_frequency,
                rx,
            )
            .await
            .expect("initializing CreateSourceRunner")
            {
                Some(runner) => runner,
                None => return,
            };

            while let Ok(true) = { runner.invoke().await } {
                tokio::task::yield_now().await;
            }
        }
    });

    builder.build_async(scope.clone(), |mut capabilities, frontiers, scheduler| {
        // Must be one to begin
        let mut capabilities = Some(capabilities.pop().unwrap());

        let activator = scope.activator_for(&operator_info.address[..]);

        let waker_activator = Arc::new(scope.sync_activator_for(&operator_info.address[..]));
        let waker = futures::task::waker(waker_activator);

        let drop_activator = Arc::new(scope.sync_activator_for(&operator_info.address[..]));
        let drop_activator_weak = Arc::downgrade(&drop_activator);

        // Export a token to the outside word that will keep this source alive.
        token = Some(SourceToken {
            activator: drop_activator,
        });

        let name_clone = name.clone();
        async move {
            if let SourceMessageProcessingState::Finished = source_reader {
                return;
            }

            let mut runner = match CreateSourceRunner::initialize(
                name_clone.clone(),
                source_reader,
                activator,
                waker,
                collection_metadata,
                timestamp_frequency,
                source_id,
                tx.clone(),
            )
            .await
            .expect("initializing CreateSourceRunner")
            {
                Some(runner) => runner,
                None => return,
            };

            loop {
                scheduler.notified().await;
                let iteration_frontiers = (*frontiers.borrow()).clone();

                // Drop all capabilities if the thread-safe `SourceToken` is dropped.
                if drop_activator_weak.upgrade().is_none() {
                    capabilities = None;
                }

                if let Some(data_cap) = &mut capabilities {
                    // We still have our capability, so the source is still alive.
                    // Delegate to the inner source.
                    if let SourceStatus::Done = {
                        runner
                            .invoke(data_output.activate(), data_cap)
                            .await
                            .unwrap()
                    } {
                        // The inner source is finished. Drop our capability.
                        capabilities = None;
                    }
                }

                if capabilities.is_none() && iteration_frontiers.iter().all(|f| f.is_empty()) {
                    break;
                }
            }
        }
    });

    // `build()` promises to call the provided closure before returning,
    // so we are guaranteed that `token` is non-None.
    let token = token.unwrap();

    let (ok_stream, err_stream) = data_stream.map_fallible("SourceErrorDemux", |r| r);
    if active {
        ((ok_stream, err_stream), Some(token))
    } else {
        // Immediately drop the capability if worker is not an active reader for source
        ((ok_stream, err_stream), None)
    }
}
