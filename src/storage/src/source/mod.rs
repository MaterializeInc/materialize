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
//! upstream service. The main export of this module is [`create_raw_source`],
//! which turns [`RawSourceCreationConfig`]s, [`SourceConnection`]s,
//! and [`SourceReader`] implementations into the aforementioned streams.
//!
//! The full source, which is the _differential_ stream that represents the actual object
//! created by a `CREATE SOURCE` statement, is created by composing
//! [`create_raw_source`] with
//! decoding, `SourceEnvelope` rendering, and more.

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use differential_dataflow::Hashable;
use futures::stream::LocalBoxStream;
use futures::{Stream, StreamExt as _};
use prometheus::core::{AtomicI64, AtomicU64};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Capability, Event};
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp as _};
use timely::scheduling::activate::SyncActivator;
use timely::scheduling::ActivateOnDrop;
use timely::{Data, PartialOrder};
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;

use mz_avro::types::Value;
use mz_expr::PartitionId;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operator::StreamExt as _;

use crate::controller::CollectionMetadata;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::reclock::ReclockOperator;
use crate::source::util::source;
use crate::types::connections::ConnectionContext;
use crate::types::errors::{DecodeError, SourceError, SourceErrorDetails};
use crate::types::sources::encoding::SourceDataEncoding;
use crate::types::sources::{MzOffset, SourceConnection};

pub mod generator;
mod kafka;
mod kinesis;
pub mod metrics;
pub mod persist_source;
mod postgres;
mod pubnub;
mod reclock;
mod s3;
pub mod util;

pub use generator::LoadGeneratorSourceReader;
pub use kafka::KafkaSourceReader;
pub use kinesis::KinesisSourceReader;
pub use postgres::PostgresSourceReader;
pub use pubnub::PubNubSourceReader;
pub use s3::S3SourceReader;

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
    /// Storage Metadata
    pub storage_metadata: CollectionMetadata,
    /// The upper frontier this source should resume ingestion at
    pub resume_upper: Antichain<Timestamp>,
    /// A handle to the persist client cachce
    pub persist_clients: Arc<Mutex<PersistClientCache>>,
}

/// A record produced by a source
#[derive(Clone, Serialize, Debug, Deserialize)]
pub struct SourceOutput<K, V, D>
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
    pub position: MzOffset,
    /// The time the record was created in the upstream system, as milliseconds since the epoch
    pub upstream_time_millis: Option<i64>,
    /// The partition of this message, present iff the partition comes from Kafka
    pub partition: PartitionId,
    /// Headers, if the source is configured to pass them along. If it is, but there are none, it
    /// passes `Some([])`
    pub headers: Option<Vec<(String, Option<Vec<u8>>)>>,

    /// Indicator for what the differential `diff` value
    /// for this decoded message should be
    pub diff: D,
}

/// A wrapper that converts a delimited source reader that only provides
/// values into a key/value reader whose key is always None
pub struct DelimitedValueSource<S>(S);

impl<S, D: timely::Data> SourceReader for DelimitedValueSource<S>
where
    S: SourceReader<Key = (), Value = Option<Vec<u8>>, Diff = D>,
{
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;
    type Diff = D;

    fn new(
        source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        connection: SourceConnection,
        restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        encoding: SourceDataEncoding,
        metrics: crate::source::metrics::SourceBaseMetrics,
        connection_context: ConnectionContext,
    ) -> Result<Self, anyhow::Error> {
        S::new(
            source_name,
            source_id,
            worker_id,
            worker_count,
            consumer_activator,
            connection,
            restored_offsets,
            encoding,
            metrics,
            connection_context,
        )
        .map(Self)
    }

    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        match self.0.get_next_message()? {
            NextMessage::Ready(SourceMessageType::Finalized(SourceMessage {
                key: _,
                value,
                partition,
                offset,
                upstream_time_millis,
                headers,
                specific_diff,
            })) => Ok(NextMessage::Ready(SourceMessageType::Finalized(
                SourceMessage {
                    key: None,
                    value,
                    partition,
                    offset,
                    upstream_time_millis,
                    headers,
                    specific_diff,
                },
            ))),
            NextMessage::Ready(SourceMessageType::InProgress(SourceMessage {
                key: _,
                value,
                partition,
                offset,
                upstream_time_millis,
                headers,
                specific_diff,
            })) => Ok(NextMessage::Ready(SourceMessageType::InProgress(
                SourceMessage {
                    key: None,
                    value,
                    partition,
                    offset,
                    upstream_time_millis,
                    headers,
                    specific_diff,
                },
            ))),
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
    /// The decoded value, as well as the the
    /// differential `diff` value for this value, if the value
    /// is present and not and error.
    pub value: Option<Result<(Row, Diff), DecodeError>>,
    /// The index of the decoded value in the stream
    pub position: MzOffset,
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

impl<K, V, D> SourceOutput<K, V, D>
where
    K: Data,
    V: Data,
{
    /// Build a new SourceOutput
    pub fn new(
        key: K,
        value: V,
        position: MzOffset,
        upstream_time_millis: Option<i64>,
        partition: PartitionId,
        headers: Option<Vec<(String, Option<Vec<u8>>)>>,
        diff: D,
    ) -> SourceOutput<K, V, D> {
        SourceOutput {
            key,
            value,
            position,
            upstream_time_millis,
            partition,
            headers,
            diff,
        }
    }
}
impl<K, V, D> SourceOutput<K, V, D>
where
    K: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
    V: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
    D: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
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
    _activator: Rc<ActivateOnDrop<()>>,
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
    type Diff: timely::Data;

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
        connection: SourceConnection,
        restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        encoding: SourceDataEncoding,
        metrics: crate::source::metrics::SourceBaseMetrics,
        connection_context: ConnectionContext,
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
    ) -> Option<Result<SourceMessageType<Self::Key, Self::Value, Self::Diff>, SourceReaderError>>
    {
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
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        Ok(NextMessage::Pending)
    }

    /// Returns an adapter that treats the source as a stream.
    ///
    /// The stream produces the messages that would be produced by repeated calls to `next`.
    fn into_stream<'a>(
        mut self,
        timestamp_frequency: Duration,
    ) -> LocalBoxStream<
        'a,
        Result<SourceMessageType<Self::Key, Self::Value, Self::Diff>, SourceReaderError>,
    >
    where
        Self: Sized + 'a,
    {
        Box::pin(async_stream::stream!({
            while let Some(msg) = self.next(timestamp_frequency).await {
                yield msg;
            }
        }))
    }
}

pub enum NextMessage<Key, Value, Diff> {
    Ready(SourceMessageType<Key, Value, Diff>),
    Pending,
    TransientDelay,
    Finished,
}

/// A wrapper around [`SourceMessage`] that allows
/// [`SourceReader`]'s to communicate if a message
/// if the final message a specific offset
pub enum SourceMessageType<Key, Value, Diff> {
    /// Communicate that this [`SourceMessage`] is the final
    /// message its its offset.
    Finalized(SourceMessage<Key, Value, Diff>),
    /// Communicate that more [`SourceMessage`]'s
    /// will come later at the same offset as this one.
    InProgress(SourceMessage<Key, Value, Diff>),
}

/// Source-agnostic wrapper for messages. Each source must implement a
/// conversion to Message.
pub struct SourceMessage<Key, Value, Diff> {
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

    /// Allow sources to optionally output a specific differential
    /// `diff` value. Defaults to `+1`.
    ///
    /// Only supported with `SourceEnvelope::None`
    pub specific_diff: Diff,
}

impl fmt::Debug for SourceMessage<(), Option<Vec<u8>>, ()> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceMessage")
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("upstream_time_millis", &self.upstream_time_millis)
            .finish()
    }
}

impl fmt::Debug for SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>, ()> {
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
    _pid: &PartitionId,
) -> bool {
    // In the future, load balancing will depend on persist shard ids but currently we write everything to a single shard
    (usize::cast_from(source_id.hashed()) % worker_count) == worker_id
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
    offset_ingested: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Highest offset that has been received by the source
    offset_received: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Value of the highest timestamp that is closed (for which all messages have been ingested)
    closed_ts: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Total number of messages that have been received by the source and timestamped
    messages_ingested: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
    last_offset: u64,
    last_timestamp: i64,
}

impl PartitionMetrics {
    /// Record the latest offset ingested high-water mark
    fn record_offset(
        &mut self,
        _source_name: &str,
        _source_id: GlobalId,
        _partition_id: &PartitionId,
        offset: u64,
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

/// Creates a raw source dataflow operator from a connection that has a corresponding [`SourceReader`]
/// implentation. The type of SourceConnection determines the type of
/// connection that _should_ be created.
///
/// This is also the place where _reclocking_
/// (<https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210714_reclocking.md>)
/// happens.
///
/// See the [`module` docs](self) for more details about how
/// raw sources are used.
pub fn create_raw_source<G, S: 'static>(
    config: RawSourceCreationConfig<G>,
    source_connection: &SourceConnection,
    connection_context: ConnectionContext,
) -> (
    (
        timely::dataflow::Stream<G, SourceOutput<S::Key, S::Value, S::Diff>>,
        timely::dataflow::Stream<G, SourceError>,
    ),
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader,
{
    let RawSourceCreationConfig {
        name,
        upstream_name,
        id,
        scope,
        worker_id,
        worker_count,
        active,
        timestamp_frequency,
        encoding,
        storage_metadata,
        resume_upper,
        base_metrics,
        now,
        persist_clients,
    } = config;

    let bytes_read_counter = base_metrics.bytes_read.clone();

    let (stream, capability) = source(scope, name.clone(), move |info| {
        let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
        let waker = futures::task::waker(waker_activator);

        let metrics_name = upstream_name.unwrap_or_else(|| name.clone());
        let mut source_metrics =
            SourceMetrics::new(base_metrics, &metrics_name, id, &worker_id.to_string());

        let sync_activator = scope.sync_activator_for(&info.address[..]);
        let base_metrics = base_metrics.clone();
        let source_connection = source_connection.clone();
        let mut source_reader = Box::pin(async_stream::stream!({
            let upper_ts = resume_upper.as_option().copied().unwrap();
            let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
            let mut timestamper = match ReclockOperator::new(
                persist_clients,
                storage_metadata,
                now,
                timestamp_frequency.clone(),
                as_of,
            )
            .await
            {
                Ok(t) => t,
                Err(e) => {
                    panic!("Failed to create source {} timestamper: {:#}", name, e);
                }
            };

            let mut source_upper = timestamper.source_upper_at(upper_ts.saturating_sub(1));
            let mut start_offsets = Vec::with_capacity(source_upper.len());
            for (pid, offset) in source_upper.iter() {
                start_offsets.push((pid.clone(), Some(offset.clone())));
            }

            let source_reader = S::new(
                name.clone(),
                id,
                worker_id,
                worker_count,
                sync_activator,
                source_connection.clone(),
                start_offsets,
                encoding,
                base_metrics,
                connection_context.clone(),
            );
            let source_stream = source_reader
                .expect("Failed to create source")
                .into_stream(timestamp_frequency)
                .fuse();

            tokio::pin!(source_stream);

            let mut timestamp_interval = tokio::time::interval(timestamp_frequency);
            timestamp_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut ts_upper = Antichain::from_elem(Timestamp::minimum());
            let mut untimestamped_messages = HashMap::<_, Vec<_>>::new();
            let mut non_definite_errors = vec![];
            loop {
                // TODO(guswyn): move lots of this out of the macro so rustfmt works better
                tokio::select! {
                    // N.B. This branch is cancel-safe because `next` only borrows the underlying stream.
                    item = source_stream.next() => {
                        match item {
                            Some(Ok(message)) => {
                                // Note that this
                                // 1. Requires that sources that produce `InProgress` messages
                                //    ALWAYS produce a `Finalized` for the final message.
                                // 2. Requires that sources that produce `InProgress` messages
                                //    NEVER produces messages at offsets below the most recent
                                //    `Finalized` message.
                                let is_final = matches!(message, SourceMessageType::Finalized(_));
                                match message {
                                    SourceMessageType::Finalized(message) | SourceMessageType::InProgress(message) => {
                                        let pid = message.partition.clone();
                                        let offset = message.offset;
                                        // advance the _offset_ frontier if this the final message for that offset
                                        if is_final {
                                            *source_upper.entry(pid.clone()).or_default() = offset + 1;
                                        }
                                        untimestamped_messages.entry(pid).or_default().push((message, offset));
                                    }
                                }
                            }
                            // TODO: make errors definite
                            Some(Err(e)) => non_definite_errors.push(e),
                            None => {},
                        }
                    }
                    // It's time to timestamp a batch
                    _ = timestamp_interval.tick() => {
                        let reclocked_msgs = match timestamper.reclock(&mut untimestamped_messages).await {
                            Ok(msgs) => msgs,
                            Err((pid, offset)) => panic!("failed to reclock {} @ {}", pid, offset),
                        };
                        for (_, part_messages) in reclocked_msgs {
                            for (message, ts) in part_messages {
                                yield Event::Message(ts, Ok(message));
                            }
                        }

                        timestamper.advance().await;

                        for err in non_definite_errors.drain(..) {
                            // TODO: make errors definite
                            yield Event::Message(0, Err(err));
                        }
                        // TODO(petrosagg): compaction should be driven by AllowCompaction commands
                        // coming from the storage controller
                        let new_ts_upper = timestamper
                            .reclock_frontier(&source_upper)
                            .expect("compacted past upper");
                        if PartialOrder::less_than(&ts_upper, &new_ts_upper) {
                            // Downgrade our capability to the new upper, but only if has changed
                            // since last time we downgraded it. Note property #2 documented above
                            // is critical here: once this capability is downgraded, that timestamp
                            // is sealed forever.
                            ts_upper = new_ts_upper.clone();
                            yield Event::Progress(new_ts_upper.into_option());
                        }
                        if source_stream.is_done() {
                            yield Event::Progress(None);
                            break;
                        }
                    }
                }
            }
        }));

        let activator = scope.activator_for(&info.address[..]);
        move |cap, output| {
            if !active {
                return SourceStatus::Done;
            }

            // Accumulate updates to bytes_read for Prometheus metrics collection
            let mut bytes_read = 0;
            // Accumulate updates to offsets for system table metrics collection
            let mut metric_updates = HashMap::new();
            // Record operator has been scheduled
            source_metrics.operator_scheduled_counter.inc();

            let mut context = Context::from_waker(&waker);
            let mut source_status = SourceStatus::Alive;

            let timer = Instant::now();

            while let Poll::Ready(Some(event)) = source_reader.as_mut().poll_next(&mut context) {
                match event {
                    Event::Progress(upper) => {
                        let ts = upper.unwrap_or(Timestamp::MAX);
                        for partition_metrics in source_metrics.partition_metrics.values_mut() {
                            partition_metrics.closed_ts.set(ts);
                        }
                        // TODO(petrosagg): `cap` should become a CapabilitySet to allow
                        // downgrading it to the empty frontier. For now setting SourceStatus to
                        // Done achieves the same effect
                        cap.downgrade(&ts);
                        if upper.is_none() {
                            source_status = SourceStatus::Done;
                        }
                    }
                    Event::Message(ts, message) => match message {
                        Ok(message) => handle_message::<S>(
                            message,
                            &mut bytes_read,
                            &cap,
                            output,
                            &mut metric_updates,
                            ts,
                        ),
                        // TODO: make errors definite
                        Err(e) => {
                            output.session(&cap).give(Err(SourceError {
                                source_id: id,
                                error: e.inner,
                            }));
                        }
                    },
                }
                if timer.elapsed() > YIELD_INTERVAL {
                    let _ = activator.activate();
                    break;
                }
            }

            bytes_read_counter.inc_by(bytes_read as u64);
            source_metrics.record_partition_offsets(metric_updates);
            source_metrics.capability.set(*cap.time());

            source_status
        }
    });
    let (ok_stream, err_stream) = stream.map_fallible("SourceErrorDemux", |r| r);
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
fn handle_message<S: SourceReader>(
    message: SourceMessage<S::Key, S::Value, S::Diff>,
    bytes_read: &mut usize,
    cap: &Capability<Timestamp>,
    output: &mut OutputHandle<
        Timestamp,
        Result<SourceOutput<S::Key, S::Value, S::Diff>, SourceError>,
        Tee<Timestamp, Result<SourceOutput<S::Key, S::Value, S::Diff>, SourceError>>,
    >,
    metric_updates: &mut HashMap<PartitionId, (MzOffset, Timestamp, i64)>,
    ts: Timestamp,
) {
    let partition = message.partition.clone();
    let offset = message.offset;

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
        offset,
        message.upstream_time_millis,
        message.partition,
        message.headers,
        message.specific_diff,
    )));
    match metric_updates.entry(partition) {
        Entry::Occupied(mut entry) => {
            entry.insert((offset, ts, entry.get().2 + 1));
        }
        Entry::Vacant(entry) => {
            entry.insert((offset, ts, 1));
        }
    }
}
