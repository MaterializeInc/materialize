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

use std::any::Any;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use differential_dataflow::Hashable;
use futures::stream::LocalBoxStream;
use futures::Stream;
use itertools::Itertools;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;
use prometheus::core::{AtomicI64, AtomicU64};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Broadcast, CapabilitySet};
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::scheduling::activate::SyncActivator;
use timely::scheduling::ActivateOnDrop;
use timely::Data;
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;
use tokio_stream::StreamExt;
use tracing::trace;

use mz_avro::types::Value;
use mz_expr::PartitionId;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operator::StreamExt as _;

use crate::controller::CollectionMetadata;
use crate::source::healthcheck::{Healthchecker, SourceStatusUpdate};
use crate::source::metrics::SourceBaseMetrics;
use crate::source::reclock::ReclockFollower;
use crate::source::reclock::ReclockOperator;
use crate::source::util::source;
use crate::types::connections::ConnectionContext;
use crate::types::errors::{DecodeError, SourceError, SourceErrorDetails};
use crate::types::sources::encoding::SourceDataEncoding;
use crate::types::sources::{MzOffset, SourceConnection};

pub mod generator;
mod healthcheck;
mod kafka;
mod kinesis;
pub mod metrics;
pub mod persist_source;
mod postgres;
mod reclock;
mod s3;
pub mod util;

pub use generator::LoadGeneratorSourceReader;
pub use kafka::KafkaSourceReader;
pub use kinesis::KinesisSourceReader;
pub use postgres::PostgresSourceReader;
pub use s3::S3SourceReader;

// Interval after which the source operator will yield control.
const YIELD_INTERVAL: Duration = Duration::from_millis(10);

/// Shared configuration information for all source types.
/// This is used in the `create_raw_source` functions, which
/// produce raw sources.
#[derive(Clone)]
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
    /// Granularity with which timestamps should be closed (and capabilities
    /// downgraded).
    pub timestamp_interval: Duration,
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
    /// A handle to the persist client cache
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
            NextMessage::Ready(SourceMessageType::DropPartitionCapabilities(pids)) => Ok(
                NextMessage::Ready(SourceMessageType::DropPartitionCapabilities(pids)),
            ),
            NextMessage::Ready(SourceMessageType::SourceStatus(update)) => {
                Ok(NextMessage::Ready(SourceMessageType::SourceStatus(update)))
            }
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

/// This trait defines the interface between Materialize and external sources,
/// and must be implemented for every new kind of source.
///
/// ## Contract between [`SourceReader`] and the ingestion framework
///
/// A source reader uses updates emitted from
/// [`SourceReader::next`]/[`SourceReader::get_next_message`] to update the
/// ingestion framework about new updates retrieved from the external system and
/// about its internal state.
///
/// The framework will spawn a [`SourceReader`] on each timely worker. It is the
/// responsibility of the reader to figure out which of the partitions (if any)
/// it is responsible for reading using [`crate::source::responsible_for`].
///
/// The reader implicitly is given a capability for emitting updates for each
/// new partition (identified by a [`PartitionId`]) that it discovers. It must
/// downgrade those capabilities by either emitting updates for those partitions
/// that it is responsible for or by emitting a
/// [`SourceMessageType::DropPartitionCapabilities`] for those partitions which
/// it is not responsible for.
//
// TODO: this trait is still a little too Kafka-centric, specifically the concept of
// a "partition" is baked into this trait and introduces some cognitive overhead as
// we are forced to treat things like file sources as "single-partition"
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
        timestamp_interval: Duration,
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
                Ok(NextMessage::Pending) => tokio::time::sleep(timestamp_interval).await,
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
        timestamp_interval: Duration,
    ) -> LocalBoxStream<
        'a,
        Result<SourceMessageType<Self::Key, Self::Value, Self::Diff>, SourceReaderError>,
    >
    where
        Self: Sized + 'a,
    {
        Box::pin(async_stream::stream!({
            while let Some(msg) = self.next(timestamp_interval).await {
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

/// A wrapper around [`SourceMessage`] that allows [`SourceReader`]'s to
/// communicate additional "maintenance" messages.
pub enum SourceMessageType<Key, Value, Diff> {
    /// Communicate that this [`SourceMessage`] is the final
    /// message its its offset.
    Finalized(SourceMessage<Key, Value, Diff>),
    /// Communicate that more [`SourceMessage`]'s
    /// will come later at the same offset as this one.
    InProgress(SourceMessage<Key, Value, Diff>),
    /// Information about the source status
    SourceStatus(SourceStatusUpdate),
    /// Signals that this [`SourceReader`] instance will never emit
    /// messages/updates for a given partition anymore. This is similar enough
    /// to a timely operator dropping a capability, hence the naming.
    ///
    /// We need these to compute a "global" source upper, when determining
    /// completeness of a timestamp.
    DropPartitionCapabilities(Vec<PartitionId>),
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

/// A batch of messages from a source reader, along with the current upper and
/// any errors that occured while reading that batch.
pub struct SourceMessageBatch<Key, Value, Diff> {
    messages: HashMap<PartitionId, Vec<(SourceMessage<Key, Value, Diff>, MzOffset)>>,
    /// Any errors that occured while obtaining this batch.
    // TODO: These non-definite errors should not show up in the dataflows/the
    // persist shard but it's the current "correct" behaviour. We need to fix
    // this as a follow-up issue because it's a bigger thing that breaks with
    // the current behaviour.
    non_definite_errors: Vec<SourceError>,
    /// The current upper of the `SourceReader`, at the time this batch was
    /// emitted. Source uppers of emitted batches must never regress.
    source_upper: HashMap<PartitionId, MzOffset>,
}

/// The source upper at the time of emitting a batch. This contains only the
/// partitions that a given source reader operator is responsible for, so a
/// downstream consumer needs summaries of all source reader operators in order
/// to form a full view of the upper.
#[derive(Clone, Serialize, Deserialize)]
pub struct SourceUpperSummary {
    source_upper: HashMap<PartitionId, MzOffset>,
}

/// Returns true if the given source id/worker id is responsible for handling the given
/// partition.
pub fn responsible_for(
    _source_id: &GlobalId,
    worker_id: usize,
    worker_count: usize,
    pid: &PartitionId,
) -> bool {
    // Distribute partitions equally amongst workers.
    (usize::cast_from(pid.hashed()) % worker_count) == worker_id
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

/// Creates a source dataflow operator graph from a connection that has a
/// corresponding [`SourceReader`] implementation. The type of SourceConnection
/// determines the type of connection that _should_ be created.
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
    Option<Rc<dyn Any>>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader,
{
    let ((batches, source_upper_summaries), source_reader_token) =
        source_reader_operator::<G, S>(config.clone(), source_connection, connection_context);

    let (remap_stream, remap_token) =
        remap_operator::<G, S>(config.clone(), source_upper_summaries);

    let ((reclocked_stream, reclocked_err_stream), _reclock_token) =
        reclock_operator::<G, S>(config, batches, remap_stream);

    let token = Rc::new((source_reader_token, remap_token));

    ((reclocked_stream, reclocked_err_stream), Some(token))
}

/// Reads from a [`SourceReader`] and returns a stream of "un-timestamped"
/// [`SourceMessageBatch`]. Also returns a second stream that can be used to
/// learn about the `source_upper` that all the source reader instances now
/// about. This second stream will be used by `remap_operator` to mint new
/// timestamp bindings into the remap shard.
pub fn source_reader_operator<G, S: 'static>(
    config: RawSourceCreationConfig<G>,
    source_connection: &SourceConnection,
    connection_context: ConnectionContext,
) -> (
    (
        timely::dataflow::Stream<
            G,
            Rc<RefCell<Option<SourceMessageBatch<S::Key, S::Value, S::Diff>>>>,
        >,
        timely::dataflow::Stream<G, SourceUpperSummary>,
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
        timestamp_interval,
        encoding,
        storage_metadata,
        resume_upper,
        base_metrics,
        now,
        persist_clients,
    } = config;

    let (stream, capability) = source(scope, name.clone(), move |info| {
        let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
        let waker = futures::task::waker(waker_activator);

        let metrics_name = upstream_name.clone().unwrap_or_else(|| name.clone());
        let source_metrics =
            SourceMetrics::new(base_metrics, &metrics_name, id, &worker_id.to_string());

        let sync_activator = scope.sync_activator_for(&info.address[..]);
        let base_metrics = base_metrics.clone();
        let source_connection = source_connection.clone();
        let mut source_reader = Box::pin(async_stream::stream!({
            let upper_ts = resume_upper.as_option().copied().unwrap();
            let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
            let timestamper = match ReclockOperator::new(
                Arc::clone(&persist_clients),
                storage_metadata.clone(),
                now.clone(),
                timestamp_interval.clone(),
                as_of,
            )
            .await
            {
                Ok(t) => t,
                Err(e) => {
                    panic!("Failed to create source {} timestamper: {:#}", name, e);
                }
            };

            let mut healthchecker = if storage_metadata.status_shard.is_some() {
                match Healthchecker::new(
                    name.clone(),
                    upstream_name,
                    id,
                    source_connection.name(),
                    worker_id,
                    worker_count,
                    true,
                    &persist_clients,
                    &storage_metadata,
                    now.clone(),
                )
                .await
                {
                    Ok(h) => Some(h),
                    Err(e) => {
                        panic!(
                            "Failed to create healthchecker for source {}: {:#}",
                            &name, e
                        );
                    }
                }
            } else {
                None
            };

            let mut source_upper = timestamper.source_upper_at(upper_ts.saturating_sub(1));

            // Send along an empty batch, so that the reclock operator knows
            // about the current frontier. Otherwise, if there are no new
            // messages after a restart, the reclock operator would be stuck and
            // not advance its downstream frontier.
            yield Some((HashMap::new(), Vec::new(), Vec::new(), source_upper.clone()));

            trace!("source_reader({id}) {worker_id}/{worker_count}: source_upper before thinning: {source_upper:?}");
            source_upper.retain(|pid, _offset| {
                crate::source::responsible_for(&id, worker_id, worker_count, &pid)
            });
            trace!("source_reader({id}) {worker_id}/{worker_count}: source_upper after thinning: {source_upper:?}");

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
                .into_stream(timestamp_interval)
                .fuse();

            tokio::pin!(source_stream);

            // Emit batches more frequently than we mint new timestamps. We're
            // hoping that most of the batches that we emit will end up making
            // it into the freshly minted bindings when remap_operator ticks.
            let mut emission_interval = tokio::time::interval(timestamp_interval / 5);
            emission_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut untimestamped_messages = HashMap::<_, Vec<_>>::new();
            let mut unconsumed_partitions = Vec::new();
            let mut non_definite_errors = vec![];
            loop {
                // TODO(guswyn): move lots of this out of the macro so rustfmt works better
                // TODO(aljoscha): Get rid of the async stream here. To ease my
                // cancel-safety anxiety.
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
                                    SourceMessageType::DropPartitionCapabilities(mut pids) => {
                                        unconsumed_partitions.append(&mut pids);
                                    }
                                    SourceMessageType::Finalized(message) | SourceMessageType::InProgress(message) => {
                                        let pid = message.partition.clone();
                                        let offset = message.offset;
                                        // advance the _offset_ frontier if this the final message for that offset
                                        if is_final {
                                            *source_upper.entry(pid.clone()).or_default() = offset + 1;
                                        }
                                        untimestamped_messages.entry(pid).or_default().push((message, offset));
                                    }
                                    SourceMessageType::SourceStatus(update) => {
                                        if let Some(healthchecker) = &mut healthchecker {
                                            healthchecker.update_status(update).await;
                                        }
                                    }
                                }
                            }
                            // TODO: Report these errors to the Healthchecker!
                            Some(Err(e)) => {
                                non_definite_errors.push(e);
                            }
                            None => {
                                // This source reader is done. Yield one final
                                // update of the source_upper.
                                yield Some(
                                    (
                                        std::mem::take(&mut untimestamped_messages),
                                        non_definite_errors.drain(..).collect_vec(),
                                        unconsumed_partitions,
                                        source_upper.clone()
                                    )
                                );

                                // Then, let the consumer know we're done.
                                yield None;

                                return;
                            },
                        }
                    }
                    // It's time to emit a batch of messages
                    _ = emission_interval.tick() => {

                        if !untimestamped_messages.is_empty() {
                            trace!("source_reader({id}) {worker_id}/{worker_count}: \
                                  emitting new batch. \
                                  untimestamped_messages.len(): {} \
                                  unconsumed_partitions: {:?} \
                                  source_upper: {:?}",
                                  untimestamped_messages.len(),
                                  unconsumed_partitions.clone(),
                                  source_upper);
                        }

                        // Emit empty batches as well. Just to keep downstream
                        // operators informed about the unconsumed partitions
                        // and the source upper.
                        yield Some(
                            (
                                std::mem::take(&mut untimestamped_messages),
                                non_definite_errors.drain(..).collect_vec(),
                                unconsumed_partitions.clone(),
                                source_upper.clone()
                            )
                        );
                    }
                }
            }
        }));

        let activator = scope.activator_for(&info.address[..]);
        move |cap_set, output| {
            // Record operator has been scheduled
            // WIP: Should we have these metrics for all three involved
            // operators?
            source_metrics.operator_scheduled_counter.inc();

            let mut context = Context::from_waker(&waker);

            let timer = Instant::now();

            // We just use an advancing number for our capability. No one cares
            // about what this actually is, downstream.
            let mut batch_counter = 0;

            while let Poll::Ready(Some(update)) = source_reader.as_mut().poll_next(&mut context) {
                let (messages, non_definite_errors, unconsumed_partitions, source_upper) =
                    if let Some(update) = update {
                        update
                    } else {
                        trace!("source_reader({id}) {worker_id}/{worker_count}: is terminated");
                        // We will never produce more data, clear our capabilities to
                        // communicate this downstream.
                        cap_set.downgrade(&[]);
                        return;
                    };

                trace!(
                    "create_source_raw({id}) {worker_id}/{worker_count}: message_batch.len(): {:?}",
                    messages.len()
                );
                trace!(
                    "create_source_raw({id}) {worker_id}/{worker_count}: source_upper: {:?}",
                    source_upper
                );

                // We forward only the partitions that we are responsible for to
                // the remap operator.
                let source_upper_summary = SourceUpperSummary {
                    source_upper: source_upper.clone(),
                };

                // Pull the upper to `max` for partitions that we are not
                // responsible for. That way, the downstream reclock operator
                // can correcly decide when a reclocked timestamp is closed.
                // We basically take those partitions "out of the calculation".
                let mut extended_source_upper = source_upper.clone();
                extended_source_upper.extend(
                    unconsumed_partitions
                        .into_iter()
                        .map(|pid| (pid, MzOffset { offset: u64::MAX })),
                );

                let non_definite_errors = non_definite_errors
                    .into_iter()
                    .map(|e| SourceError {
                        source_id: id,
                        error: e.inner,
                    })
                    .collect_vec();

                let message_batch = SourceMessageBatch {
                    messages,
                    non_definite_errors,
                    source_upper: extended_source_upper,
                };
                // Wrap in an Rc to avoid cloning when sending it on.
                let message_batch = Rc::new(RefCell::new(Some(message_batch)));

                let cap = cap_set.delayed(&batch_counter);
                let mut session = output.session(&cap);

                session.give((message_batch, source_upper_summary));

                batch_counter += 1;

                if timer.elapsed() > YIELD_INTERVAL {
                    let _ = activator.activate();
                    break;
                }
            }
        }
    });

    // TODO: Roll all this into one source operator.
    let operator_name = format!("source_reader({})-demux", id);
    let mut demux_op = OperatorBuilder::new(operator_name, scope.clone());

    let mut input = demux_op.new_input(&stream, Pipeline);
    let (mut batch_output, batch_stream) = demux_op.new_output();
    let (mut summary_output, summary_stream) = demux_op.new_output();
    let summary_output_port = summary_stream.name().port;

    demux_op.build(move |_caps| {
        let mut buffer = Vec::new();

        move |_frontiers| {
            input.for_each(|cap, data| {
                data.swap(&mut buffer);

                let mut batch_output = batch_output.activate();
                let mut summary_output = summary_output.activate();

                for (message_batch, source_upper) in buffer.drain(..) {
                    let mut session = batch_output.session(&cap);
                    session.give(message_batch);

                    let summary_cap = cap.delayed_for_output(cap.time(), summary_output_port);
                    let mut session = summary_output.session(&summary_cap);
                    session.give(source_upper);
                }
            });
        }
    });

    ((batch_stream, summary_stream), Some(capability))
}

/// Mints new contents for the remap shard based on summaries about the source
/// upper it receives from the raw reader operators.
///
/// Only one worker will be active and write to the remap shard. All source
/// upper summaries will be exchanged to it.
pub fn remap_operator<G, S: 'static>(
    config: RawSourceCreationConfig<G>,
    source_upper_summaries: timely::dataflow::Stream<G, SourceUpperSummary>,
) -> (
    timely::dataflow::Stream<G, Vec<(PartitionId, Vec<(u64, MzOffset)>)>>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader,
{
    let RawSourceCreationConfig {
        name,
        upstream_name: _,
        id,
        scope,
        worker_id,
        worker_count,
        timestamp_granularity,
        encoding: _,
        storage_metadata,
        resume_upper,
        base_metrics: _,
        now,
        persist_clients,
    } = config;

    let chosen_worker = (id.hashed() % worker_count as u64) as usize;
    let active_worker = chosen_worker == worker_id;

    let operator_name = format!("remap({})", id);
    let mut remap_op = OperatorBuilder::new(operator_name, scope.clone());
    let (mut remap_output, remap_stream) = remap_op.new_output();

    let mut input = remap_op.new_input_connection(
        &source_upper_summaries,
        Exchange::new(move |_x| chosen_worker as u64),
        // We don't want frontier information to flow from the input to the
        // output. This operator is it's own "root source" of capabilities for
        // current reclocked, wall-clock time.
        vec![Antichain::new()],
    );

    let activator = scope.activator_for(&remap_op.operator_info().address[..]);

    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);

    remap_op.build_async(
        scope.clone(),
        move |mut capabilities, frontiers, scheduler| async move {
            let mut buffer = Vec::new();
            let mut cap_set = if active_worker {
                CapabilitySet::from_elem(capabilities.pop().expect("missing capability"))
            } else {
                CapabilitySet::new()
            };
            // Explicitly release the unneeded capabilities!
            capabilities.clear();

            let upper_ts = resume_upper.as_option().copied().unwrap();
            let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
            let mut timestamper = match ReclockOperator::new(
                Arc::clone(&persist_clients),
                storage_metadata.clone(),
                now.clone(),
                timestamp_granularity.clone(),
                as_of,
            )
            .await
            {
                Ok(t) => t,
                Err(e) => {
                    panic!("Failed to create source {} timestamper: {:#}", name, e);
                }
            };
            // The global view of the source_upper, which we track by combining
            // summaries from the raw reader operators.
            let mut global_source_upper = timestamper.source_upper_at(upper_ts.saturating_sub(1));

            if active_worker {
                let new_ts_upper = timestamper
                    .reclock_frontier(&global_source_upper)
                    .expect("compacted past upper");

                cap_set.downgrade(new_ts_upper);

                // Emit initial snapshot of the remap_shard, bootstrapping
                // downstream reclock operators.
                let remap_trace = timestamper.remap_trace();
                trace!(
                    "remap({id}) {worker_id}/{worker_count}: \
                    emitting initial remap_trace. \
                    source_upper: {:?} \
                    trace_updates: {:?}",
                    global_source_upper,
                    remap_trace
                );

                let mut remap_output = remap_output.activate();
                let cap = cap_set.delayed(cap_set.first().unwrap());
                let mut session = remap_output.session(&cap);
                session.give(remap_trace);
            }

            while scheduler.notified().await {
                if token_weak.upgrade().is_none() {
                    // Make sure we don't accidentally mint new updates when this
                    // source has been dropped. This way, we also make sure to not
                    // react to spurious frontier advancements to `[]` that happen
                    // when the input source operator is shutting down.
                    return;
                }

                if !active_worker {
                    // We simply loop because we cannot return here. Otherwise
                    // the one active worker would not get frontier upgrades
                    // anymore or other things could break.
                    // WIP: Is this correct?
                    continue;
                }

                input.for_each(|_cap, data| {
                    data.swap(&mut buffer);

                    for source_upper_summary in buffer.drain(..) {
                        for (pid, offset) in source_upper_summary.source_upper {
                            let previous_offset = global_source_upper.insert(pid, offset);
                            if let Some(previous_offset) = previous_offset {
                                assert!(previous_offset <= offset);
                            }
                        }
                    }
                });

                if let Err(wait_time) = timestamper.next_mint_timestamp() {
                    activator.activate_after(wait_time);
                    continue;
                }

                let remap_trace_updates = timestamper.mint(&global_source_upper).await;
                let mut remap_output = remap_output.activate();
                let cap = cap_set.delayed(cap_set.first().unwrap());
                let mut session = remap_output.session(&cap);

                timestamper.advance().await;
                let new_ts_upper = timestamper
                    .reclock_frontier(&global_source_upper)
                    .expect("compacted past upper");

                trace!(
                    "remap({id}) {worker_id}/{worker_count}: minted new bindings. \
                    source_upper: {:?} \
                    trace_updates: {:?} \
                    new_ts_upper: {:?}",
                    global_source_upper,
                    remap_trace_updates,
                    new_ts_upper
                );

                session.give(remap_trace_updates);

                cap_set.downgrade(new_ts_upper);

                // Make sure we do this after writing any timestamp bindings to
                // the remap shard that might be needed for the reported source
                // uppers.
                let input_frontier = &frontiers.borrow()[0];
                if input_frontier.is_empty() {
                    cap_set.downgrade(&[]);
                    return;
                }
            }
        },
    );

    (remap_stream, token)
}

/// Receives un-timestamped batches from the source reader and updates to the
/// remap trace on a second input. This operator takes the remap information,
/// reclocks incoming batches and sends them forward.
pub fn reclock_operator<G, S: 'static>(
    config: RawSourceCreationConfig<G>,
    batches: timely::dataflow::Stream<
        G,
        Rc<RefCell<Option<SourceMessageBatch<S::Key, S::Value, S::Diff>>>>,
    >,
    remap_trace_updates: timely::dataflow::Stream<G, Vec<(PartitionId, Vec<(u64, MzOffset)>)>>,
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
        timestamp_granularity: _,
        encoding: _,
        storage_metadata: _,
        resume_upper,
        base_metrics,
        now: _,
        persist_clients: _,
    } = config;

    let bytes_read_counter = base_metrics.bytes_read.clone();

    let operator_name = format!("reclock({})", id);
    let mut remap_op = OperatorBuilder::new(operator_name, scope.clone());
    let (mut reclocked_output, reclocked_stream) = remap_op.new_output();

    let mut batch_input = remap_op.new_input_connection(
        &batches,
        Pipeline,
        // We don't want frontier information to flow from the input to the
        // output. The second input (the remap information) should be the stream
        // that drives downstream timestamps.
        vec![Antichain::new()],
    );
    // Need to broadcast the remap changes to all workers.
    let remap_trace_updates = remap_trace_updates.broadcast();
    let mut remap_input = remap_op.new_input(&remap_trace_updates, Pipeline);

    remap_op.build(move |mut capabilities| {
        capabilities.clear();

        let metrics_name = upstream_name.clone().unwrap_or_else(|| name.clone());
        let mut source_metrics =
            SourceMetrics::new(base_metrics, &metrics_name, id, &worker_id.to_string());

        // Use this to retain capabilities from the remap_operator input.
        let mut cap_set = CapabilitySet::new();
        let mut batch_buffer = Vec::new();
        let mut remap_trace_buffer = Vec::new();

        // The global view of the source_upper, which we track by combining
        // summaries from the raw reader operators.
        let mut global_source_upper = HashMap::new();
        let mut untimestamped_batches = Vec::new();

        let upper_ts = resume_upper.as_option().copied().unwrap();
        let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
        let mut timestamper = ReclockFollower::new(as_of);

        move |frontiers| {
            batch_input.for_each(|_cap, data| {
                data.swap(&mut batch_buffer);
                for batch in batch_buffer.drain(..) {
                    let batch = batch
                        .borrow_mut()
                        .take()
                        .expect("batch already taken, but we should be the only consumer");
                    for (pid, offset) in batch.source_upper.iter() {
                        let previous_offset = global_source_upper.insert(pid.clone(), *offset);
                        if let Some(previous_offset) = previous_offset {
                            assert!(previous_offset <= *offset);
                        }
                    }
                    untimestamped_batches.push(batch);
                }
            });

            remap_input.for_each(|cap, data| {
                data.swap(&mut remap_trace_buffer);
                for update in remap_trace_buffer.drain(..) {
                    timestamper.push_trace_updates(update.into_iter())
                }
                cap_set.insert(cap.retain());
            });

            let remap_frontier = &frontiers[1];
            trace!(
                "reclock({id}) {worker_id}/{worker_count}: remap frontier: {:?}",
                remap_frontier.frontier()
            );
            timestamper.push_upper_update(remap_frontier.frontier().to_owned());

            // Accumulate updates to bytes_read for Prometheus metrics collection
            let mut bytes_read = 0;
            // Accumulate updates to offsets for system table metrics collection
            let mut metric_updates = HashMap::new();

            trace!(
                "reclock({id}) {worker_id}/{worker_count}: \
                untimestamped_batches.len(): {}",
                untimestamped_batches.len(),
            );

            // WIP: Be smarter about this.
            while let Some(untimestamped_batch) = untimestamped_batches.first_mut() {
                let reclocked = match timestamper.reclock(&mut untimestamped_batch.messages) {
                    Ok(reclocked) => reclocked,
                    Err((pid, offset)) => panic!("failed to reclock {} @ {}", pid, offset),
                };

                if let Some(reclocked) = reclocked {
                    let mut output = reclocked_output.activate();

                    for (_, part_messages) in reclocked {
                        for (message, ts) in part_messages {
                            trace!(
                                "reclock({id}) {worker_id}/{worker_count}: \
                                handling reclocked message: {:?}:{:?} -> {}",
                                message.partition,
                                message.offset,
                                ts
                            );
                            handle_message::<S>(
                                message,
                                &mut bytes_read,
                                &cap_set,
                                &mut output,
                                &mut metric_updates,
                                ts,
                            )
                        }
                    }

                    // TODO: We should not emit the non-definite errors as
                    // DataflowErrors, which will make them end up on the
                    // persist shard for this source. Instead they should be
                    // reported to the Healthchecker. But that's future work.
                    if !untimestamped_batch.non_definite_errors.is_empty() {
                        // If there are errors, it means that someone must also
                        // have given us a capability because a
                        // batch/batch-summary was emitted to the remap
                        // operator.
                        let err_cap = cap_set.delayed(
                            cap_set
                                .first()
                                .expect("missing a capability for emitting errors"),
                        );
                        let mut session = output.session(&err_cap);
                        let errors = untimestamped_batch
                            .non_definite_errors
                            .iter()
                            .map(|e| Err(e.clone()));
                        session.give_iterator(errors);
                    }

                    // Pop off the processed batch.
                    untimestamped_batches.remove(0);
                } else {
                    trace!(
                        "reclock({id}) {worker_id}/{worker_count}: \
                        cannot yet reclock batch with source frontier {:?} \
                        reclock.source_frontier: {:?}",
                        untimestamped_batch.source_upper,
                        timestamper.source_upper
                    );
                    // We keep batches in the order they arrive from the
                    // source. And we assume that the source frontier never
                    // regressses. So we can break now.
                    break;
                }
            }

            bytes_read_counter.inc_by(bytes_read as u64);
            source_metrics.record_partition_offsets(metric_updates);

            // This is correct for totally ordered times because there can be at
            // most one entry in the `CapabilitySet`. If this ever changes we
            // need to rethink how we surface this in metrics. We will notice
            // when that happens because the `expect()` will fail.
            source_metrics.capability.set(
                cap_set
                    .iter()
                    .at_most_one()
                    .expect("there can be at most one element for totally ordered times")
                    .map(|c| c.time())
                    .cloned()
                    .unwrap_or(Timestamp::MAX),
            );

            // It can happen that our view of the global source_upper is not yet
            // up to date with what the ReclockOperator thinks. Ignore that for
            // now.
            if let Ok(new_ts_upper) = timestamper.reclock_frontier(&global_source_upper) {
                let ts = new_ts_upper.as_option().cloned().unwrap_or(Timestamp::MAX);
                for partition_metrics in source_metrics.partition_metrics.values_mut() {
                    partition_metrics.closed_ts.set(ts);
                }

                if !cap_set.is_empty() {
                    trace!(
                        "reclock({id}) {worker_id}/{worker_count}: \
                        downgrading to {:?}",
                        new_ts_upper
                    );

                    cap_set
                        .try_downgrade(new_ts_upper.iter())
                        .expect("cannot downgrade in reclock");
                }
            }
        }
    });

    let (ok_stream, err_stream) = reclocked_stream.map_fallible("reclock-demux", |r| r);

    ((ok_stream, err_stream), None)
}

/// Take `message` and assign it the appropriate timestamps and push it into the
/// dataflow layer, if possible.
///
/// TODO: This function is a bit of a mess rn but hopefully this function makes the
/// existing mess more obvious and points towards ways to improve it.
fn handle_message<S: SourceReader>(
    message: SourceMessage<S::Key, S::Value, S::Diff>,
    bytes_read: &mut usize,
    cap_set: &CapabilitySet<Timestamp>,
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
    let ts_cap = cap_set.delayed(&ts);
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
