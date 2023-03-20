// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the source ingestion pipeline/framework.

// https://github.com/tokio-rs/prost/issues/237
// #![allow(missing_docs)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use differential_dataflow::difference::Semigroup;
use futures::stream::LocalBoxStream;
use prometheus::core::{AtomicI64, AtomicU64};
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::Capability;
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::activate::SyncActivator;

use mz_expr::PartitionId;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_repr::{GlobalId, Row};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::errors::{DecodeError, SourceErrorDetails};
use mz_storage_client::types::sources::encoding::SourceDataEncoding;
use mz_storage_client::types::sources::{MzOffset, SourceTimestamp};

use crate::source::metrics::SourceBaseMetrics;
use crate::source::source_reader_pipeline::HealthStatus;

/// Extension trait to the SourceConnection trait that defines how to intantiate a particular
/// connetion into a reader and offset committer
pub trait SourceConnectionBuilder {
    type Reader: SourceReader + 'static;
    type OffsetCommitter: OffsetCommitter<<Self::Reader as SourceReader>::Time>
        + Send
        + Sync
        + 'static;

    /// Turn this connection into a new source reader.
    ///
    /// This function returns the source reader and its corresponding offset committed.
    fn into_reader(
        self,
        source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        data_capability: Capability<<Self::Reader as SourceReader>::Time>,
        upper_capability: Capability<<Self::Reader as SourceReader>::Time>,
        resume_upper: Antichain<<Self::Reader as SourceReader>::Time>,
        encoding: SourceDataEncoding,
        metrics: crate::source::metrics::SourceBaseMetrics,
        connection_context: ConnectionContext,
    ) -> Result<(Self::Reader, Self::OffsetCommitter), anyhow::Error>;
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
#[async_trait(?Send)]
pub trait SourceReader {
    type Key: timely::Data + MaybeLength;
    type Value: timely::Data + MaybeLength;
    type Time: SourceTimestamp;
    type Diff: timely::Data + Semigroup;

    /// Returns the next message available from the source.
    async fn next(
        &mut self,
        timestamp_granularity: Duration,
    ) -> Option<SourceMessageType<Self::Key, Self::Value, Self::Time, Self::Diff>> {
        // Compatiblity implementation that delegates to the deprecated [Self::get_next_method]
        // call. Once all source implementations have been transitioned to implement
        // [SourceReader::next] directly this provided implementation should be removed and the
        // method should become a required method.
        loop {
            match self.get_next_message() {
                NextMessage::Ready(msg) => return Some(msg),
                // There was a temporary hiccup in getting messages, check again asap.
                NextMessage::TransientDelay => tokio::time::sleep(Duration::from_millis(1)).await,
                // There were no new messages, check again after a delay
                NextMessage::Pending => tokio::time::sleep(timestamp_granularity).await,
                NextMessage::Finished => return None,
            }
        }
    }

    /// Returns the next message available from the source.
    ///
    /// # Deprecated
    ///
    /// Source implementation should implement the async [SourceReader::next] method instead.
    fn get_next_message(&mut self) -> NextMessage<Self::Key, Self::Value, Self::Time, Self::Diff> {
        NextMessage::Pending
    }

    /// Returns an adapter that treats the source as a stream.
    ///
    /// The stream produces the messages that would be produced by repeated calls to `next`.
    fn into_stream<'a>(
        mut self,
        timestamp_granularity: Duration,
    ) -> LocalBoxStream<'a, SourceMessageType<Self::Key, Self::Value, Self::Time, Self::Diff>>
    where
        Self: Sized + 'a,
    {
        Box::pin(async_stream::stream!({
            while let Some(msg) = self.next(timestamp_granularity).await {
                yield msg;
            }
        }))
    }
}

/// A sibling trait to `SourceReader` that represents a source's ability to commit the frontier
/// that all updates that materialize may need in the future will be beyond of.
#[async_trait]
pub trait OffsetCommitter<Time: SourceTimestamp> {
    /// Commit the given frontier upstream. When this method is called permission is given to the
    /// source to delete all updates that are not beyond this frontier and the system promises to
    /// never request them again.
    async fn commit_offsets(&self, offsets: Antichain<Time>) -> Result<(), anyhow::Error>;
}

pub enum NextMessage<Key, Value, Time: Timestamp, Diff> {
    Ready(SourceMessageType<Key, Value, Time, Diff>),
    Pending,
    TransientDelay,
    Finished,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct HealthStatusUpdate {
    pub update: HealthStatus,
    pub should_halt: bool,
}

/// A wrapper around [`SourceMessage`] that allows [`SourceReader`]'s to
/// communicate additional "maintenance" messages.
#[derive(Debug)]
pub enum SourceMessageType<Key, Value, Time: Timestamp, Diff> {
    /// A source message
    Message(
        Result<SourceMessage<Key, Value>, SourceReaderError>,
        Capability<Time>,
        Diff,
    ),
    /// Information about the source status
    SourceStatus(HealthStatusUpdate),
}

impl<Key, Value, Time: Timestamp, Diff> SourceMessageType<Key, Value, Time, Diff> {
    pub fn status(update: HealthStatus) -> Self {
        SourceMessageType::SourceStatus(HealthStatusUpdate {
            update,
            should_halt: false,
        })
    }
}

/// Source-agnostic wrapper for messages. Each source must implement a
/// conversion to Message.
#[derive(Debug, Clone)]
pub struct SourceMessage<Key, Value> {
    /// The output stream this message belongs to. Later in the pipeline the stream is partitioned
    /// based on this value and is fed to the appropriate source exports
    pub output: usize,
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

/// A record produced by a source
#[derive(Clone, Serialize, Debug, Deserialize)]
pub struct SourceOutput<K, V> {
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
}

impl<K, V> SourceOutput<K, V> {
    /// Build a new SourceOutput
    pub fn new(
        key: K,
        value: V,
        position: MzOffset,
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

/// The output of the decoding operator
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct DecodeResult {
    /// The decoded key
    pub key: Option<Result<Row, DecodeError>>,
    /// The decoded value, as well as the the
    /// differential `diff` value for this value, if the value
    /// is present and not and error.
    pub value: Option<Result<Row, DecodeError>>,
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

/// A structured error for `SourceReader::get_next_message` implementors.
#[derive(Debug, Clone)]
pub struct SourceReaderError {
    pub inner: SourceErrorDetails,
}

impl SourceReaderError {
    /// This is an unclassified but definite error. This is typically only appropriate
    /// when the error is permanently fatal for the source... some critical invariant
    /// is violated or data is corrupted, for example.
    pub fn other_definite(e: anyhow::Error) -> SourceReaderError {
        SourceReaderError {
            inner: SourceErrorDetails::Other(format!("{}", e)),
        }
    }
}

/// Source-specific metrics in the persist sink
pub struct SourcePersistSinkMetrics {
    pub(crate) progress: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    pub(crate) row_inserts: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) row_retractions: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) error_inserts: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) error_retractions: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) processed_batches: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,

    // TODO(guswynn): consider if this metric (and others) should be put into `StorageState`...
    pub(crate) enable_multi_worker_storage_persist_sink:
        DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
}

impl SourcePersistSinkMetrics {
    /// Initialises source metrics used in the `persist_sink`.
    pub fn new(
        base: &SourceBaseMetrics,
        source_id: GlobalId,
        parent_source_id: GlobalId,
        worker_id: usize,
        shard_id: &mz_persist_client::ShardId,
        output_index: usize,
    ) -> SourcePersistSinkMetrics {
        let shard = shard_id.to_string();
        SourcePersistSinkMetrics {
            enable_multi_worker_storage_persist_sink: base
                .source_specific
                .enable_multi_worker_storage_persist_sink
                .get_delete_on_drop_gauge(vec![
                    source_id.to_string(),
                    worker_id.to_string(),
                    parent_source_id.to_string(),
                    output_index.to_string(),
                ]),
            progress: base.source_specific.progress.get_delete_on_drop_gauge(vec![
                parent_source_id.to_string(),
                output_index.to_string(),
                shard.clone(),
            ]),
            row_inserts: base
                .source_specific
                .row_inserts
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard.clone(),
                ]),
            row_retractions: base
                .source_specific
                .row_retractions
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard.clone(),
                ]),
            error_inserts: base
                .source_specific
                .error_inserts
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard.clone(),
                ]),
            error_retractions: base
                .source_specific
                .error_retractions
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard.clone(),
                ]),
            processed_batches: base
                .source_specific
                .persist_sink_processed_batches
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard,
                ]),
        }
    }
}

/// Source-specific Prometheus metrics
pub struct SourceMetrics {
    /// Value of the capability associated with this source
    pub(crate) capability: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The resume_upper for a source.
    pub(crate) resume_upper: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// Per-partition Prometheus metrics.
    pub(crate) partition_metrics: BTreeMap<PartitionId, PartitionMetrics>,
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
            capability: base
                .source_specific
                .capability
                .get_delete_on_drop_gauge(labels.to_vec()),
            resume_upper: base
                .source_specific
                .resume_upper
                .get_delete_on_drop_gauge(vec![source_id.to_string()]),
            partition_metrics: Default::default(),
            source_name: source_name.to_string(),
            source_id,
            base_metrics: base.clone(),
        }
    }

    /// Log updates to which offsets / timestamps read up to.
    pub fn record_partition_offsets(
        &mut self,
        offsets: BTreeMap<PartitionId, (MzOffset, mz_repr::Timestamp, i64)>,
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
                i64::try_from(timestamp).expect("materialize exists after 250M AD"),
            );
        }
    }
}

/// Partition-specific metrics, recorded to both Prometheus and a system table
pub struct PartitionMetrics {
    /// Highest offset that has been received by the source and timestamped
    pub(crate) offset_ingested: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Highest offset that has been received by the source
    pub(crate) offset_received: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Value of the highest timestamp that is closed (for which all messages have been ingested)
    pub(crate) closed_ts: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Total number of messages that have been received by the source and timestamped
    pub(crate) messages_ingested: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
    pub(crate) last_offset: u64,
    pub(crate) last_timestamp: i64,
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

/// Source reader operator specific Prometheus metrics
pub struct SourceReaderMetrics {
    /// Per-partition Prometheus metrics.
    pub(crate) partition_metrics: BTreeMap<PartitionId, SourceReaderPartitionMetrics>,
    source_id: GlobalId,
    base_metrics: SourceBaseMetrics,
}

impl SourceReaderMetrics {
    /// Initialises source metrics for a given (source_id, worker_id)
    pub fn new(base: &SourceBaseMetrics, source_id: GlobalId) -> SourceReaderMetrics {
        SourceReaderMetrics {
            partition_metrics: Default::default(),
            source_id,
            base_metrics: base.clone(),
        }
    }

    /// Log updates to which offsets / timestamps read up to.
    pub fn metrics_for_partition(&mut self, pid: &PartitionId) -> &SourceReaderPartitionMetrics {
        self.partition_metrics
            .entry(pid.clone())
            .or_insert_with(|| {
                SourceReaderPartitionMetrics::new(&self.base_metrics, self.source_id, pid)
            })
    }

    /// Get metrics struct for offset committing.
    pub fn offset_commit_metrics(&self) -> OffsetCommitMetrics {
        OffsetCommitMetrics::new(&self.base_metrics, self.source_id)
    }
}

/// Partition-specific metrics, recorded to both Prometheus and a system table
pub struct SourceReaderPartitionMetrics {
    /// The offset-domain resume_upper for a source.
    pub(crate) source_resume_upper: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl SourceReaderPartitionMetrics {
    /// Initialises partition metrics for a given (source_id, partition_id)
    pub fn new(
        base_metrics: &SourceBaseMetrics,
        source_id: GlobalId,
        partition_id: &PartitionId,
    ) -> SourceReaderPartitionMetrics {
        let base = &base_metrics.partition_specific;
        SourceReaderPartitionMetrics {
            source_resume_upper: base
                .source_resume_upper
                .get_delete_on_drop_gauge(vec![source_id.to_string(), partition_id.to_string()]),
        }
    }
}

/// Metrics about committing offsets
pub struct OffsetCommitMetrics {
    /// The offset-domain resume_upper for a source.
    pub(crate) offset_commit_failures: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl OffsetCommitMetrics {
    /// Initialises partition metrics for a given (source_id, partition_id)
    pub fn new(base_metrics: &SourceBaseMetrics, source_id: GlobalId) -> OffsetCommitMetrics {
        let base = &base_metrics.source_specific;
        OffsetCommitMetrics {
            offset_commit_failures: base
                .offset_commit_failures
                .get_delete_on_drop_counter(vec![source_id.to_string()]),
        }
    }
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

impl<T: MaybeLength> MaybeLength for Option<T> {
    fn len(&self) -> Option<usize> {
        self.as_ref().and_then(|v| v.len())
    }
}
