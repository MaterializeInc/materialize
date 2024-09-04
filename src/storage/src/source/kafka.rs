// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::str::{self};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime};
use differential_dataflow::AsCollection;
use futures::StreamExt;
use maplit::btreemap;
use mz_kafka_util::client::{get_partitions, MzClientContext, PartitionId, TunnelingClientContext};
use mz_ore::assert_none;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::thread::{JoinHandleExt, UnparkOnDropHandle};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{adt::jsonb::Jsonb, Datum, Diff, GlobalId, Row};
use mz_ssh_util::tunnel::SshTunnelStatus;
use mz_storage_types::errors::{
    ContextCreationError, DataflowError, SourceError, SourceErrorDetails,
};
use mz_storage_types::sources::kafka::{
    KafkaMetadataKind, KafkaSourceConnection, KafkaTimestamp, RangeBound,
};
use mz_storage_types::sources::{
    IndexedSourceExport, MzOffset, SourceExport, SourceExportDetails, SourceTimestamp,
};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use mz_timely_util::containers::stack::AccountedStackBuilder;
use mz_timely_util::order::Partitioned;
use rdkafka::client::Client;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientContext, Message, TopicPartitionList};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::PartialOrder;
use tokio::sync::Notify;
use tracing::{error, info, trace};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::metrics::source::kafka::KafkaSourceMetrics;
use crate::source::types::{
    ProgressStatisticsUpdate, SignaledFuture, SourceRender, StackedCollection,
};
use crate::source::{RawSourceCreationConfig, SourceMessage};

#[derive(Default)]
struct HealthStatus {
    kafka: Option<HealthStatusUpdate>,
    ssh: Option<HealthStatusUpdate>,
}

/// Contains all information necessary to ingest data from Kafka
pub struct KafkaSourceReader {
    /// Name of the topic on which this source is backed on
    topic_name: String,
    /// Name of the source (will have format kafka-source-id)
    source_name: String,
    /// Source global ID
    id: GlobalId,
    /// Kafka consumer for this source
    consumer: Arc<BaseConsumer<TunnelingClientContext<GlueConsumerContext>>>,
    /// List of consumers. A consumer should be assigned per partition to guarantee fairness
    partition_consumers: Vec<PartitionConsumer>,
    /// Worker ID
    worker_id: usize,
    /// Total count of workers
    worker_count: usize,
    /// The most recently read offset for each partition known to this source
    /// reader by output-index. An offset of -1 indicates that no prior message
    /// has been read for the given partition.
    last_offsets: BTreeMap<usize, BTreeMap<PartitionId, i64>>,
    /// The offset to start reading from for each partition.
    start_offsets: BTreeMap<PartitionId, i64>,
    /// Channel to receive Kafka statistics JSON blobs from the stats callback.
    stats_rx: crossbeam_channel::Receiver<Jsonb>,
    /// Progress statistics as collected from the `resume_uppers` stream and the partition metadata
    /// thread.
    progress_statistics: Arc<Mutex<PartialProgressStatistics>>,
    /// The last partition info we received. For each partition we also fetch the high watermark.
    partition_info: Arc<Mutex<Option<BTreeMap<PartitionId, WatermarkOffsets>>>>,
    /// A handle to the spawned metadata thread
    // Drop order is important here, we want the thread to be unparked after the `partition_info`
    // Arc has been dropped, so that the unpacked thread notices it and exits immediately
    _metadata_thread_handle: UnparkOnDropHandle<()>,
    /// A handle to the partition specific metrics
    partition_metrics: KafkaSourceMetrics,
    /// The latest status detected by the metadata refresh thread.
    health_status: Arc<Mutex<HealthStatus>>,
    /// Per partition capabilities used to produce messages
    partition_capabilities: BTreeMap<PartitionId, PartitionCapability>,
}

/// A partially-filled version of `ProgressStatisticsUpdate`. This allows us to
/// only emit updates when `offset_known` is updated by the metadata thread.
#[derive(Default)]
struct PartialProgressStatistics {
    offset_known: Option<u64>,
    offset_committed: Option<u64>,
}

struct PartitionCapability {
    /// The capability of the data produced
    data: Capability<KafkaTimestamp>,
    /// The capability of the progress stream
    progress: Capability<KafkaTimestamp>,
}

/// Represents the low and high watermark offsets of a Kafka partition.
#[derive(Debug)]
struct WatermarkOffsets {
    /// The offset of the earliest message in the topic/partition. If no messages have been written
    /// to the topic, the low watermark offset is set to 0. The low watermark will also be 0 if one
    /// message has been written to the partition (with offset 0).
    low: u64,
    /// The high watermark offset, which is the offset of the latest message in the topic/partition
    /// available for consumption + 1.
    high: u64,
}

/// Processes `resume_uppers` stream updates, committing them upstream and
/// storing them in the `progress_statistics` to be emitted later.
pub struct KafkaResumeUpperProcessor {
    config: RawSourceCreationConfig,
    topic_name: String,
    consumer: Arc<BaseConsumer<TunnelingClientContext<GlueConsumerContext>>>,
    progress_statistics: Arc<Mutex<PartialProgressStatistics>>,
}

/// Computes whether this worker is responsible for consuming a partition. It assigns partitions to
/// workers in a round-robin fashion, starting at an arbitrary worker based on the hash of the
/// source id.
fn responsible_for_pid(config: &RawSourceCreationConfig, pid: i32) -> bool {
    let pid = usize::try_from(pid).expect("positive pid");
    ((config.responsible_worker(config.id) + pid) % config.worker_count) == config.worker_id
}

struct SourceOutputInfo {
    output_index: usize,
    resume_upper: Antichain<KafkaTimestamp>,
    metadata_columns: Vec<KafkaMetadataKind>,
}

impl SourceRender for KafkaSourceConnection {
    // TODO(petrosagg): The type used for the partition (RangeBound<PartitionId>) doesn't need to
    // be so complicated and we could instead use `Partitioned<PartitionId, Option<u64>>` where all
    // ranges are inclusive and a time of `None` signifies that a particular partition is not
    // present. This requires an shard migration of the remap shard.
    type Time = KafkaTimestamp;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Kafka;

    fn render<G: Scope<Timestamp = KafkaTimestamp>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<KafkaTimestamp>> + 'static,
        start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        StackedCollection<G, (usize, Result<SourceMessage, DataflowError>)>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Stream<G, ProgressStatisticsUpdate>,
        Vec<PressOnDropButton>,
    ) {
        let mut builder = AsyncOperatorBuilder::new(config.name.clone(), scope.clone());

        let (mut data_output, stream) = builder.new_output::<AccountedStackBuilder<_>>();
        let (_progress_output, progress_stream) =
            builder.new_output::<CapacityContainerBuilder<_>>();
        let (mut health_output, health_stream) = builder.new_output();
        let (mut stats_output, stats_stream) = builder.new_output();

        let mut outputs = vec![];
        for (id, export) in &config.source_exports {
            let IndexedSourceExport {
                ingestion_output,
                export:
                    SourceExport {
                        details,
                        storage_metadata: _,
                        data_config: _,
                    },
            } = export;
            let resume_upper = Antichain::from_iter(
                config
                    .source_resume_uppers
                    .get(id)
                    .expect("all source exports must be present in source resume uppers")
                    .iter()
                    .map(Partitioned::<RangeBound<PartitionId>, MzOffset>::decode_row),
            );

            let metadata_columns = match details {
                SourceExportDetails::Kafka(details) => details
                    .metadata_columns
                    .iter()
                    .map(|(_name, kind)| kind.clone())
                    .collect::<Vec<_>>(),
                SourceExportDetails::None => {
                    // This is an export that doesn't need any data output to it.
                    continue;
                }
                _ => panic!("unexpected source export details: {:?}", details),
            };

            let output = SourceOutputInfo {
                resume_upper,
                output_index: *ingestion_output,
                metadata_columns,
            };
            outputs.push(output);
        }

        let busy_signal = Arc::clone(&config.busy_signal);
        let button = builder.build(move |caps| {
            SignaledFuture::new(busy_signal, async move {
                let [mut data_cap, mut progress_cap, health_cap, stats_cap]: [_; 4] =
                    caps.try_into().unwrap();

                let client_id = self.client_id(
                    config.config.config_set(),
                    &config.config.connection_context,
                    config.id,
                );
                let group_id = self.group_id(&config.config.connection_context, config.id);
                let KafkaSourceConnection {
                    connection,
                    topic,
                    topic_metadata_refresh_interval,
                    start_offsets,
                    metadata_columns: _,
                    // Exhaustive match protects against forgetting to apply an
                    // option. Ignored fields are justified below.
                    connection_id: _,   // not needed here
                    group_id_prefix: _, // used above via `self.group_id`
                } = self;

                // Start offsets is a map from partition to the next offset to read from.
                let mut start_offsets: BTreeMap<_, i64> = start_offsets
                    .clone()
                    .into_iter()
                    .filter(|(pid, _offset)| responsible_for_pid(&config, *pid))
                    .map(|(k, v)| (k, v))
                    .collect();

                let mut partition_capabilities = BTreeMap::new();
                let mut max_pid = None;
                let resume_upper = Antichain::from_iter(
                    outputs
                        .iter()
                        .map(|output| output.resume_upper.clone())
                        .flatten(),
                );

                // Whether or not this instance of the dataflow is performing a snapshot.
                let mut is_snapshotting = &*resume_upper == &[Partitioned::minimum()];

                for ts in resume_upper.elements() {
                    if let Some(pid) = ts.interval().singleton() {
                        let pid = pid.unwrap_exact();
                        max_pid = std::cmp::max(max_pid, Some(*pid));
                        if responsible_for_pid(&config, *pid) {
                            let restored_offset = i64::try_from(ts.timestamp().offset)
                                .expect("restored kafka offsets must fit into i64");
                            if let Some(start_offset) = start_offsets.get_mut(pid) {
                                *start_offset = std::cmp::max(restored_offset, *start_offset);
                            } else {
                                start_offsets.insert(*pid, restored_offset);
                            }

                            let part_ts = Partitioned::new_singleton(
                                RangeBound::exact(*pid),
                                ts.timestamp().clone(),
                            );
                            let part_cap = PartitionCapability {
                                data: data_cap.delayed(&part_ts),
                                progress: progress_cap.delayed(&part_ts),
                            };
                            partition_capabilities.insert(*pid, part_cap);
                        }
                    }
                }
                let lower = max_pid
                    .map(RangeBound::after)
                    .unwrap_or(RangeBound::NegInfinity);
                let future_ts =
                    Partitioned::new_range(lower, RangeBound::PosInfinity, MzOffset::from(0));
                data_cap.downgrade(&future_ts);
                progress_cap.downgrade(&future_ts);

                info!(
                    source_id = config.id.to_string(),
                    worker_id = config.worker_id,
                    num_workers = config.worker_count,
                    "instantiating Kafka source reader at offsets {start_offsets:?}"
                );

                let (stats_tx, stats_rx) = crossbeam_channel::unbounded();
                let health_status = Arc::new(Mutex::new(Default::default()));
                let notificator = Arc::new(Notify::new());
                let consumer: Result<BaseConsumer<_>, _> = connection
                    .create_with_context(
                        &config.config,
                        GlueConsumerContext {
                            notificator: Arc::clone(&notificator),
                            stats_tx,
                            inner: MzClientContext::default(),
                        },
                        &btreemap! {
                            // Disable Kafka auto commit. We manually commit offsets
                            // to Kafka once we have reclocked those offsets, so
                            // that users can use standard Kafka tools for progress
                            // tracking.
                            "enable.auto.commit" => "false".into(),
                            // Always begin ingest at 0 when restarted, even if Kafka
                            // contains committed consumer read offsets
                            "auto.offset.reset" => "earliest".into(),
                            // Use the user-configured topic metadata refresh
                            // interval.
                            "topic.metadata.refresh.interval.ms" =>
                                topic_metadata_refresh_interval
                                .as_millis()
                                .to_string(),
                            // TODO: document the rationale for this.
                            "fetch.message.max.bytes" => "134217728".into(),
                            // Consumer group ID, which may have been overridden by
                            // the user. librdkafka requires this, and we use offset
                            // committing to provide a way for users to monitor
                            // ingest progress, though we do not rely on the
                            // committed offsets for any functionality.
                            "group.id" => group_id.clone(),
                            // Allow Kafka monitoring tools to identify this
                            // consumer.
                            "client.id" => client_id.clone(),
                        },
                        InTask::Yes,
                    )
                    .await;

                let consumer = match consumer {
                    Ok(consumer) => Arc::new(consumer),
                    Err(e) => {
                        let update = HealthStatusUpdate::halting(
                            format!(
                                "failed creating kafka consumer: {}",
                                e.display_with_causes()
                            ),
                            None,
                        );
                        health_output.give(
                            &health_cap,
                            HealthStatusMessage {
                                index: 0,
                                namespace: if matches!(e, ContextCreationError::Ssh(_)) {
                                    StatusNamespace::Ssh
                                } else {
                                    Self::STATUS_NAMESPACE.clone()
                                },
                                update,
                            },
                        );
                        // IMPORTANT: wedge forever until the `SuspendAndRestart` is processed.
                        // Returning would incorrectly present to the remap operator as progress to the
                        // empty frontier which would be incorrectly recorded to the remap shard.
                        std::future::pending::<()>().await;
                        unreachable!("pending future never returns");
                    }
                };

                // Note that we wait for this AFTER we downgrade to the source `resume_upper`. This
                // allows downstream operators (namely, the `reclock_operator`) to downgrade to the
                // `resume_upper`, which is necessary for this basic form of backpressure to work.
                start_signal.await;
                info!(
                    source_id = config.id.to_string(),
                    worker_id = config.worker_id,
                    num_workers = config.worker_count,
                    "kafka worker noticed rehydration is finished, starting partition queues..."
                );

                let partition_info = Arc::new(Mutex::new(None));
                let metadata_thread_handle = {
                    let partition_info = Arc::downgrade(&partition_info);
                    let topic = topic.clone();
                    let consumer = Arc::clone(&consumer);

                    // We want a fairly low ceiling on our polling frequency, since we rely
                    // on this heartbeat to determine the health of our Kafka connection.
                    let poll_interval = topic_metadata_refresh_interval.min(
                        config
                            .config
                            .parameters
                            .kafka_timeout_config
                            .default_metadata_fetch_interval,
                    );

                    let status_report = Arc::clone(&health_status);

                    thread::Builder::new()
                        .name("kafka-metadata".to_string())
                        .spawn(move || {
                            trace!(
                                source_id = config.id.to_string(),
                                worker_id = config.worker_id,
                                num_workers = config.worker_count,
                                poll_interval =? poll_interval,
                                "kafka metadata thread: starting..."
                            );
                            while let Some(partition_info) = partition_info.upgrade() {
                                let result = fetch_partition_info(
                                    consumer.client(),
                                    &topic,
                                    config
                                        .config
                                        .parameters
                                        .kafka_timeout_config
                                        .fetch_metadata_timeout,
                                );
                                trace!(
                                    source_id = config.id.to_string(),
                                    worker_id = config.worker_id,
                                    num_workers = config.worker_count,
                                    "kafka metadata thread: metadata fetch result: {:?}",
                                    result
                                );
                                match result {
                                    Ok(info) => {
                                        *partition_info.lock().unwrap() = Some(info);
                                        trace!(
                                        source_id = config.id.to_string(),
                                        worker_id = config.worker_id,
                                        num_workers = config.worker_count,
                                        "kafka metadata thread: updated partition metadata info",
                                    );

                                        // Clear all the health namespaces we know about.
                                        // Note that many kafka sources's don't have an ssh tunnel, but
                                        // the `health_operator` handles this fine.
                                        *status_report.lock().unwrap() = HealthStatus {
                                            kafka: Some(HealthStatusUpdate::running()),
                                            ssh: Some(HealthStatusUpdate::running()),
                                        };
                                    }
                                    Err(e) => {
                                        let kafka_status = Some(HealthStatusUpdate::stalled(
                                            format!("{}", e.display_with_causes()),
                                            None,
                                        ));

                                        let ssh_status =
                                            consumer.client().context().tunnel_status();
                                        let ssh_status = match ssh_status {
                                            SshTunnelStatus::Running => {
                                                Some(HealthStatusUpdate::running())
                                            }
                                            SshTunnelStatus::Errored(e) => {
                                                Some(HealthStatusUpdate::stalled(e, None))
                                            }
                                        };

                                        *status_report.lock().unwrap() = HealthStatus {
                                            kafka: kafka_status,
                                            ssh: ssh_status,
                                        }
                                    }
                                }
                                thread::park_timeout(poll_interval);
                            }
                            info!(
                            source_id = config.id.to_string(),
                            worker_id = config.worker_id,
                            num_workers = config.worker_count,
                            "kafka metadata thread: partition info has been dropped; shutting down."
                        )
                        })
                        .unwrap()
                        .unpark_on_drop()
                };
                let partition_ids = start_offsets.keys().copied().collect();

                let offset_commit_metrics = config.metrics.get_offset_commit_metrics(config.id);

                let mut reader = KafkaSourceReader {
                    topic_name: topic.clone(),
                    source_name: config.name.clone(),
                    id: config.id,
                    partition_consumers: Vec::new(),
                    consumer: Arc::clone(&consumer),
                    worker_id: config.worker_id,
                    worker_count: config.worker_count,
                    last_offsets: outputs
                        .iter()
                        .map(|output| (output.output_index, BTreeMap::new()))
                        .collect(),
                    start_offsets,
                    stats_rx,
                    progress_statistics: Default::default(),
                    partition_info,
                    _metadata_thread_handle: metadata_thread_handle,
                    partition_metrics: config.metrics.get_kafka_source_metrics(
                        partition_ids,
                        topic.clone(),
                        config.id,
                    ),
                    health_status,
                    partition_capabilities,
                };

                let offset_committer = KafkaResumeUpperProcessor {
                    config: config.clone(),
                    topic_name: topic.clone(),
                    consumer,
                    progress_statistics: Arc::clone(&reader.progress_statistics),
                };

                // Seed the progress metrics with `0` if we are snapshotting.
                if is_snapshotting {
                    if let Err(e) = offset_committer
                        .process_frontier(resume_upper.clone())
                        .await
                    {
                        offset_commit_metrics.offset_commit_failures.inc();
                        tracing::warn!(
                            %e,
                            "timely-{} source({}) failed to commit offsets: resume_upper={}",
                            config.id,
                            config.worker_id,
                            resume_upper.pretty()
                        );
                    }
                }

                let resume_uppers_process_loop = async move {
                    tokio::pin!(resume_uppers);
                    while let Some(frontier) = resume_uppers.next().await {
                        if let Err(e) = offset_committer.process_frontier(frontier.clone()).await {
                            offset_commit_metrics.offset_commit_failures.inc();
                            tracing::warn!(
                                %e,
                                "timely-{} source({}) failed to commit offsets: resume_upper={}",
                                config.id,
                                config.worker_id,
                                frontier.pretty()
                            );
                        }
                    }
                    // During dataflow shutdown this loop can end due to the general chaos caused by
                    // dropping tokens as a means to shutdown. This call ensures this future never ends
                    // and we instead rely on this operator being dropped altogether when *its* token
                    // is dropped.
                    std::future::pending::<()>().await;
                };
                tokio::pin!(resume_uppers_process_loop);

                let mut prev_pid_info: Option<BTreeMap<PartitionId, WatermarkOffsets>> = None;
                let mut snapshot_total = None;

                let max_wait_time =
                    mz_storage_types::dyncfgs::KAFKA_POLL_MAX_WAIT.get(config.config.config_set());
                loop {
                    let partition_info = reader.partition_info.lock().unwrap().take();
                    if let Some(partitions) = partition_info {
                        let max_pid = partitions.keys().last().cloned();
                        let lower = max_pid
                            .map(RangeBound::after)
                            .unwrap_or(RangeBound::NegInfinity);
                        let future_ts = Partitioned::new_range(
                            lower,
                            RangeBound::PosInfinity,
                            MzOffset::from(0),
                        );

                        // Topics are identified by name but it's possible that a user recreates a
                        // topic with the same name but different configuration. Ideally we'd want to
                        // catch all of these cases and immediately error out the source, since the
                        // data is effectively gone. Unfortunately this is not possible without
                        // something like KIP-516 so we're left with heuristics.
                        //
                        // The first heuristic is whether the reported number of partitions went down
                        if !PartialOrder::less_equal(data_cap.time(), &future_ts) {
                            let prev_pid_count = prev_pid_info.map(|info| info.len()).unwrap_or(0);
                            let pid_count = partitions.len();
                            let err = DataflowError::SourceError(Box::new(SourceError {
                                error: SourceErrorDetails::Other(format!(
                                    "topic was recreated: partition \
                                     count regressed from {prev_pid_count} to {pid_count}"
                                )),
                            }));
                            let time = data_cap.time().clone();
                            data_output
                                .give_fueled(&data_cap, ((0, Err(err)), time, 1))
                                .await;
                            return;
                        }

                        // The second heuristic is whether the high watermark regressed
                        if let Some(prev_pid_info) = prev_pid_info {
                            for (pid, prev_watermarks) in prev_pid_info {
                                let watermarks = &partitions[&pid];
                                if !(prev_watermarks.high <= watermarks.high) {
                                    let err = DataflowError::SourceError(Box::new(SourceError {
                                        error: SourceErrorDetails::Other(format!(
                                            "topic was recreated: high watermark of \
                                        partition {pid} regressed from {} to {}",
                                            prev_watermarks.high, watermarks.high
                                        )),
                                    }));
                                    let time = data_cap.time().clone();
                                    data_output
                                        .give_fueled(&data_cap, ((0, Err(err)), time, 1))
                                        .await;
                                    return;
                                }
                            }
                        }

                        let mut upstream_stat = 0;
                        for (&pid, watermarks) in &partitions {
                            if responsible_for_pid(&config, pid) {
                                upstream_stat += watermarks.high;
                                reader.ensure_partition(pid);
                                if let Entry::Vacant(entry) =
                                    reader.partition_capabilities.entry(pid)
                                {
                                    let start_offset = match reader.start_offsets.get(&pid) {
                                        Some(&offset) => offset.try_into().unwrap(),
                                        None => 0u64,
                                    };
                                    let start_offset = std::cmp::max(start_offset, watermarks.low);
                                    let part_since_ts = Partitioned::new_singleton(
                                        RangeBound::exact(pid),
                                        MzOffset::from(start_offset),
                                    );
                                    let part_upper_ts = Partitioned::new_singleton(
                                        RangeBound::exact(pid),
                                        MzOffset::from(watermarks.high),
                                    );

                                    // This is the moment at which we have discovered a new partition
                                    // and we need to make sure we produce its initial snapshot at a,
                                    // single timestamp so that the source transitions from no data
                                    // from this partition to all the data of this partition. We do
                                    // this by initializing the data capability to the starting offset
                                    // and, importantly, the progress capability directly to the high
                                    // watermark. This jump of the progress capability ensures that
                                    // everything until the high watermark will be reclocked to a
                                    // single point.
                                    entry.insert(PartitionCapability {
                                        data: data_cap.delayed(&part_since_ts),
                                        progress: progress_cap.delayed(&part_upper_ts),
                                    });
                                }
                            }
                        }

                        // If we are snapshotting, record our first set of partitions as the snapshot
                        // size.
                        if is_snapshotting && snapshot_total.is_none() {
                            // Note that we want to represent the _number of offsets_, which
                            // means the watermark's frontier semantics is correct, without
                            // subtracting (Kafka offsets start at 0).
                            snapshot_total = Some(upstream_stat);
                        }

                        reader
                            .progress_statistics
                            .lock()
                            .expect("poisoned")
                            .offset_known = Some(upstream_stat);
                        data_cap.downgrade(&future_ts);
                        progress_cap.downgrade(&future_ts);
                        prev_pid_info = Some(partitions);
                    }

                    // Poll the consumer once. We split the consumer's partitions out into separate
                    // queues and poll those individually, but it's still necessary to drive logic that
                    // consumes from rdkafka's internal event queue, such as statistics callbacks.
                    //
                    // Additionally, assigning topics and splitting them off into separate queues is
                    // not atomic, so we expect to see at least some messages to show up when polling
                    // the consumer directly.
                    while let Some(result) = reader.consumer.poll(Duration::from_secs(0)) {
                        match result {
                            Err(e) => {
                                let error = format!(
                                "kafka error when polling consumer for source: {} topic: {} : {}",
                                reader.source_name, reader.topic_name, e
                            );
                                let status = HealthStatusUpdate::stalled(error, None);
                                health_output.give(
                                    &health_cap,
                                    HealthStatusMessage {
                                        index: 0,
                                        namespace: Self::STATUS_NAMESPACE.clone(),
                                        update: status,
                                    },
                                );
                            }
                            Ok(message) => {
                                let output_messages = outputs
                                    .iter()
                                    .map(|output| {
                                        let (message, ts) = construct_source_message(
                                            &message,
                                            &output.metadata_columns,
                                        );
                                        (output.output_index, message, ts)
                                    })
                                    // This vec allocation is required to allow obtaining a `&mut`
                                    // on `reader` for the `reader.handle_message` call in the
                                    // loop below since  `message` is borrowed from `reader`.
                                    .collect::<Vec<_>>();
                                for (output_index, message, ts) in output_messages {
                                    if let Some((msg, time, diff)) =
                                        reader.handle_message(message, ts, &output_index)
                                    {
                                        let pid =
                                            time.interval().singleton().unwrap().unwrap_exact();
                                        let part_cap = &reader.partition_capabilities[pid].data;
                                        let msg = msg.map_err(|e| {
                                            DataflowError::SourceError(Box::new(SourceError {
                                                error: SourceErrorDetails::Other(format!("{}", e)),
                                            }))
                                        });
                                        data_output
                                            .give_fueled(
                                                part_cap,
                                                ((output_index, msg), time, diff),
                                            )
                                            .await;
                                    }
                                }
                            }
                        }
                    }

                    reader.update_stats();

                    // Take the consumers temporarily to get around borrow checker errors
                    let mut consumers = std::mem::take(&mut reader.partition_consumers);
                    for consumer in consumers.iter_mut() {
                        let pid = consumer.pid();
                        while let Some(message) = consumer.get_next_message().transpose() {
                            for output in outputs.iter() {
                                let message = match &message {
                                    Ok((msg, pid)) => {
                                        let (msg, ts) =
                                            construct_source_message(msg, &output.metadata_columns);
                                        assert_eq!(*pid, ts.0);
                                        Ok(reader.handle_message(msg, ts, &output.output_index))
                                    }
                                    Err(err) => Err(err),
                                };
                                match message {
                                    Ok(Some((msg, time, diff))) => {
                                        let pid =
                                            time.interval().singleton().unwrap().unwrap_exact();
                                        let part_cap = &reader.partition_capabilities[pid].data;
                                        let msg = msg.map_err(|e| {
                                            DataflowError::SourceError(Box::new(SourceError {
                                                error: SourceErrorDetails::Other(format!("{}", e)),
                                            }))
                                        });
                                        data_output
                                            .give_fueled(
                                                part_cap,
                                                ((output.output_index, msg), time, diff),
                                            )
                                            .await;
                                    }
                                    // The message was from an offset we've already seen.
                                    Ok(None) => continue,
                                    Err(err) => {
                                        let last_offset = reader
                                            .last_offsets
                                            .get(&output.output_index)
                                            .expect("output known to be installed")
                                            .get(&pid)
                                            .expect("partition known to be installed");

                                        let status = HealthStatusUpdate::stalled(
                                            format!(
                                                "error consuming from source: {} topic: {topic}:\
                                                partition: {pid} last processed offset:\
                                                {last_offset} : {err}",
                                                config.name
                                            ),
                                            None,
                                        );
                                        health_output.give(
                                            &health_cap,
                                            HealthStatusMessage {
                                                index: output.output_index,
                                                namespace: Self::STATUS_NAMESPACE.clone(),
                                                update: status,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                    }
                    // We can now put them back
                    assert!(reader.partition_consumers.is_empty());
                    reader.partition_consumers = consumers;

                    let positions = reader.consumer.position().unwrap();
                    let topic_positions = positions.elements_for_topic(&reader.topic_name);
                    let mut snapshot_staged = 0;

                    for position in topic_positions {
                        // The offset begins in the `Offset::Invalid` state in which case we simply
                        // skip this partition.
                        if let Offset::Offset(offset) = position.offset() {
                            let pid = position.partition();
                            let upper_offset = MzOffset::from(u64::try_from(offset).unwrap());
                            let upper =
                                Partitioned::new_singleton(RangeBound::exact(pid), upper_offset);

                            let part_cap = reader.partition_capabilities.get_mut(&pid).unwrap();
                            match part_cap.data.try_downgrade(&upper) {
                                Ok(()) => {
                                    if is_snapshotting {
                                        // The `.position()` of the consumer represents what offset we have
                                        // read up to.
                                        snapshot_staged += offset.try_into().unwrap_or(0u64);
                                        // This will always be `Some` at this point.
                                        if let Some(snapshot_total) = snapshot_total {
                                            // We will eventually read past the snapshot total, so we need
                                            // to bound it here.
                                            snapshot_staged =
                                                std::cmp::min(snapshot_staged, snapshot_total);
                                        }
                                    }
                                }
                                Err(_) => {
                                    // If we can't downgrade, it means we have already seen this offset.
                                    // This is expected and we can safely ignore it.
                                    info!(
                                        source_id = config.id.to_string(),
                                        worker_id = config.worker_id,
                                        num_workers = config.worker_count,
                                        "kafka source frontier downgrade skipped due to already \
                                        seen offset: {:?}",
                                        upper
                                    );
                                }
                            };

                            // We use try_downgrade here because during the initial snapshot phase the
                            // data capability is not beyond the progress capability and therefore a
                            // normal downgrade would panic. Once it catches up though the data
                            // capbility is what's pushing the progress capability forward.
                            let _ = part_cap.progress.try_downgrade(&upper);
                        }
                    }

                    let (kafka_status, ssh_status) = {
                        let mut health_status = reader.health_status.lock().unwrap();
                        (health_status.kafka.take(), health_status.ssh.take())
                    };
                    if let Some(status) = kafka_status {
                        health_output.give(
                            &health_cap,
                            HealthStatusMessage {
                                index: 0,
                                namespace: Self::STATUS_NAMESPACE.clone(),
                                update: status,
                            },
                        );
                    }
                    if let Some(status) = ssh_status {
                        health_output.give(
                            &health_cap,
                            HealthStatusMessage {
                                index: 0,
                                namespace: StatusNamespace::Ssh,
                                update: status,
                            },
                        );
                    }

                    // If we have a new `offset_known` from the partition metadata thread, and
                    // `committed` from reading the `resume_uppers` stream, we can emit a
                    // progress stats update.
                    let progress_statistics = {
                        let mut stats = reader.progress_statistics.lock().expect("poisoned");

                        if stats.offset_committed.is_some() && stats.offset_known.is_some() {
                            Some((
                                stats.offset_known.take().unwrap(),
                                stats.offset_committed.take().unwrap(),
                            ))
                        } else {
                            None
                        }
                    };
                    if let Some((offset_known, offset_committed)) = progress_statistics {
                        stats_output.give(
                            &stats_cap,
                            ProgressStatisticsUpdate::SteadyState {
                                offset_committed,
                                offset_known,
                            },
                        );
                    }

                    if let (Some(snapshot_total), true) = (snapshot_total, is_snapshotting) {
                        stats_output.give(
                            &stats_cap,
                            ProgressStatisticsUpdate::Snapshot {
                                records_known: snapshot_total,
                                records_staged: snapshot_staged,
                            },
                        );

                        if snapshot_total == snapshot_staged {
                            is_snapshotting = false;
                        }
                    }

                    // Wait to be notified while also making progress with offset committing
                    tokio::select! {
                        // TODO(petrosagg): remove the timeout and rely purely on librdkafka waking us
                        // up
                        _  = tokio::time::timeout(max_wait_time, notificator.notified()) => {},
                        // This future is not cancel safe but we are only passing a reference to it in
                        // the select! loop so the future stays on the stack and never gets cancelled
                        // until the end of the function.
                        _ = resume_uppers_process_loop.as_mut() => {},
                    }
                }
            })
        });

        (
            stream.as_collection(),
            Some(progress_stream),
            health_stream,
            stats_stream,
            vec![button.press_on_drop()],
        )
    }
}

impl KafkaResumeUpperProcessor {
    async fn process_frontier(
        &self,
        frontier: Antichain<KafkaTimestamp>,
    ) -> Result<(), anyhow::Error> {
        use rdkafka::consumer::CommitMode;

        // Generate a list of partitions that this worker is responsible for
        let mut offsets = vec![];
        let mut progress_stat = 0;
        for ts in frontier.iter() {
            if let Some(pid) = ts.interval().singleton() {
                let pid = pid.unwrap_exact();
                if responsible_for_pid(&self.config, *pid) {
                    offsets.push((pid.clone(), *ts.timestamp()));

                    // Note that we do not subtract 1 from the frontier. Imagine
                    // that frontier is 2 for this pid. That means we have
                    // full processed offset 0 and offset 1, which means we have
                    // processed _2_ offsets.
                    progress_stat += ts.timestamp().offset;
                }
            }
        }
        self.progress_statistics
            .lock()
            .expect("poisoned")
            .offset_committed = Some(progress_stat);

        if !offsets.is_empty() {
            let mut tpl = TopicPartitionList::new();
            for (pid, offset) in offsets {
                let offset_to_commit =
                    Offset::Offset(offset.offset.try_into().expect("offset to be vald i64"));
                tpl.add_partition_offset(&self.topic_name, pid, offset_to_commit)
                    .expect("offset known to be valid");
            }
            let consumer = Arc::clone(&self.consumer);
            mz_ore::task::spawn_blocking(
                || format!("source({}) kafka offset commit", self.config.id),
                move || consumer.commit(&tpl, CommitMode::Sync),
            )
            .await??;
        }
        Ok(())
    }
}

impl KafkaSourceReader {
    /// Ensures that a partition queue for `pid` exists.
    fn ensure_partition(&mut self, pid: PartitionId) {
        for last_offsets in self.last_offsets.values() {
            // early exit if we've already inserted this partition
            if last_offsets.contains_key(&pid) {
                return;
            }
        }

        let start_offset = self.start_offsets.get(&pid).copied().unwrap_or(0);
        self.create_partition_queue(pid, Offset::Offset(start_offset));

        for last_offsets in self.last_offsets.values_mut() {
            let prev = last_offsets.insert(pid, start_offset - 1);
            assert_none!(prev);
        }
    }

    /// Creates a new partition queue for `partition_id`.
    fn create_partition_queue(&mut self, partition_id: PartitionId, initial_offset: Offset) {
        info!(
            source_id = self.id.to_string(),
            worker_id = self.worker_id,
            num_workers = self.worker_count,
            "activating Kafka queue for topic {}, partition {}",
            self.topic_name,
            partition_id,
        );

        // Collect old partition assignments
        let tpl = self.consumer.assignment().unwrap();
        // Create list from assignments
        let mut partition_list = TopicPartitionList::new();
        for partition in tpl.elements_for_topic(&self.topic_name) {
            partition_list
                .add_partition_offset(partition.topic(), partition.partition(), partition.offset())
                .expect("offset known to be valid");
        }
        // Add new partition
        partition_list
            .add_partition_offset(&self.topic_name, partition_id, initial_offset)
            .expect("offset known to be valid");
        self.consumer
            .assign(&partition_list)
            .expect("assignment known to be valid");

        // Since librdkafka v1.6.0, we need to recreate all partition queues
        // after every call to `self.consumer.assign`.
        let context = Arc::clone(self.consumer.context());
        for pc in &mut self.partition_consumers {
            pc.partition_queue = self
                .consumer
                .split_partition_queue(&self.topic_name, pc.pid)
                .expect("partition known to be valid");
            pc.partition_queue.set_nonempty_callback({
                let context = Arc::clone(&context);
                move || context.inner().activate()
            });
        }

        let mut partition_queue = self
            .consumer
            .split_partition_queue(&self.topic_name, partition_id)
            .expect("partition known to be valid");
        partition_queue.set_nonempty_callback(move || context.inner().activate());
        self.partition_consumers
            .push(PartitionConsumer::new(partition_id, partition_queue));
        assert_eq!(
            self.consumer
                .assignment()
                .unwrap()
                .elements_for_topic(&self.topic_name)
                .len(),
            self.partition_consumers.len()
        );
    }

    /// Read any statistics JSON blobs generated via the rdkafka statistics callback.
    fn update_stats(&mut self) {
        while let Ok(stats) = self.stats_rx.try_recv() {
            match serde_json::from_str::<Statistics>(&stats.to_string()) {
                Ok(statistics) => {
                    let topic = statistics.topics.get(&self.topic_name);
                    match topic {
                        Some(topic) => {
                            for (id, partition) in &topic.partitions {
                                self.partition_metrics
                                    .set_offset_max(*id, partition.hi_offset);
                            }
                        }
                        None => error!("No stats found for topic: {}", &self.topic_name),
                    }
                }
                Err(e) => {
                    error!("failed decoding librdkafka statistics JSON: {}", e);
                }
            }
        }
    }

    /// Checks if the given message is viable for emission. This checks if the message offset is
    /// past the expected offset and returns None if it is not.
    fn handle_message(
        &mut self,
        message: Result<SourceMessage, KafkaHeaderParseError>,
        (partition, offset): (PartitionId, MzOffset),
        output_index: &usize,
    ) -> Option<(
        Result<SourceMessage, KafkaHeaderParseError>,
        KafkaTimestamp,
        Diff,
    )> {
        // Offsets are guaranteed to be 1) monotonically increasing *unless* there is
        // a network issue or a new partition added, at which point the consumer may
        // start processing the topic from the beginning, or we may see duplicate offsets
        // At all times, the guarantee : if we see offset x, we have seen all offsets [0,x-1]
        // that we are ever going to see holds.
        // Offsets are guaranteed to be contiguous when compaction is disabled. If compaction
        // is enabled, there may be gaps in the sequence.
        // If we see an "old" offset, we skip that message.

        // Given the explicit consumer to partition assignment, we should never receive a message
        // for a partition for which we have no metadata
        assert!(self
            .last_offsets
            .get(output_index)
            .unwrap()
            .contains_key(&partition));

        let last_offset_ref = self
            .last_offsets
            .get_mut(output_index)
            .expect("output known to be installed")
            .get_mut(&partition)
            .expect("partition known to be installed");

        let last_offset = *last_offset_ref;
        let offset_as_i64: i64 = offset.offset.try_into().expect("offset to be < i64::MAX");
        if offset_as_i64 <= last_offset {
            info!(
                source_id = self.id.to_string(),
                worker_id = self.worker_id,
                num_workers = self.worker_count,
                "kafka message before expected offset: \
                source {} (reading topic {}, partition {}, output {}) \
                received offset {} expected offset {:?}",
                self.source_name,
                self.topic_name,
                partition,
                output_index,
                offset.offset,
                last_offset + 1,
            );
            // We explicitly should not consume the message as we have already processed it.
            None
        } else {
            *last_offset_ref = offset_as_i64;

            let ts = Partitioned::new_singleton(RangeBound::exact(partition), offset);
            Some((message, ts, 1))
        }
    }
}

fn construct_source_message(
    msg: &BorrowedMessage<'_>,
    metadata_columns: &[KafkaMetadataKind],
) -> (
    Result<SourceMessage, KafkaHeaderParseError>,
    (PartitionId, MzOffset),
) {
    let pid = msg.partition();
    let Ok(offset) = u64::try_from(msg.offset()) else {
        panic!(
            "got negative offset ({}) from otherwise non-error'd kafka message",
            msg.offset()
        );
    };

    let mut metadata = Row::default();
    let mut packer = metadata.packer();
    for kind in metadata_columns {
        match kind {
            KafkaMetadataKind::Partition => packer.push(Datum::from(pid)),
            KafkaMetadataKind::Offset => packer.push(Datum::UInt64(offset)),
            KafkaMetadataKind::Timestamp => {
                let ts = msg
                    .timestamp()
                    .to_millis()
                    .expect("kafka sources always have upstream_time");

                let d: Datum = DateTime::from_timestamp_millis(ts)
                    .and_then(|dt| {
                        let ct: Option<CheckedTimestamp<NaiveDateTime>> =
                            dt.naive_utc().try_into().ok();
                        ct
                    })
                    .into();
                packer.push(d)
            }
            KafkaMetadataKind::Header { key, use_bytes } => {
                match msg.headers() {
                    Some(headers) => {
                        let d = headers
                            .iter()
                            .filter(|header| header.key == key)
                            .last()
                            .map(|header| match header.value {
                                Some(v) => {
                                    if *use_bytes {
                                        Ok(Datum::Bytes(v))
                                    } else {
                                        match str::from_utf8(v) {
                                            Ok(str) => Ok(Datum::String(str)),
                                            Err(_) => Err(KafkaHeaderParseError::Utf8Error {
                                                key: key.clone(),
                                                raw: v.to_vec(),
                                            }),
                                        }
                                    }
                                }
                                None => Ok(Datum::Null),
                            })
                            .unwrap_or(Err(KafkaHeaderParseError::KeyNotFound {
                                key: key.clone(),
                            }));
                        match d {
                            Ok(d) => packer.push(d),
                            //abort with a definite error when the header is not found or cannot be parsed correctly
                            Err(err) => return (Err(err), (pid, offset.into())),
                        }
                    }
                    None => packer.push(Datum::Null),
                }
            }
            KafkaMetadataKind::Headers => {
                packer.push_list_with(|r| {
                    if let Some(headers) = msg.headers() {
                        for header in headers.iter() {
                            match header.value {
                                Some(v) => r.push_list_with(|record_row| {
                                    record_row.push(Datum::String(header.key));
                                    record_row.push(Datum::Bytes(v));
                                }),
                                None => r.push_list_with(|record_row| {
                                    record_row.push(Datum::String(header.key));
                                    record_row.push(Datum::Null);
                                }),
                            }
                        }
                    }
                });
            }
        }
    }

    let key = match msg.key() {
        Some(bytes) => Row::pack([Datum::Bytes(bytes)]),
        None => Row::pack([Datum::Null]),
    };
    let value = match msg.payload() {
        Some(bytes) => Row::pack([Datum::Bytes(bytes)]),
        None => Row::pack([Datum::Null]),
    };
    (
        Ok(SourceMessage {
            key,
            value,
            metadata,
        }),
        (pid, offset.into()),
    )
}

/// Wrapper around a partition containing the underlying consumer
struct PartitionConsumer {
    /// the partition id with which this consumer is associated
    pid: PartitionId,
    /// The underlying Kafka partition queue
    partition_queue: PartitionQueue<TunnelingClientContext<GlueConsumerContext>>,
}

impl PartitionConsumer {
    /// Creates a new partition consumer from underlying Kafka consumer
    fn new(
        pid: PartitionId,
        partition_queue: PartitionQueue<TunnelingClientContext<GlueConsumerContext>>,
    ) -> Self {
        PartitionConsumer {
            pid,
            partition_queue,
        }
    }

    /// Returns the next message to process for this partition (if any).
    ///
    /// The outer `Result` represents irrecoverable failures, the inner one can and will
    /// be transformed into empty values.
    ///
    /// The inner `Option` represents if there is a message to process.
    fn get_next_message(&mut self) -> Result<Option<(BorrowedMessage, PartitionId)>, KafkaError> {
        match self.partition_queue.poll(Duration::from_millis(0)) {
            Some(Ok(msg)) => Ok(Some((msg, self.pid))),
            Some(Err(err)) => Err(err),
            _ => Ok(None),
        }
    }

    /// Return the partition id for this PartitionConsumer
    fn pid(&self) -> PartitionId {
        self.pid
    }
}

/// An implementation of [`ConsumerContext`] that forwards statistics to the
/// worker
struct GlueConsumerContext {
    notificator: Arc<Notify>,
    stats_tx: crossbeam_channel::Sender<Jsonb>,
    inner: MzClientContext,
}

impl ClientContext for GlueConsumerContext {
    fn stats_raw(&self, statistics: &[u8]) {
        match Jsonb::from_slice(statistics) {
            Ok(statistics) => {
                self.stats_tx
                    .send(statistics)
                    .expect("timely operator hung up while Kafka source active");
                self.activate();
            }
            Err(e) => error!("failed decoding librdkafka statistics JSON: {}", e),
        };
    }

    // The shape of the rdkafka *Context traits require us to forward to the `MzClientContext`
    // implementation.
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(level, fac, log_message)
    }
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        self.inner.error(error, reason)
    }
}

impl GlueConsumerContext {
    fn activate(&self) {
        self.notificator.notify_waiters();
    }
}

impl ConsumerContext for GlueConsumerContext {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use mz_kafka_util::client::create_new_client_config_simple;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::{Message, Offset, TopicPartitionList};
    use uuid::Uuid;

    // Splitting off a partition queue with an `Offset` that is not `Offset::Beginning` seems to
    // lead to a race condition where sometimes we receive messages from polling the main consumer
    // instead of on the partition queue. This can be surfaced by running the test in a loop (in
    // the dataflow directory) using:
    //
    // cargo stress --lib --release source::kafka::tests::reproduce_kafka_queue_issue
    //
    // cargo-stress can be installed via `cargo install cargo-stress`
    //
    // You need to set up a topic "queue-test" with 1000 "hello" messages in it. Obviously, running
    // this test requires a running Kafka instance at localhost:9092.
    #[mz_ore::test]
    #[ignore]
    fn demonstrate_kafka_queue_race_condition() -> Result<(), anyhow::Error> {
        let topic_name = "queue-test";
        let pid = 0;

        let mut kafka_config = create_new_client_config_simple();
        kafka_config.set("bootstrap.servers", "localhost:9092".to_string());
        kafka_config.set("enable.auto.commit", "false");
        kafka_config.set("group.id", Uuid::new_v4().to_string());
        kafka_config.set("fetch.message.max.bytes", "100");
        let consumer: BaseConsumer<_> = kafka_config.create()?;

        let consumer = Arc::new(consumer);

        let mut partition_list = TopicPartitionList::new();
        // Using Offset:Beginning here will work fine, only Offset:Offset(0) leads to the race
        // condition.
        partition_list.add_partition_offset(topic_name, pid, Offset::Offset(0))?;

        consumer.assign(&partition_list)?;

        let partition_queue = consumer
            .split_partition_queue(topic_name, pid)
            .expect("missing partition queue");

        let expected_messages = 1_000;

        let mut common_queue_count = 0;
        let mut partition_queue_count = 0;

        loop {
            if let Some(msg) = consumer.poll(Duration::from_millis(0)) {
                match msg {
                    Ok(msg) => {
                        let _payload =
                            std::str::from_utf8(msg.payload().expect("missing payload"))?;
                        if partition_queue_count > 0 {
                            anyhow::bail!("Got message from common queue after we internally switched to partition queue.");
                        }

                        common_queue_count += 1;
                    }
                    Err(err) => anyhow::bail!("{}", err),
                }
            }

            match partition_queue.poll(Duration::from_millis(0)) {
                Some(Ok(msg)) => {
                    let _payload = std::str::from_utf8(msg.payload().expect("missing payload"))?;
                    partition_queue_count += 1;
                }
                Some(Err(err)) => anyhow::bail!("{}", err),
                _ => (),
            }

            if (common_queue_count + partition_queue_count) == expected_messages {
                break;
            }
        }

        assert!(
            common_queue_count == 0,
            "Got {} out of {} messages from common queue. Partition queue: {}",
            common_queue_count,
            expected_messages,
            partition_queue_count
        );

        Ok(())
    }
}

/// Fetches the list of partitions and their corresponding high watermark
fn fetch_partition_info<C: ClientContext>(
    client: &Client<C>,
    topic: &str,
    fetch_timeout: Duration,
) -> Result<BTreeMap<PartitionId, WatermarkOffsets>, anyhow::Error> {
    let pids = get_partitions(client, topic, fetch_timeout)?;

    let mut result = BTreeMap::new();

    for pid in pids {
        let (low, high) = client.fetch_watermarks(topic, pid, fetch_timeout)?;
        let watermarks = WatermarkOffsets {
            low: low.try_into().expect("invalid negative offset"),
            high: high.try_into().expect("invalid negative offset"),
        };
        result.insert(pid, watermarks);
    }
    Ok(result)
}

#[derive(Debug, thiserror::Error)]
pub enum KafkaHeaderParseError {
    #[error("A header with key '{key}' was not found in the message headers")]
    KeyNotFound { key: String },
    #[error("Found ill-formed byte sequence in header '{key}' that cannot be decoded as valid utf-8 (original bytes: {raw:x?})")]
    Utf8Error { key: String, raw: Vec<u8> },
}
