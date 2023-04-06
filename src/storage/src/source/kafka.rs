// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use std::any::Any;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use differential_dataflow::{AsCollection, Collection};
use futures::StreamExt;
use maplit::btreemap;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::Offset;
use rdkafka::types::RDKafkaRespErr;
use rdkafka::{ClientContext, Message, TopicPartitionList};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::Notify;
use tracing::{error, info, trace, warn};

use mz_kafka_util::client::{BrokerRewritingClientContext, MzClientContext};
use mz_ore::error::ErrorExt;
use mz_ore::thread::{JoinHandleExt, UnparkOnDropHandle};
use mz_repr::{adt::jsonb::Jsonb, Diff, GlobalId};
use mz_storage_client::types::connections::{ConnectionContext, StringOrSecret};
use mz_storage_client::types::sources::{KafkaSourceConnection, MzOffset, SourceTimestamp};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::OperatorBuilder as AsyncOperatorBuilder;
use mz_timely_util::order::Partitioned;

use self::metrics::KafkaPartitionMetrics;
use crate::source::types::{HealthStatus, HealthStatusUpdate, SourceReaderMetrics, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod metrics;

type PartitionId = i32;

/// Contains all information necessary to ingest data from Kafka
pub struct KafkaSourceReader {
    /// Name of the topic on which this source is backed on
    topic_name: String,
    /// Name of the source (will have format kafka-source-id)
    source_name: String,
    /// Source global ID
    id: GlobalId,
    /// Kafka consumer for this source
    consumer: Arc<BaseConsumer<BrokerRewritingClientContext<GlueConsumerContext>>>,
    /// List of consumers. A consumer should be assigned per partition to guarantee fairness
    partition_consumers: Vec<PartitionConsumer>,
    /// Worker ID
    worker_id: usize,
    /// Total count of workers
    worker_count: usize,
    /// The most recently read offset for each partition known to this source
    /// reader. An offset of -1 indicates that no prior message has been read
    /// for the given partition.
    last_offsets: BTreeMap<PartitionId, i64>,
    /// The offset to start reading from for each partition.
    start_offsets: BTreeMap<PartitionId, i64>,
    /// Channel to receive Kafka statistics JSON blobs from the stats callback.
    stats_rx: crossbeam_channel::Receiver<Jsonb>,
    /// The last partition we received
    partition_info: Arc<Mutex<Option<Vec<PartitionId>>>>,
    /// A handle to the spawned metadata thread
    // Drop order is important here, we want the thread to be unparked after the `partition_info`
    // Arc has been dropped, so that the unpacked thread notices it and exits immediately
    _metadata_thread_handle: UnparkOnDropHandle<()>,
    /// A handle to the partition specific metrics
    partition_metrics: KafkaPartitionMetrics,
    /// Whether or not to unpack and allocate headers and pass them through in the `SourceMessage`
    include_headers: bool,
    /// The latest status detected by the metadata refresh thread.
    health_status: Arc<Mutex<Option<HealthStatus>>>,
    /// Per partition capabilities used to produce messages
    partition_capabilities: BTreeMap<PartitionId, Capability<Partitioned<PartitionId, MzOffset>>>,
}

pub struct KafkaOffsetCommiter {
    source_id: GlobalId,
    /// Worker ID
    worker_id: usize,
    /// Total count of workers
    worker_count: usize,
    topic_name: String,
    consumer: Arc<BaseConsumer<BrokerRewritingClientContext<GlueConsumerContext>>>,
}

impl SourceRender for KafkaSourceConnection {
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;
    type Time = Partitioned<PartitionId, MzOffset>;

    fn render<G: Scope<Timestamp = Partitioned<PartitionId, MzOffset>>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        connection_context: ConnectionContext,
        resume_uppers: impl futures::Stream<Item = Antichain<Partitioned<PartitionId, MzOffset>>>
            + 'static,
    ) -> (
        Collection<G, Result<SourceMessage<Self::Key, Self::Value>, SourceReaderError>, Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusUpdate>,
        Rc<dyn Any>,
    ) {
        let mut builder = AsyncOperatorBuilder::new(config.name.clone(), scope.clone());

        let (mut data_output, stream) = builder.new_output();
        let (mut health_output, health_stream) = builder.new_output();

        let button = builder.build(move |mut capabilities| async move {
            let health_cap = capabilities.pop().unwrap();
            let mut data_cap = capabilities.pop().unwrap();
            assert!(capabilities.is_empty());

            let KafkaSourceConnection {
                connection,
                connection_id,
                topic,
                group_id_prefix,
                environment_id,
                ..
            } = self;
            let (stats_tx, stats_rx) = crossbeam_channel::unbounded();
            let health_status = Arc::new(Mutex::new(None));
            let notificator = Arc::new(Notify::new());
            let consumer = connection
                .create_with_context(
                    &connection_context,
                    GlueConsumerContext {
                        notificator: Arc::clone(&notificator),
                        stats_tx,
                    },
                    &btreemap! {
                        // Default to disabling Kafka auto commit. This can be
                        // explicitly enabled by the user if they want to use it for
                        // progress tracking.
                        "enable.auto.commit" => "false".into(),
                        // Always begin ingest at 0 when restarted, even if Kafka
                        // contains committed consumer read offsets
                        "auto.offset.reset" => "earliest".into(),
                        // How often to refresh metadata from the Kafka broker. This
                        // can have a minor impact on startup latency and latency
                        // after adding a new partition, as the metadata for a
                        // partition must be fetched before we can retrieve data
                        // from it. We try to manually trigger metadata fetches when
                        // it makes sense, but if those manual fetches fail, this is
                        // the interval at which we retry.
                        //
                        // 30s may seem low, but the default is 5m. More frequent
                        // metadata refresh rates are surprising to Kafka users, as
                        // topic partition counts hardly ever change in production.
                        "topic.metadata.refresh.interval.ms" => "30000".into(), // 30s
                        // TODO: document the rationale for this.
                        "fetch.message.max.bytes" => "134217728".into(),
                        // Consumer group ID. librdkafka requires this, and we use
                        // offset committing to provide a way for users to monitor
                        // ingest progress, though we do not rely on the committed
                        // offsets for any functionality.
                        //
                        // The group ID is partially dictated by the user and
                        // partially dictated by us. Users can set a prefix so they
                        // can see which consumers belong to which Materialize
                        // deployment, but we set a suffix to ensure uniqueness. A
                        // unique consumer group ID is the most surefire way to
                        // ensure that librdkafka does not try to perform its own
                        // consumer group balancing, which would wreak havoc with
                        // our careful partition assignment strategy.
                        "group.id" => format!(
                            "{}materialize-{}-{}-{}",
                            group_id_prefix.unwrap_or_else(String::new),
                            environment_id,
                            connection_id,
                            config.id,
                        ),
                    },
                )
                .await;

            let consumer = match consumer {
                Ok(consumer) => Arc::new(consumer),
                Err(e) => {
                    let update = HealthStatusUpdate {
                        update: HealthStatus::StalledWithError {
                            error: format!(
                                "failed creating kafka consumer: {}",
                                e.display_with_causes()
                            ),
                            hint: None,
                        },
                        should_halt: true,
                    };
                    health_output.give(&health_cap, update).await;
                    // IMPORTANT: wedge forever until the `SuspendAndRestart` is processed.
                    // Returning would incorrectly present to the remap operator as progress to the
                    // empty frontier which would be incorrectly recorded to the remap shard.
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            // Start offsets is a map from partition to the next offset to read
            // from.
            let mut start_offsets: BTreeMap<_, i64> = self
                .start_offsets
                .into_iter()
                .filter(|(pid, _offset)| {
                    crate::source::responsible_for(
                        &config.id,
                        config.worker_id,
                        config.worker_count,
                        pid,
                    )
                })
                .map(|(k, v)| (k, v))
                .collect();

            let mut partition_capabilities = BTreeMap::new();
            let mut max_pid = None;
            let resume_upper = Antichain::from_iter(
                config
                    .source_resume_upper
                    .iter()
                    .map(Partitioned::<_, _>::decode_row),
            );
            for ts in resume_upper.elements() {
                if let Some(pid) = ts.partition() {
                    max_pid = std::cmp::max(max_pid, Some(*pid));
                    if crate::source::responsible_for(
                        &config.id,
                        config.worker_id,
                        config.worker_count,
                        pid,
                    ) {
                        let restored_offset = i64::try_from(ts.timestamp().offset)
                            .expect("restored kafka offsets must fit into i64");
                        if let Some(start_offset) = start_offsets.get_mut(pid) {
                            *start_offset = std::cmp::max(restored_offset, *start_offset);
                        } else {
                            start_offsets.insert(*pid, restored_offset);
                        }

                        let part_ts = Partitioned::with_partition(*pid, ts.timestamp().clone());
                        partition_capabilities.insert(*pid, data_cap.delayed(&part_ts));
                    }
                }
            }
            let future_ts = Partitioned::with_range(max_pid, None, MzOffset::from(0));
            data_cap.downgrade(&future_ts);

            info!(
                source_id = config.id.to_string(),
                worker_id = config.worker_id,
                num_workers = config.worker_count,
                "instantiating Kafka source reader at offsets {start_offsets:?}"
            );

            let partition_info = Arc::new(Mutex::new(None));
            let metadata_thread_handle = {
                let partition_info = Arc::downgrade(&partition_info);
                let topic = topic.clone();
                let consumer = Arc::clone(&consumer);
                let metadata_refresh_interval = connection
                    .options
                    .get("topic.metadata.refresh.interval.ms")
                    // Safe conversion: statement::extract_config enforces that option is a value
                    // between 0 and 3600000
                    .map(|s| match s {
                        StringOrSecret::String(s) => Duration::from_millis(s.parse().unwrap()),
                        StringOrSecret::Secret(_) => unreachable!(),
                    })
                    // By default, rdkafka will check for updated metadata every five minutes:
                    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                    .unwrap_or_else(|| Duration::from_secs(15));

                // We want a fairly low ceiling on our polling frequency, since we rely
                // on this heartbeat to determine the health of our Kafka connection.
                let metadata_refresh_frequency =
                    metadata_refresh_interval.min(Duration::from_secs(60));

                let status_report = Arc::clone(&health_status);

                thread::Builder::new()
                    .name("kafka-metadata".to_string())
                    .spawn(move || {
                        trace!(
                            source_id = config.id.to_string(),
                            worker_id = config.worker_id,
                            num_workers = config.worker_count,
                            refresh_frequency =? metadata_refresh_frequency,
                            "kafka metadata thread: starting..."
                        );
                        while let Some(partition_info) = partition_info.upgrade() {
                            let result =
                                get_kafka_partitions(&consumer, &topic, Duration::from_secs(30));
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
                                    *status_report.lock().unwrap() = Some(HealthStatus::Running);
                                    thread::park_timeout(metadata_refresh_frequency);
                                }
                                Err(e) => {
                                    *status_report.lock().unwrap() =
                                        Some(HealthStatus::StalledWithError {
                                            error: format!("{}", e.display_with_causes()),
                                            hint: None,
                                        });
                                    thread::park_timeout(metadata_refresh_frequency);
                                }
                            }
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

            let source_metrics = SourceReaderMetrics::new(&config.base_metrics, config.id);
            let offset_commit_metrics = source_metrics.offset_commit_metrics();

            let mut reader = KafkaSourceReader {
                topic_name: topic.clone(),
                source_name: config.name.clone(),
                id: config.id,
                partition_consumers: Vec::new(),
                consumer: Arc::clone(&consumer),
                worker_id: config.worker_id,
                worker_count: config.worker_count,
                last_offsets: BTreeMap::new(),
                start_offsets,
                stats_rx,
                partition_info,
                include_headers: self.include_headers.is_some(),
                _metadata_thread_handle: metadata_thread_handle,
                partition_metrics: KafkaPartitionMetrics::new(
                    config.base_metrics,
                    partition_ids,
                    topic.clone(),
                    config.id,
                ),
                health_status,
                partition_capabilities,
            };

            let offset_committer = KafkaOffsetCommiter {
                source_id: config.id,
                worker_id: config.worker_id,
                worker_count: config.worker_count,
                topic_name: topic.clone(),
                consumer,
            };

            let offset_commit_loop = async move {
                tokio::pin!(resume_uppers);
                while let Some(frontier) = resume_uppers.next().await {
                    if let Err(e) = offset_committer.commit_offsets(frontier.clone()).await {
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
            };
            tokio::pin!(offset_commit_loop);

            loop {
                let partition_info = reader.partition_info.lock().unwrap().take();
                if let Some(partitions) = partition_info {
                    let mut max_pid = None;
                    for pid in partitions {
                        max_pid = std::cmp::max(max_pid, Some(pid));
                        let is_responsible = crate::source::responsible_for(
                            &reader.id,
                            reader.worker_id,
                            reader.worker_count,
                            pid,
                        );
                        if is_responsible {
                            reader.ensure_partition(pid);
                            let part_min_ts = Partitioned::with_partition(pid, MzOffset::from(0));
                            reader
                                .partition_capabilities
                                .entry(pid)
                                .or_insert_with(|| data_cap.delayed(&part_min_ts));
                        }
                    }
                    let future_ts = Partitioned::with_range(max_pid, None, MzOffset::from(0));
                    data_cap.downgrade(&future_ts);
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
                            let status = HealthStatusUpdate::from(HealthStatus::StalledWithError {
                                error,
                                hint: None,
                            });
                            health_output.give(&health_cap, status).await;
                        }
                        Ok(message) => {
                            let (message, ts) =
                                construct_source_message(&message, reader.include_headers);
                            if let Some((msg, time, diff)) = reader.handle_message(message, ts) {
                                let pid = time.partition().unwrap();
                                let part_cap = &reader.partition_capabilities[pid];
                                data_output.give(part_cap, (Ok(msg), time, diff)).await;
                            }
                        }
                    }
                }

                reader.update_stats();

                // Take the consumers temporarily to get around borrow checker errors
                let mut consumers = std::mem::take(&mut reader.partition_consumers);
                for consumer in consumers.iter_mut() {
                    while let Some(message) = consumer.get_next_message().transpose() {
                        let message = match message {
                            Ok((msg, ts)) => Ok(reader.handle_message(msg, ts)),
                            Err(err) => Err(err),
                        };
                        match message {
                            Ok(Some((msg, time, diff))) => {
                                let pid = time.partition().unwrap();
                                let part_cap = &reader.partition_capabilities[pid];
                                data_output.give(part_cap, (Ok(msg), time, diff)).await;
                            }
                            Ok(None) => continue,
                            Err(err) => {
                                let pid = consumer.pid();
                                let last_offset = reader
                                    .last_offsets
                                    .get(&pid)
                                    .expect("partition known to be installed");

                                let status = HealthStatus::StalledWithError {
                                    error: format!(
                                        "error consuming from source: {} topic: {topic}: partition:\
                                        {pid} last processed offset: {last_offset} : {err}",
                                        config.name
                                    ),
                                    hint: None,
                                };
                                health_output.give(&health_cap, status.into()).await;
                            }
                        }
                    }
                }
                // We can now put them back
                assert!(reader.partition_consumers.is_empty());
                reader.partition_consumers = consumers;

                for (pid, last_offset) in reader.last_offsets.iter() {
                    let pid_upper = MzOffset::from(u64::try_from(*last_offset + 1).unwrap());
                    let upper = Partitioned::with_partition(*pid, pid_upper);
                    let part_cap = reader.partition_capabilities.get_mut(pid).unwrap();
                    part_cap.downgrade(&upper);
                }

                let status = reader.health_status.lock().unwrap().take();
                if let Some(status) = status {
                    health_output.give(&health_cap, status.into()).await;
                }

                // Wait to be notified while also making progress with offset committing
                tokio::select! {
                    // TODO(petrosagg): remove the timeout and rely purely on librdkafka waking us
                    // up
                    _  = tokio::time::timeout(Duration::from_secs(1), notificator.notified()) => {},
                    // This future is not cancel safe but we are only passing a reference to it in
                    // the select! loop so the future stays on the stack and never gets cancelled
                    // until the end of the function.
                    _ = offset_commit_loop.as_mut() => {},
                }
            }
        });

        (
            stream.as_collection(),
            None,
            health_stream,
            Rc::new(button.press_on_drop()),
        )
    }
}

impl KafkaOffsetCommiter {
    async fn commit_offsets(
        &self,
        frontier: Antichain<Partitioned<PartitionId, MzOffset>>,
    ) -> Result<(), anyhow::Error> {
        use rdkafka::consumer::CommitMode;

        // Generate a list of partitions that this worker is responsible for
        let mut offsets = vec![];
        for ts in frontier.iter() {
            if let Some(pid) = ts.partition() {
                if crate::source::responsible_for(
                    &self.source_id,
                    self.worker_id,
                    self.worker_count,
                    pid,
                ) {
                    offsets.push((pid.clone(), *ts.timestamp()));
                }
            }
        }

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
                || format!("source({}) kafka offset commit", self.source_id),
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
        if self.last_offsets.contains_key(&pid) {
            return;
        }

        let start_offset = self.start_offsets.get(&pid).copied().unwrap_or(0);
        self.create_partition_queue(pid, Offset::Offset(start_offset));

        let prev = self.last_offsets.insert(pid, start_offset - 1);

        assert!(prev.is_none());
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
        self.partition_consumers.push(PartitionConsumer::new(
            partition_id,
            partition_queue,
            self.include_headers,
        ));
        assert_eq!(
            self.consumer
                .assignment()
                .unwrap()
                .elements_for_topic(&self.topic_name)
                .len(),
            self.partition_consumers.len()
        );
    }

    /// Fast-forward consumer to specified Kafka Offset. Prints a warning if failed to do so
    /// Assumption: if offset does not exist (for instance, because of compaction), will seek
    /// to the next available offset
    fn fast_forward_consumer(&self, pid: PartitionId, next_offset: i64) {
        let res = self.consumer.seek(
            &self.topic_name,
            pid,
            Offset::Offset(next_offset),
            Duration::from_secs(1),
        );
        match res {
            Ok(_) => {
                let res = self.consumer.position().unwrap_or_default().to_topic_map();
                let position = res
                    .get(&(self.topic_name.clone(), pid))
                    .and_then(|p| match p {
                        Offset::Offset(o) => Some(o),
                        _ => None,
                    });
                if let Some(position) = position {
                    if *position != next_offset {
                        warn!(
                            source_id = self.id.to_string(),
                            worker_id = self.worker_id,
                            num_workers = self.worker_count,
                            "did not fast-forward consumer on \
                            partition {} to the correct Kafka offset. Currently \
                            at offset: {} Expected offset: {}",
                            pid,
                            position,
                            next_offset
                        );
                    } else {
                        info!(
                            source_id = self.id.to_string(),
                            worker_id = self.worker_id,
                            num_workers = self.worker_count,
                            "successfully fast-forwarded consumer on \
                            partition {} to Kafka offset {}.",
                            pid,
                            position
                        );
                    }
                } else {
                    warn!(
                        source_id = self.id.to_string(),
                        worker_id = self.worker_id,
                        num_workers = self.worker_count,
                        "tried to fast-forward consumer on \
                        partition {} to Kafka offset {}. Could not obtain new consumer position",
                        pid,
                        next_offset
                    );
                }
            }
            Err(e) => error!(
                source_id = self.id.to_string(),
                worker_id = self.worker_id,
                num_workers = self.worker_count,
                "failed to fast-forward consumer for source {}, Error:{}",
                self.source_name,
                e
            ),
        }
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
    /// past the expected offset and seeks the consumer if it is not.
    fn handle_message(
        &mut self,
        message: SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>>,
        (partition, offset): (PartitionId, MzOffset),
    ) -> Option<(
        SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>>,
        Partitioned<PartitionId, MzOffset>,
        Diff,
    )> {
        // Offsets are guaranteed to be 1) monotonically increasing *unless* there is
        // a network issue or a new partition added, at which point the consumer may
        // start processing the topic from the beginning, or we may see duplicate offsets
        // At all times, the guarantee : if we see offset x, we have seen all offsets [0,x-1]
        // that we are ever going to see holds.
        // Offsets are guaranteed to be contiguous when compaction is disabled. If compaction
        // is enabled, there may be gaps in the sequence.
        // If we see an "old" offset, we ast-forward the consumer and skip that message

        // Given the explicit consumer to partition assignment, we should never receive a message
        // for a partition for which we have no metadata
        assert!(self.last_offsets.contains_key(&partition));

        let last_offset_ref = self
            .last_offsets
            .get_mut(&partition)
            .expect("partition known to be installed");

        let last_offset = *last_offset_ref;
        let offset_as_i64: i64 = offset.offset.try_into().expect("offset to be < i64::MAX");
        if offset_as_i64 <= last_offset {
            info!(
                source_id = self.id.to_string(),
                worker_id = self.worker_id,
                num_workers = self.worker_count,
                "kafka message before expected offset, skipping: \
                source {} (reading topic {}, partition {}) \
                received offset {} expected offset {:?}",
                self.source_name,
                self.topic_name,
                partition,
                offset.offset,
                last_offset + 1,
            );
            // Seek to the *next* offset that we have not yet processed
            self.fast_forward_consumer(partition, last_offset + 1);
            // We explicitly should not consume the message as we have already processed it
            // However, we make sure to activate the source to make sure that we get a chance
            // to read from this consumer again (even if no new data arrives)
            None
        } else {
            *last_offset_ref = offset_as_i64;

            let ts = Partitioned::with_partition(partition, offset);
            Some((message, ts, 1))
        }
    }
}

fn construct_source_message(
    msg: &BorrowedMessage<'_>,
    include_headers: bool,
) -> (
    SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>>,
    (PartitionId, MzOffset),
) {
    let headers = match msg.headers() {
        Some(headers) if include_headers => Some(
            headers
                .iter()
                .map(|h| (h.key.into(), h.value.map(|v| v.to_vec())))
                .collect::<Vec<_>>(),
        ),
        _ => None,
    };
    let pid = msg.partition();
    let Ok(offset) = u64::try_from(msg.offset()) else {
        panic!("got negative offset ({}) from otherwise non-error'd kafka message", msg.offset());
    };
    let msg = SourceMessage {
        output: 0,
        upstream_time_millis: msg.timestamp().to_millis(),
        key: msg.key().map(|k| k.to_vec()),
        value: msg.payload().map(|p| p.to_vec()),
        headers,
    };
    (msg, (pid, offset.into()))
}

/// Wrapper around a partition containing the underlying consumer
struct PartitionConsumer {
    /// the partition id with which this consumer is associated
    pid: PartitionId,
    /// The underlying Kafka partition queue
    partition_queue: PartitionQueue<BrokerRewritingClientContext<GlueConsumerContext>>,
    /// Whether or not to unpack and allocate headers and pass them through in the `SourceMessage`
    include_headers: bool,
}

impl PartitionConsumer {
    /// Creates a new partition consumer from underlying Kafka consumer
    fn new(
        pid: PartitionId,
        partition_queue: PartitionQueue<BrokerRewritingClientContext<GlueConsumerContext>>,
        include_headers: bool,
    ) -> Self {
        PartitionConsumer {
            pid,
            partition_queue,
            include_headers,
        }
    }

    /// Returns the next message to process for this partition (if any).
    ///
    /// The outer `Result` represents irrecoverable failures, the inner one can and will
    /// be transformed into empty values.
    ///
    /// The inner `Option` represents if there is a message to process.
    fn get_next_message(
        &mut self,
    ) -> Result<
        Option<(
            SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>>,
            (PartitionId, MzOffset),
        )>,
        KafkaError,
    > {
        match self.partition_queue.poll(Duration::from_millis(0)) {
            Some(Ok(msg)) => {
                let (msg, ts) = construct_source_message(&msg, self.include_headers);
                assert_eq!(ts.0, self.pid);
                Ok(Some((msg, ts)))
            }
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
#[derive(Clone)]
struct GlueConsumerContext {
    notificator: Arc<Notify>,
    stats_tx: crossbeam_channel::Sender<Jsonb>,
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
        MzClientContext.log(level, fac, log_message)
    }
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        MzClientContext.error(error, reason)
    }
}

impl GlueConsumerContext {
    fn activate(&self) {
        self.notificator.notify_waiters();
    }
}

impl ConsumerContext for GlueConsumerContext {}

/// Return the list of partition ids associated with a specific topic
fn get_kafka_partitions<C>(
    consumer: &BaseConsumer<C>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<PartitionId>, anyhow::Error>
where
    C: ConsumerContext,
{
    let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
    let topic_meta = metadata
        .topics()
        .get(0)
        .context("expected a topic in the metadata result")?;

    fn check_err(err: Option<RDKafkaRespErr>) -> anyhow::Result<()> {
        if let Some(err) = err {
            Err(RDKafkaErrorCode::from(err))?
        }
        Ok(())
    }

    check_err(topic_meta.error())?;

    let mut partition_ids = Vec::with_capacity(topic_meta.partitions().len());
    for partition_meta in topic_meta.partitions() {
        check_err(partition_meta.error())?;

        partition_ids.push(partition_meta.id());
    }
    Ok(partition_ids)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::{Message, Offset, TopicPartitionList};
    use uuid::Uuid;

    use mz_kafka_util::client::create_new_client_config_simple;

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
    #[test]
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
