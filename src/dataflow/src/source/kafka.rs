// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryInto;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::executor::block_on;
use futures::sink::SinkExt;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics, TopicPartitionList};
use timely::scheduling::activate::{Activator, SyncActivator};

use dataflow_types::{
    Consistency, DataEncoding, ExternalSourceConnector, KafkaOffset, KafkaSourceConnector,
    MzOffset, Timestamp,
};
use expr::{GlobalId, PartitionId, SourceInstanceId};
use kafka_util::KafkaAddrs;
use log::{error, info, log_enabled, warn};

use crate::server::{
    TimestampDataUpdate, TimestampDataUpdates, TimestampMetadataUpdate, TimestampMetadataUpdates,
    WorkerPersistenceData,
};
use crate::source::{
    ConsistencyInfo, PartitionMetrics, PersistedFileMetadata, PersistenceSender, RecordIter,
    SourceConstructor, SourceInfo, SourceMessage,
};

/// Contains all information necessary to ingest data from Kafka
pub struct KafkaSourceInfo {
    /// Name of the topic on which this source is backed on
    topic_name: String,
    /// Name of the source (will have format kafka-source-id)
    source_name: String,
    /// Source instance ID (stored as a string for logging)
    source_id: String,
    /// Source global id (for persistence)
    source_global_id: GlobalId,
    /// Kafka consumer for this source
    consumer: Arc<BaseConsumer<GlueConsumerContext>>,
    /// List of consumers. A consumer should be assigned per partition to guarantee fairness
    partition_consumers: VecDeque<PartitionConsumer>,
    /// Metadata to keep track of whether a message is buffered at
    /// that partition
    buffered_metadata: HashSet<i32>,
    /// The number of known partitions.
    known_partitions: i32,
    /// Worker ID
    worker_id: i32,
    /// Worker Count
    worker_count: i32,
}

impl SourceConstructor<Vec<u8>> for KafkaSourceInfo {
    fn new(
        source_name: String,
        source_id: SourceInstanceId,
        _active: bool,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        _: &mut ConsistencyInfo,
        _: DataEncoding,
    ) -> Result<KafkaSourceInfo, failure::Error> {
        match connector {
            ExternalSourceConnector::Kafka(kc) => Ok(KafkaSourceInfo::new(
                source_name,
                source_id,
                worker_id,
                worker_count,
                consumer_activator,
                kc,
            )),
            _ => unreachable!(),
        }
    }
}

impl SourceInfo<Vec<u8>> for KafkaSourceInfo {
    fn activate_source_timestamping(
        id: &SourceInstanceId,
        consistency: &Consistency,
        _active: bool,
        timestamp_data_updates: TimestampDataUpdates,
        timestamp_metadata_channel: TimestampMetadataUpdates,
    ) -> Option<TimestampMetadataUpdates> {
        let prev = if let Consistency::BringYourOwn(_) = consistency {
            timestamp_data_updates.borrow_mut().insert(
                id.clone(),
                TimestampDataUpdate::BringYourOwn(HashMap::new()),
            )
        } else {
            timestamp_data_updates
                .borrow_mut()
                .insert(id.clone(), TimestampDataUpdate::RealTime(1))
        };
        // Check that this is the first time this source id is registered
        assert!(prev.is_none());
        timestamp_metadata_channel
            .as_ref()
            .borrow_mut()
            .push(TimestampMetadataUpdate::StartTimestamping(*id));
        Some(timestamp_metadata_channel)
    }

    /// This function determines whether it is safe to close the current timestamp.
    /// It is safe to close the current timestamp if
    /// 1) this worker does not own the current partition
    /// 2) we will never receive a message with a lower or equal timestamp than offset.
    /// This is true if
    ///     a) we have already timestamped a message >= offset
    ///     b) the consumer's position is passed ever returning message <= offset.
    fn can_close_timestamp(
        &self,
        consistency_info: &ConsistencyInfo,
        pid: &PartitionId,
        offset: MzOffset,
    ) -> bool {
        let kafka_pid = match pid {
            PartitionId::Kafka(pid) => *pid,
            // KafkaSourceInfo should only receive PartitionId::Kafka
            _ => unreachable!(),
        };

        let last_offset = consistency_info
            .partition_metadata
            .get(&pid)
            .unwrap()
            .offset;

        // For transactional or compacted topics, the "last_offset" may not correspond to
        // the last record in the topic (either because it has been GCed or because
        // it corresponds to an abort/commit marker).
        // topic and partition entries are not guaranteed to exist if the poll request to metadata has not succeeded:
        // In Kafka, the position of the consumer is set to the offset *after* the last offset that the consumer has
        // processed, so we have to decrement it by one to get the last processed offset

        // We separate these two cases, as consumer.position() is an expensive call that should
        // be avoided if possible. Case 1 and 2.a occur first, and we only test 2.b when necessary
        if !self.has_partition(kafka_pid) // Case 1
        || last_offset >= offset
        // Case 2.a
        {
            true
        } else {
            let mut current_consumer_position: MzOffset = KafkaOffset {
                offset: match self.consumer.position() {
                    Ok(topic_list) => topic_list
                        .elements_for_topic(&self.topic_name)
                        .get(kafka_pid as usize)
                        .map(|el| el.offset().to_raw() - 1),
                    Err(_) => Some(-1),
                }
                .unwrap_or(-1),
            }
            .into();

            // If a message has been buffered (but not timestamped), the consumer will already have
            // moved ahead.
            if self.is_buffered(kafka_pid) {
                current_consumer_position.offset -= 1;
            }

            // Case 2.b
            current_consumer_position >= offset
        }
    }
    /// Returns the number of partitions expected *for this worker*. Partitions are assigned
    /// round-robin in worker id order
    /// Ex: a partition count of 4 for 3 workers will assign worker 0 with partitions 0,3,
    /// worker 1 with partition 1, and worker 2 with partition 2
    fn get_worker_partition_count(&self) -> i32 {
        let pcount = self.known_partitions / self.worker_count;
        if self.worker_id < (self.known_partitions % self.worker_count) {
            pcount + 1
        } else {
            pcount
        }
    }

    /// Returns true if this worker is responsible for this partition
    /// Ex: if pid=0 and worker_id = 0, then true
    /// if pid=1 and worker_id = 0, then false
    fn has_partition(&self, partition_id: PartitionId) -> bool {
        let pid = match partition_id {
            PartitionId::Kafka(pid) => pid,
            _ => unreachable!(),
        };
        (pid % self.worker_count) == self.worker_id
    }

    /// Ensures that a partition queue for `pid` exists.
    /// In Kafka, partitions are assigned contiguously. This function consequently
    /// creates partition queues for every p <= pid
    fn ensure_has_partition(&mut self, consistency_info: &mut ConsistencyInfo, pid: PartitionId) {
        let pid = match pid {
            PartitionId::Kafka(p) => p,
            _ => unreachable!(),
        };
        for i in self.known_partitions..=pid {
            if self.has_partition(i) {
                self.create_partition_queue(i);
                consistency_info.partition_metrics.insert(
                    PartitionId::Kafka(i),
                    PartitionMetrics::new(&self.topic_name, &self.source_id, &i.to_string()),
                );
            }
            consistency_info.update_partition_metadata(PartitionId::Kafka(i));
        }
        self.known_partitions = cmp::max(self.known_partitions, pid + 1);

        assert_eq!(
            self.get_worker_partition_count(),
            self.get_partition_consumers_count()
        );
        assert_eq!(
            self.known_partitions as usize,
            consistency_info.partition_metadata.len()
        );
    }

    /// Updates the Kafka source to reflect the new partition count.
    /// Kafka creates partitions with contiguous IDs, starting from 0.
    /// as PIDs are contiguous, we ensure that we have created partitions up to PID
    /// (partition_count-1) as partitions are 0-indexed.
    fn update_partition_count(
        &mut self,
        consistency_info: &mut ConsistencyInfo,
        partition_count: i32,
    ) {
        self.ensure_has_partition(consistency_info, PartitionId::Kafka(partition_count - 1));
    }

    /// This function checks whether any messages have been buffered. If yes, returns the buffered
    /// message. Otherwise, polls from the next consumer for which a message is available. This function polls the set
    /// round-robin: when a consumer is polled, it is placed at the back of the queue.
    ///
    /// If a message has an offset that is smaller than the next expected offset for this consumer (and this partition)
    /// we skip this message, and seek to the appropriate offset
    fn get_next_message(
        &mut self,
        consistency_info: &mut ConsistencyInfo,
        activator: &Activator,
    ) -> Result<Option<SourceMessage<Vec<u8>>>, anyhow::Error> {
        let mut next_message = None;
        let consumer_count = self.get_partition_consumers_count();
        let mut attempts = 0;
        while attempts < consumer_count {
            let mut partition_queue = self.partition_consumers.pop_front().unwrap();
            let message = partition_queue.get_next_message();
            if let Some(message) = message {
                let partition = match message.partition {
                    PartitionId::Kafka(pid) => pid,
                    _ => unreachable!(),
                };
                // There are no more messages buffered on this pid
                self.buffered_metadata.remove(&partition);
                let offset = message.offset;
                // Offsets are guaranteed to be 1) monotonically increasing *unless* there is
                // a network issue or a new partition added, at which point the consumer may
                // start processing the topic from the beginning, or we may see duplicate offsets
                // At all times, the guarantee : if we see offset x, we have seen all offsets [0,x-1]
                // that we are ever going to see holds.
                // Offsets are guaranteed to be contiguous when compaction is disabled. If compaction
                // is enabled, there may be gaps in the sequence.
                // If we see an "old" offset, we fast-forward the consumer and skip that message

                // Given the explicit consumer to partition assignment, we should never receive a message
                // for a partition for which we have no metadata
                assert!(consistency_info.knows_of(PartitionId::Kafka(partition)));

                let mut last_offset = consistency_info
                    .partition_metadata
                    .get(&PartitionId::Kafka(partition))
                    .unwrap()
                    .offset;

                if offset <= last_offset {
                    warn!(
                        "Kafka message before expected offset: \
                             source {} (reading topic {}, partition {}) \
                             received Mz offset {} expected Mz offset {:?}",
                        self.source_name,
                        self.topic_name,
                        partition,
                        offset,
                        last_offset.offset + 1
                    );
                    // Seek to the *next* offset (aka last_offset + 1) that we have not yet processed
                    last_offset.offset += 1;
                    self.fast_forward_consumer(partition, last_offset.into());
                    // We explicitly should not consume the message as we have already processed it
                    // However, we make sure to activate the source to make sure that we get a chance
                    // to read from this consumer again (even if no new data arrives)
                    activator.activate();
                } else {
                    next_message = Some(message);
                }
            }
            self.partition_consumers.push_back(partition_queue);
            if next_message.is_some() {
                // Found a message, exit the loop and return message
                break;
            } else {
                attempts += 1;
            }
        }
        assert_eq!(
            self.get_partition_consumers_count(),
            self.get_worker_partition_count()
        );
        Ok(next_message)
    }

    fn buffer_message(&mut self, message: SourceMessage<Vec<u8>>) {
        // Guaranteed to exist as we just read from this consumer
        let mut consumer = self.partition_consumers.back_mut().unwrap();
        assert_eq!(message.partition, PartitionId::Kafka(consumer.pid));
        consumer.buffer = Some(message);
        // Mark the partition has buffered
        self.buffered_metadata.insert(consumer.pid);
    }

    fn read_persisted_files(&self, files: &[PathBuf]) -> Vec<(Vec<u8>, Timestamp, i64)> {
        files
            .iter()
            .filter(|f| {
                // We partition the given partitions up amongst workers, so we need to be
                // careful not to process a partition that this worker was not allocated (or
                // else we would process files multiple times).
                let meta = PersistedFileMetadata::from_fname(f.to_str().unwrap()).unwrap();
                self.has_partition(meta.partition_id)
            })
            .flat_map(|f| {
                let data = fs::read(f).unwrap();
                RecordIter { data }.map(|r| (r.data, r.time as u64, r.offset))
            })
            .collect()
    }

    fn persist_message(
        &self,
        persistence_tx: &mut Option<PersistenceSender>,
        message: &SourceMessage<Vec<u8>>,
        timestamp: Timestamp,
    ) {
        // Send this record to be persisted
        if let Some(persistence_tx) = persistence_tx {
            let partition_id = match message.partition {
                PartitionId::Kafka(p) => p,
                _ => unreachable!(),
            };

            // TODO(rkhaitan): let's experiment with wrapping these in a
            // Arc so we don't have to clone.
            let key = message.key.clone().unwrap_or_default();
            let payload = message.payload.clone().unwrap_or_default();

            let persistence_data = WorkerPersistenceData {
                source_id: self.source_global_id,
                partition: partition_id,
                offset: message.offset.offset,
                timestamp,
                key,
                payload,
            };

            let mut connector = persistence_tx.as_mut();

            // TODO(rkhaitan): revisit whether this architecture of blocking
            // within a dataflow operator makes sense.
            block_on(connector.send(persistence_data)).unwrap();
        }
    }
}

impl KafkaSourceInfo {
    /// Constructor
    pub fn new(
        source_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        kc: KafkaSourceConnector,
    ) -> KafkaSourceInfo {
        let KafkaSourceConnector {
            addrs,
            topic,
            config_options,
            group_id_prefix,
            ..
        } = kc;
        let kafka_config =
            create_kafka_config(&source_name, &addrs, group_id_prefix, &config_options);
        let source_global_id = source_id.source_id;
        let source_id = source_id.to_string();
        let consumer: BaseConsumer<GlueConsumerContext> = kafka_config
            .create_with_context(GlueConsumerContext(consumer_activator))
            .expect("Failed to create Kafka Consumer");
        KafkaSourceInfo {
            buffered_metadata: HashSet::new(),
            topic_name: topic,
            source_name,
            source_id,
            source_global_id,
            partition_consumers: VecDeque::new(),
            known_partitions: 0,
            consumer: Arc::new(consumer),
            worker_id: worker_id.try_into().unwrap(),
            worker_count: worker_count.try_into().unwrap(),
        }
    }

    /// Returns true if this worker is responsible for this partition
    /// Ex: if pid=0 and worker_id = 0, then true
    /// if pid=1 and worker_id = 0, then false
    fn has_partition(&self, partition_id: i32) -> bool {
        (partition_id % self.worker_count) == self.worker_id
    }

    /// Returns a count of total number of consumers for this source
    fn get_partition_consumers_count(&self) -> i32 {
        // Note: the number of consumers is guaranteed to always be smaller than
        // expected_partition_count (i32)
        self.partition_consumers.len().try_into().unwrap()
    }

    /// Returns true if a message has been buffered for this partition
    fn is_buffered(&self, pid: i32) -> bool {
        self.buffered_metadata.contains(&pid)
    }

    /// Creates a new partition queue for `partition_id`.
    fn create_partition_queue(&mut self, partition_id: i32) {
        info!(
            "Activating Kafka queue for {} [{}] (source {}) on worker {}",
            self.topic_name, partition_id, self.source_id, self.worker_id
        );

        // Collect old partition assignments
        let tpl = self.consumer.assignment().unwrap();
        // Create list from assignments
        let mut partition_list = TopicPartitionList::new();
        for partition in tpl.elements_for_topic(&self.topic_name) {
            partition_list.add_partition_offset(
                partition.topic(),
                partition.partition(),
                partition.offset(),
            );
        }
        // Add new partition
        partition_list.add_partition_offset(&self.topic_name, partition_id, Offset::Beginning);
        self.consumer
            .assign(&partition_list)
            .expect("assignment known to be valid");

        let partition_queue = self
            .consumer
            .split_partition_queue(&self.topic_name, partition_id)
            .expect("partition known to be valid");
        self.partition_consumers
            .push_front(PartitionConsumer::new(partition_id, partition_queue));
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
    fn fast_forward_consumer(&self, pid: i32, next_offset: KafkaOffset) {
        let res = self.consumer.seek(
            &self.topic_name,
            pid,
            Offset::Offset(next_offset.offset),
            Duration::from_secs(1),
        );
        match res {
            Ok(_) => {
                let res = self.consumer.position().unwrap_or_default().to_topic_map();
                let position = res.get(&(self.topic_name.clone(), pid));
                if let Some(position) = position {
                    let position = position.to_raw();
                    info!(
                        "Tried to fast-forward consumer on partition PID: {} to Kafka offset {}. Consumer is now at position {}",
                        pid, next_offset.offset, position);
                    if position != next_offset.offset {
                        warn!("We did not seek to the expected Kafka offset. Current Kafka offset: {} Expected Kafka offset: {}", position, next_offset.offset);
                    }
                } else {
                    warn!("Tried to fast-forward consumer on partition PID:{} to Kafka offset {}. Could not obtain new consumer position",
                          pid, next_offset.offset);
                }
            }
            Err(e) => error!(
                "Failed to fast-forward consumer for source:{}, Error:{}",
                self.source_name, e
            ),
        }
    }
}

/// Creates a Kafka config.
fn create_kafka_config(
    name: &str,
    addrs: &KafkaAddrs,
    group_id_prefix: Option<String>,
    config_options: &HashMap<String, String>,
) -> ClientConfig {
    let mut kafka_config = ClientConfig::new();

    // Broker configuration.
    kafka_config.set("bootstrap.servers", &addrs.to_string());

    // Opt-out of Kafka's offset management facilities. Whenever we restart,
    // we want to restart from the beginning of the topic.
    //
    // This is likely to change soon. See #3060 and #2490.
    kafka_config
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");

    // How often to refresh metadata from the Kafka broker. This can have a
    // minor impact on startup latency and latency after adding a new partition,
    // as the metadata for a partition must be fetched before we can retrieve
    // data from it. We try to manually trigger metadata fetches when it makes
    // sense, but if those manual fetches fail, this is the interval at which we
    // retry.
    //
    // 30s may seem low, but the default is 5m. More frequent metadata refresh
    // rates are surprising to Kafka users, as topic partition counts hardly
    // ever change in production.
    kafka_config.set("topic.metadata.refresh.interval.ms", "30000"); // 30 seconds

    kafka_config.set("fetch.message.max.bytes", "134217728");

    // Consumer group ID. We'd prefer not to set this at all, as we don't use
    // Kafka's consumer group support, but librdkafka requires it, and users
    // expect it.
    //
    // This is partially dictated by the user and partially dictated by us.
    // Users can set a prefix so they can see which consumers belong to which
    // Materialize deployment, but we set a suffix to ensure uniqueness. A
    // unique consumer group ID is the most surefire way to ensure that
    // librdkafka does not try to perform its own consumer group balancing,
    // which would wreak havoc with our careful partition assignment strategy.
    kafka_config.set(
        "group.id",
        &format!(
            "{}materialize-{}",
            group_id_prefix.unwrap_or_else(String::new),
            name
        ),
    );

    // Ensures that, when processing transactional topics, the consumer does not read data that
    // is not yet committed (and could later abort)
    kafka_config.set("isolation.level", "read_committed");

    // Patch the librdkafka debug log system into the Rust `log` ecosystem.
    // This is a very simple integration at the moment; enabling `debug`-level
    // logs for the `librdkafka` target enables the full firehouse of librdkafka
    // debug logs. We may want to investigate finer-grained control.
    if log_enabled!(target: "librdkafka", log::Level::Debug) {
        kafka_config.set("debug", "all");
    }

    // Set additional configuration operations from the user. While these look
    // arbitrary, other layers of the system tightly control which configuration
    // options are allowable.
    for (k, v) in config_options {
        kafka_config.set(k, v);
    }

    kafka_config
}

impl<'a> From<&BorrowedMessage<'a>> for SourceMessage<Vec<u8>> {
    fn from(msg: &BorrowedMessage<'a>) -> Self {
        let kafka_offset = KafkaOffset {
            offset: msg.offset(),
        };
        Self {
            payload: msg.payload().map(|p| p.to_vec()),
            partition: PartitionId::Kafka(msg.partition()),
            offset: kafka_offset.into(),
            key: msg.key().map(|k| k.to_vec()),
        }
    }
}

/// Wrapper around a partition containing both a buffer and the underlying consumer
/// To read from this partition consumer 1) first check whether the buffer is empty. If not,
/// read from buffer. 2) If buffer is empty, poll consumer to get a new message
struct PartitionConsumer {
    /// the partition id with which this consumer is associated
    pid: i32,
    /// A buffer to store messages that cannot be timestamped yet
    buffer: Option<SourceMessage<Vec<u8>>>,
    /// The underlying Kafka partition queue
    partition_queue: PartitionQueue<GlueConsumerContext>,
}

impl PartitionConsumer {
    /// Creates a new partition consumer from underlying Kafka consumer
    fn new(pid: i32, partition_queue: PartitionQueue<GlueConsumerContext>) -> Self {
        PartitionConsumer {
            pid,
            buffer: None,
            partition_queue,
        }
    }

    /// Returns the next message to process for this partition (if any).
    /// Either reads from the buffer or polls from the consumer
    fn get_next_message(&mut self) -> Option<SourceMessage<Vec<u8>>> {
        if let Some(message) = self.buffer.take() {
            assert_eq!(message.partition, PartitionId::Kafka(self.pid));
            Some(message)
        } else {
            match self.partition_queue.poll(Duration::from_millis(0)) {
                Some(Ok(msg)) => {
                    let result = SourceMessage::from(&msg);
                    assert_eq!(result.partition, PartitionId::Kafka(self.pid));
                    Some(result)
                }
                Some(Err(err)) => {
                    error!("kafka error: {}", err);
                    None
                }
                _ => None,
            }
        }
    }
}

/// An implementation of [`ConsumerContext`] that unparks the wrapped thread
/// when the message queue switches from nonempty to empty.
struct GlueConsumerContext(SyncActivator);

impl ClientContext for GlueConsumerContext {
    fn stats(&self, statistics: Statistics) {
        info!("Client stats: {:#?}", statistics);
    }
}

impl GlueConsumerContext {
    fn activate(&self) {
        self.0
            .activate()
            .expect("timely operator hung up while Kafka source active");
    }
}

impl ConsumerContext for GlueConsumerContext {
    fn message_queue_nonempty_callback(&self) {
        self.activate();
    }
}
