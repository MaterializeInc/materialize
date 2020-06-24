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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dataflow_types::{Consistency, KafkaOffset, KafkaSourceConnector, MzOffset, Timestamp};
use expr::{PartitionId, SourceInstanceId};
use lazy_static::lazy_static;
use log::{error, info, log_enabled, warn};
use prometheus::core::{AtomicI64, AtomicU64};
use prometheus::{
    register_int_counter, register_int_counter_vec, register_int_gauge_vec,
    register_uint_gauge_vec, DeleteOnDropCounter, DeleteOnDropGauge, IntCounter, IntCounterVec,
    IntGaugeVec, UIntGauge, UIntGaugeVec,
};
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics, TopicPartitionList};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::activate::{Activator, SyncActivator};
use url::Url;

use super::util::source;
use super::{SourceConfig, SourceOutput, SourceStatus, SourceToken};
use crate::server::{
    TimestampDataUpdate, TimestampDataUpdates, TimestampMetadataUpdate, TimestampMetadataUpdates,
};

// Global Kafka metrics.
lazy_static! {
    static ref KAFKA_BYTES_READ_COUNTER: IntCounter = register_int_counter!(
        "mz_kafka_bytes_read_total",
        "Count of kafka bytes we have read from the wire"
    )
    .unwrap();
}

/// Per-Kafka source metrics.
pub struct SourceMetrics {
    operator_scheduled_counter: IntCounter,
    capability: UIntGauge,
}

impl SourceMetrics {
    fn new(topic_name: &str, source_id: &str, worker_id: &str) -> SourceMetrics {
        lazy_static! {
            static ref OPERATOR_SCHEDULED_COUNTER: IntCounterVec = register_int_counter_vec!(
                "mz_operator_scheduled_total",
                "The number of times the kafka client got invoked for this source",
                &["topic", "source_id", "worker_id"]
            )
            .unwrap();
            static ref CAPABILITY: UIntGaugeVec = register_uint_gauge_vec!(
                "mz_kafka_capability",
                "The current capability for this dataflow. This corresponds to min(mz_kafka_partition_closed_ts)",
                &["topic", "source_id", "worker_id"]
            )
            .unwrap();
        }
        let labels = &[topic_name, source_id, worker_id];
        SourceMetrics {
            operator_scheduled_counter: OPERATOR_SCHEDULED_COUNTER.with_label_values(labels),
            capability: CAPABILITY.with_label_values(labels),
        }
    }
}

/// Per-Kafka source partition metrics.
pub struct PartitionMetrics {
    offset_ingested: DeleteOnDropGauge<'static, AtomicI64>,
    offset_received: DeleteOnDropGauge<'static, AtomicI64>,
    closed_ts: DeleteOnDropGauge<'static, AtomicU64>,
    messages_ingested: DeleteOnDropCounter<'static, AtomicI64>,
}

impl PartitionMetrics {
    fn new(topic_name: &str, source_id: &str, partition_id: &str) -> PartitionMetrics {
        lazy_static! {
            static ref OFFSET_INGESTED: IntGaugeVec = register_int_gauge_vec!(
                "mz_kafka_partition_offset_ingested",
                "The most recent kafka offset that we have ingested into a dataflow. This correspond to \
                data that we have 1)ingested 2) assigned a timestamp",
                &["topic", "source_id", "partition_id"]
            )
            .unwrap();
            static ref OFFSET_RECEIVED: IntGaugeVec = register_int_gauge_vec!(
                "mz_kafka_partition_offset_received",
                "The most recent kafka offset that we have been received by this source.",
                &["topic", "source_id", "partition_id"]
            )
            .unwrap();
            static ref CLOSED_TS: UIntGaugeVec = register_uint_gauge_vec!(
                "mz_kafka_partition_closed_ts",
                "The highest closed timestamp for each partition in this dataflow",
                &["topic", "source_id", "partition_id"]
            )
            .unwrap();
            static ref MESSAGES_INGESTED: IntCounterVec = register_int_counter_vec!(
                "mz_kafka_messages_ingested",
                "The number of messages ingested per partition.",
                &["topic", "source_id", "partition_id"]
            )
            .unwrap();

        }
        let labels = &[topic_name, source_id, partition_id];
        PartitionMetrics {
            offset_ingested: DeleteOnDropGauge::new_with_error_handler(
                OFFSET_INGESTED.with_label_values(labels),
                &OFFSET_INGESTED,
                |e, v| log::warn!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            offset_received: DeleteOnDropGauge::new_with_error_handler(
                OFFSET_RECEIVED.with_label_values(labels),
                &OFFSET_RECEIVED,
                |e, v| log::warn!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            closed_ts: DeleteOnDropGauge::new_with_error_handler(
                CLOSED_TS.with_label_values(labels),
                &CLOSED_TS,
                |e, v| log::warn!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            messages_ingested: DeleteOnDropCounter::new_with_error_handler(
                MESSAGES_INGESTED.with_label_values(labels),
                &MESSAGES_INGESTED,
                |e, v| log::warn!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
        }
    }
}

// There is other stuff in librdkafka messages, e.g. headers.
// But this struct only contains what we actually use, to avoid unnecessary cloning.
struct MessageParts {
    payload: Option<Vec<u8>>,
    partition: i32,
    offset: KafkaOffset,
    key: Option<Vec<u8>>,
}

impl<'a> From<&BorrowedMessage<'a>> for MessageParts {
    fn from(msg: &BorrowedMessage<'a>) -> Self {
        Self {
            payload: msg.payload().map(|p| p.to_vec()),
            partition: msg.partition(),
            offset: KafkaOffset {
                offset: msg.offset(),
            },
            key: msg.key().map(|k| k.to_vec()),
        }
    }
}

/// Creates a Kafka config.
fn create_kafka_config(
    name: &str,
    url: &Url,
    group_id_prefix: Option<String>,
    config_options: &HashMap<String, String>,
) -> ClientConfig {
    let mut kafka_config = ClientConfig::new();

    // Broker configuration.
    kafka_config.set("bootstrap.servers", &url.to_string());

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

/// Consistency information. Each partition contains information about
/// 1) the last closed timestamp for this partition
/// 2) the last processed offset
#[derive(Copy, Clone)]
struct ConsInfo {
    /// the last closed timestamp for this partition
    pub ts: Timestamp,
    /// the last processed offset
    pub offset: MzOffset,
}

/// Wrapper around a partition containing both a buffer and the underlying consumer
/// To read from this partition consumer 1) first check whether the buffer is empty. If not,
/// read from buffer. 2) If buffer is empty, poll consumer to get a new message
struct PartitionConsumer {
    /// the partition id with which this consumer is associated
    pid: i32,
    /// A buffer to store messages that cannot be timestamped yet
    buffer: Option<MessageParts>,
    /// The underlying Kafka consumer. This consumer is assigned to read from one partition
    /// exclusively
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
    fn get_next_message(&mut self) -> Option<MessageParts> {
        if let Some(message) = self.buffer.take() {
            assert_eq!(message.partition, self.pid);
            Some(message)
        } else {
            match self.partition_queue.poll(Duration::from_millis(0)) {
                Some(Ok(msg)) => {
                    let result = MessageParts::from(&msg);
                    assert_eq!(result.partition, self.pid);
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
/// Contains all information necessary to ingest data from Kafka
struct DataPlaneInfo {
    /// Name of the topic on which this source is backed on
    topic_name: String,
    /// Name of the source (will have format kafka-source-id)
    source_name: String,
    /// Source instance ID (stored as a string for logging)
    source_id: String,
    /// Kafka consumer for this source
    consumer: Arc<BaseConsumer<GlueConsumerContext>>,
    /// List of consumers. A consumer should be assigned per partition to guarantee fairness
    partition_consumers: VecDeque<PartitionConsumer>,
    /// Metadata to keep track of whether a message is buffered at
    /// that partition
    buffered_metadata: HashSet<i32>,
    /// The number of known partitions.
    known_partitions: i32,
    /// Per-source metrics.
    source_metrics: SourceMetrics,
    /// Per-partition metrics.
    partition_metrics: HashMap<i32, PartitionMetrics>,
    /// Worker ID
    worker_id: i32,
    /// Worker Count
    worker_count: i32,
}

impl DataPlaneInfo {
    fn new(
        topic_name: String,
        source_name: String,
        source_id: SourceInstanceId,
        kafka_config: ClientConfig,
        consumer_activator: Arc<Mutex<SyncActivator>>,
        worker_id: usize,
        worker_count: usize,
    ) -> DataPlaneInfo {
        let source_id = source_id.to_string();
        let consumer: BaseConsumer<GlueConsumerContext> = kafka_config
            .create_with_context(GlueConsumerContext(consumer_activator))
            .expect("Failed to create Kafka Consumer");
        DataPlaneInfo {
            source_metrics: SourceMetrics::new(&topic_name, &source_id, &worker_id.to_string()),
            partition_metrics: HashMap::new(),
            buffered_metadata: HashSet::new(),
            topic_name: topic_name.clone(),
            source_name,
            source_id: source_id.clone(),
            partition_consumers: VecDeque::new(),
            known_partitions: 0,
            consumer: Arc::new(consumer),
            worker_id: worker_id.try_into().unwrap(),
            worker_count: worker_count.try_into().unwrap(),
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
    fn has_partition(&self, partition_id: i32) -> bool {
        (partition_id % self.worker_count) == self.worker_id
    }

    /// Returns a count of total number of consumers for this source
    fn get_partition_consumers_count(&self) -> i32 {
        // Note: the number of consumers is guaranteed to always be smaller than
        // expected_partition_count (i32)
        self.partition_consumers.len().try_into().unwrap()
    }

    /// Ensures that a partition queue for `pid` exists.
    fn ensure_partition(&mut self, cp_info: &mut ControlPlaneInfo, pid: i32) {
        for i in self.known_partitions..=pid {
            if self.has_partition(i) {
                self.create_partition_queue(i);
            }
            cp_info.update_partition_metadata(i);
        }
        self.known_partitions = cmp::max(self.known_partitions, pid + 1);

        assert_eq!(
            self.get_worker_partition_count(),
            self.get_partition_consumers_count()
        );
        assert_eq!(
            self.known_partitions as usize,
            cp_info.partition_metadata.len()
        );
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

        // Trick librdkafka into updating its metadata for the topic so that we
        // start seeing data for the partition immediately. Otherwise we might
        // need to wait the full `topic.metadata.refresh.interval.ms` interval.
        //
        // We don't actually care about the results of the metadata refresh, and
        // would prefer not to wait for it to complete, so we execute this
        // request with a timeout of 0s and ignore the result. This relies on an
        // implementation detail, which is that librdkafka does not proactively
        // cancel metadata fetch operations when they reach their timeout.
        // Unfortunately there is no asynchronous metadata fetch API.
        //
        // It is not a problem if the metadata request fails, because the
        // background metadata refresh will retry indefinitely. As long as a
        // background request succeeds eventually, we'll start receiving data
        // for the new partitions.
        //
        // TODO(benesch): remove this if upstream makes this metadata refresh
        // happen automatically [0].
        //
        // [0]: https://github.com/edenhill/librdkafka/issues/2917
        let _ = self
            .consumer
            .fetch_metadata(Some(&self.topic_name), Duration::from_secs(0));

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
        self.partition_metrics.insert(
            partition_id,
            PartitionMetrics::new(&self.topic_name, &self.source_id, &partition_id.to_string()),
        );
    }

    /// This function checks whether any messages have been buffered. If yes, returns the buffered
    /// message. Otherwise, polls from the next consumer for which a message is available. This function polls the set
    /// round-robin: when a consumer is polled, it is placed at the back of the queue.
    ///
    /// If a message has an offset that is smaller than the next expected offset for this consumer (and this partition)
    /// we skip this message, and seek to the appropriate offset
    fn get_next_message(
        &mut self,
        cp_info: &ControlPlaneInfo,
        activator: &Activator,
    ) -> Option<MessageParts> {
        let mut next_message = None;
        let consumer_count = self.get_partition_consumers_count();
        let mut attempts = 0;
        while attempts < consumer_count {
            let mut partition_queue = self.partition_consumers.pop_front().unwrap();
            let message = partition_queue.get_next_message();
            if let Some(message) = message {
                let partition = message.partition;
                // There are no more messages buffered on this pid
                self.buffered_metadata.remove(&partition);
                let offset = MzOffset::from(message.offset);
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
                assert!(cp_info.knows_of(partition));

                let mut last_offset = cp_info.partition_metadata.get(&partition).unwrap().offset;

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
        next_message
    }

    /// Buffer message that could not be timestamped
    /// Assumption: this message necessarily belongs to the last consumer that we tried to
    /// read from.
    fn buffer_message(&mut self, msg: MessageParts) {
        // Guaranteed to exist as we just read from this consumer
        let mut consumer = self.partition_consumers.back_mut().unwrap();
        assert_eq!(msg.partition, consumer.pid);
        consumer.buffer = Some(msg);
        // Mark the partition has buffered
        self.buffered_metadata.insert(consumer.pid);
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

/// Contains all necessary consistency metadata information
struct ControlPlaneInfo {
    /// Last closed timestamp
    last_closed_ts: u64,
    /// Time since last capability downgrade
    time_since_downgrade: Instant,
    /// Frequency at which we should downgrade capability (in milliseconds)
    downgrade_capability_frequency: u64,
    /// Per partition (a partition ID in Kafka is an i32), keep track of the last closed offset
    /// and the last closed timestamp
    partition_metadata: HashMap<i32, ConsInfo>,
    /// Optional: Materialize Offset from which source should start reading (default is 0)
    start_offset: MzOffset,
    /// Source Type (Real-time or BYO)
    source_type: Consistency,
}

impl ControlPlaneInfo {
    fn new(
        start_offset: MzOffset,
        consistency: Consistency,
        timestamp_frequency: Duration,
    ) -> ControlPlaneInfo {
        ControlPlaneInfo {
            last_closed_ts: 0,
            // Safe conversion: statement.rs checks that value specified fits in u64
            downgrade_capability_frequency: timestamp_frequency.as_millis().try_into().unwrap(),
            partition_metadata: HashMap::new(),
            start_offset,
            source_type: consistency,
            time_since_downgrade: Instant::now(),
        }
    }

    /// Returns true if we currently know of particular partition. We know (and have updated the
    /// metadata for this partition) if there is an entry for it
    fn knows_of(&self, pid: i32) -> bool {
        self.partition_metadata.contains_key(&pid)
    }

    /// Updates the underlying partition metadata structure to include the current partition.
    /// New partitions must always be added with a minimum closed offset of (last_closed_ts)
    /// They are guaranteed to only receive timestamp update greater than last_closed_ts (this
    /// is enforced in [coord::timestamp::is_ts_valid]
    fn update_partition_metadata(&mut self, pid: i32) {
        self.partition_metadata.insert(
            pid,
            ConsInfo {
                offset: self.start_offset,
                ts: self.last_closed_ts,
            },
        );
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
                error!("The new timestamp is more than 1 second behind the last assigned timestamp. To\
                avoid unnecessary blocking, Materialize will not attempt to downgrade the capability. Please\
                consider resetting your system time.");
                return None;
            }
        }
        assert!(new_ts > self.last_closed_ts);
        self.last_closed_ts = new_ts;
        Some(self.last_closed_ts)
    }
}

/// This function activates the necessary timestamping information when a source is first created
/// 1) for BYO sources, we notify the timestamping thread that a source has been created.
/// 2) for a RT source, take no steps
//
fn activate_source_timestamping<G>(config: &SourceConfig<G>) -> Option<TimestampMetadataUpdates> {
    let prev = if let Consistency::BringYourOwn(_) = config.consistency {
        config.timestamp_histories.borrow_mut().insert(
            config.id.clone(),
            TimestampDataUpdate::BringYourOwn(HashMap::new()),
        )
    } else {
        config
            .timestamp_histories
            .borrow_mut()
            .insert(config.id.clone(), TimestampDataUpdate::RealTime(0))
    };
    // Check that this is the first time this source id is registered
    assert!(prev.is_none());
    config
        .timestamp_tx
        .as_ref()
        .borrow_mut()
        .push(TimestampMetadataUpdate::StartTimestamping(config.id));
    Some(config.timestamp_tx.clone())
}

/// Creates a Kafka-based timely dataflow source operator.
pub fn kafka<G>(
    config: SourceConfig<G>,
    connector: KafkaSourceConnector,
) -> (
    Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let KafkaSourceConnector {
        url,
        topic,
        config_options,
        start_offset,
        group_id_prefix,
    } = connector;

    let timestamp_channel = activate_source_timestamping(&config);

    let SourceConfig {
        name,
        id,
        scope,
        timestamp_histories,
        worker_id,
        worker_count,
        consistency,
        timestamp_frequency,
        ..
    } = config;

    let (stream, capability) = source(id, timestamp_channel, scope, name.clone(), move |info| {
        // Create activator for source
        let activator = scope.activator_for(&info.address[..]);

        // Create control plane information (Consistency-related information)
        let mut cp_info = ControlPlaneInfo::new(
            MzOffset {
                offset: start_offset,
            },
            consistency.clone(),
            timestamp_frequency,
        );

        // Create dataplane information (Kafka-related information)
        let mut dp_info = DataPlaneInfo::new(
            topic.clone(),
            name.clone(),
            id.clone(),
            create_kafka_config(&name, &url, group_id_prefix, &config_options),
            Arc::new(Mutex::new(scope.sync_activator_for(&info.address[..]))),
            worker_id,
            worker_count,
        );

        move |cap, output| {
            let timer = Instant::now();

            dp_info.source_metrics.operator_scheduled_counter.inc();

            // Accumulate updates to BYTES_READ_COUNTER;
            let mut bytes_read = 0;

            // Trigger any waiting librdkafka callbacks. This should never
            // return any messages, because we've set things up to receive those
            // via individual partition queues.
            {
                let message = dp_info.consumer.poll(Duration::from_secs(0));
                if let Some(message) = message {
                    match message {
                        Ok(message) => {
                            panic!(
                                "Internal Error. Received an unexpected message Source: {} PID: {} Offset: {} on main partition loop.\
                                Materialize will now crash.",
                                id,
                                message.partition(),
                                message.offset()
                            );
                        }
                        Err(e) => error!("Error when polling: {}", e),
                    }
                }
            }

            while let Some(message) = dp_info.get_next_message(&cp_info, &activator) {
                let partition = message.partition;
                let offset = MzOffset::from(message.offset);

                // Update ingestion metrics
                // Entry is guaranteed to exist as it gets created when we initialise the partition.
                dp_info
                    .partition_metrics
                    .get_mut(&partition)
                    .unwrap()
                    .offset_received
                    .set(offset.offset);

                // Determine the timestamp to which we need to assign this message
                let ts = find_matching_timestamp(
                    &mut cp_info,
                    &id,
                    partition,
                    offset,
                    &timestamp_histories,
                );
                match ts {
                    None => {
                        // We have not yet decided on a timestamp for this message,
                        // we need to buffer the message
                        dp_info.buffer_message(message);
                        downgrade_capability(
                            &id,
                            cap,
                            &mut cp_info,
                            &mut dp_info,
                            &timestamp_histories,
                        );
                        activator.activate();
                        return SourceStatus::Alive;
                    }
                    Some(ts) => {
                        // Note: empty and null payload/keys are currently
                        // treated as the same thing.
                        let key = message.key.unwrap_or_default();
                        let out = message.payload.unwrap_or_default();
                        // Entry for partition_metadata is guaranteed to exist as messages
                        // are only processed after we have updated the partition_metadata for a
                        // partition and created a partition queue for it.
                        cp_info
                            .partition_metadata
                            .get_mut(&partition)
                            .unwrap()
                            .offset = offset;
                        bytes_read += key.len() as i64;
                        bytes_read += out.len() as i64;
                        let ts_cap = cap.delayed(&ts);
                        output.session(&ts_cap).give(SourceOutput::new(
                            key,
                            out,
                            Some(Into::<KafkaOffset>::into(offset).offset),
                        ));

                        // Update ingestion metrics
                        // Entry is guaranteed to exist as it gets created when we initialise the partition
                        let partition_metrics =
                            &mut dp_info.partition_metrics.get_mut(&partition).unwrap();
                        partition_metrics.offset_ingested.set(offset.offset);
                        partition_metrics.messages_ingested.inc();
                    }
                }

                if timer.elapsed().as_millis() > 10 {
                    // We didn't drain the entire queue, so indicate that we
                    // should run again. We suppress the activation when the
                    // queue is drained, as in that case librdkafka is
                    // configured to unpark our thread when a new message
                    // arrives.
                    if bytes_read > 0 {
                        KAFKA_BYTES_READ_COUNTER.inc_by(bytes_read);
                    }
                    // Downgrade capability (if possible) before exiting
                    downgrade_capability(
                        &id,
                        cap,
                        &mut cp_info,
                        &mut dp_info,
                        &timestamp_histories,
                    );
                    activator.activate();
                    return SourceStatus::Alive;
                }
            }
            if bytes_read > 0 {
                KAFKA_BYTES_READ_COUNTER.inc_by(bytes_read);
            }

            // Downgrade capability (if possible) before exiting
            downgrade_capability(&id, cap, &mut cp_info, &mut dp_info, &timestamp_histories);

            // Ensure that we activate the source frequently enough to keep downgrading
            // capabilities, even when no data has arrived
            activator.activate_after(Duration::from_millis(
                cp_info.downgrade_capability_frequency,
            ));
            SourceStatus::Alive
        }
    });
    (stream, Some(capability))
}

/// For a given offset, returns an option type returning the matching timestamp or None
/// if no timestamp can be assigned.
///
/// The timestamp history contains a sequence of
/// (partition_count, timestamp, offset) tuples. A message with offset x will be assigned the first timestamp
/// for which offset>=x.
fn find_matching_timestamp(
    cp_info: &mut ControlPlaneInfo,
    id: &SourceInstanceId,
    partition: i32,
    offset: MzOffset,
    timestamp_histories: &TimestampDataUpdates,
) -> Option<Timestamp> {
    if let Consistency::RealTime = cp_info.source_type {
        // Simply assign to this message the next timestamp that is not closed
        Some(cp_info.last_closed_ts + 1)
    } else {
        // The source is a BYO source, we must check the TimestampHistories to obtain a
        match timestamp_histories.borrow().get(id) {
            None => None,
            Some(TimestampDataUpdate::BringYourOwn(entries)) => {
                match entries.get(&PartitionId::Kafka(partition)) {
                    Some(entries) => {
                        for (_, ts, max_offset) in entries {
                            if offset <= *max_offset {
                                return Some(ts.clone());
                            }
                        }
                        None
                    }
                    None => None,
                }
            }
            _ => panic!("Unexpected entry format in TimestampDataUpdates for BYO source"),
        }
    }
}

/// This function determines whether it is safe to close the current timestamp.
/// It is safe to close the current timestamp if
/// 1) this worker does not own the current partition
/// 2) we will never receive a message with a lower or equal timestamp than offset.
/// This is true if
///     a) we have already timestamped a message >= offset
///     b) the consumer's position is passed ever returning message <= offset. This is the case if
///     the consumer's position is at an offset that is strictly greater than
fn can_close_timestamp(
    cp_info: &ControlPlaneInfo,
    dp_info: &DataPlaneInfo,
    pid: i32,
    offset: MzOffset,
) -> bool {
    let last_offset = cp_info.partition_metadata.get(&pid).unwrap().offset;

    // For transactional or compacted topics, the "last_offset" may not correspond to
    // the last record in the topic (either because it has been GCed or because
    // it corresponds to an abort/commit marker).
    // topic and partition entries are not guaranteed to exist if the poll request to metadata has not succeeded:
    // In Kafka, the position of the consumer is set to the offset *after* the last offset that the consumer has
    // processed, so we have to decrement it by one to get the last processed offset

    // We separate these two cases, as consumer.position() is an expensive call that should
    // be avoided if possible. Case 1 and 2.a occur first, and we only test 2.b when necessary
    if !dp_info.has_partition(pid) // Case 1
        || last_offset >= offset
    // Case 2.a
    {
        true
    } else {
        let mut current_consumer_position: MzOffset = KafkaOffset {
            offset: match dp_info.consumer.position() {
                Ok(topic_list) => topic_list
                    .elements_for_topic(&dp_info.topic_name)
                    .get(pid as usize)
                    .map(|el| el.offset().to_raw() - 1),
                Err(_) => Some(-1),
            }
            .unwrap_or(-1),
        }
        .into();

        // If a message has been buffered (but not timestamped), the consumer will already have
        // moved ahead.
        if dp_info.is_buffered(pid) {
            current_consumer_position.offset -= 1;
        }

        // Case 2.b
        current_consumer_position >= offset
    }
}

/// Timestamp history map is of format [pid1: (p_ct, ts1, offset1), (p_ct, ts2, offset2), pid2: (p_ct, ts1, offset)...].
/// For a given partition pid, messages in interval [0,offset1] get assigned ts1, all messages in interval [offset1+1,offset2]
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
/// partitions. This is guaranteed by the [coord::timestamp::is_valid_ts] method.
///
fn downgrade_capability(
    id: &SourceInstanceId,
    cap: &mut Capability<Timestamp>,
    cp_info: &mut ControlPlaneInfo,
    dp_info: &mut DataPlaneInfo,
    timestamp_histories: &TimestampDataUpdates,
) {
    let mut changed = false;

    if let Consistency::BringYourOwn(_) = cp_info.source_type {
        // Determine which timestamps have been closed. A timestamp is closed once we have processed
        // all messages that we are going to process for this timestamp across all partitions
        // In practice, the following happens:
        // Per partition, we iterate over the data structure to remove (ts,offset) mappings for which
        // we have seen all records <= offset. We keep track of the last "closed" timestamp in that partition
        // in next_partition_ts
        if let Some(entries) = timestamp_histories.borrow_mut().get_mut(id) {
            match entries {
                TimestampDataUpdate::BringYourOwn(entries) => {
                    // Iterate over each partition that we know about
                    for (pid, entries) in entries {
                        let pid = match pid {
                            PartitionId::Kafka(pid) => *pid,
                            _ => unreachable!(
                                "timestamp.rs should never send messages with non-Kafka partitions \
                                   to Kafka sources."
                            ),
                        };

                        dp_info.ensure_partition(cp_info, pid);

                        // Check whether timestamps can be closed on this partition
                        while let Some((partition_count, ts, offset)) = entries.front() {
                            assert!(
                                *offset >= cp_info.start_offset,
                                "Internal error! Timestamping offset went below start: {} < {}. Materialize will now crash.",
                                offset, cp_info.start_offset
                            );

                            assert!(
                                *ts > 0,
                                "Internal error! Received a zero-timestamp. Materialize will crash now."
                            );

                            // This timestamp update was done with the expectation that there were more partitions
                            // than we know about. Partition IDs are assigned contiguously
                            // starting from zero, so seeing a `partition_count` of N means
                            // that we should have partitions up to N-1.
                            dp_info.ensure_partition(cp_info, partition_count - 1);

                            if let Some(pmetrics) = dp_info.partition_metrics.get_mut(&pid) {
                                pmetrics
                                    .closed_ts
                                    .set(cp_info.partition_metadata.get(&pid).unwrap().ts);
                            }

                            if can_close_timestamp(cp_info, dp_info, pid, *offset) {
                                // We have either 1) seen all messages corresponding to this timestamp for this
                                // partition 2) do not own this partition 3) the consumer has forwarded past the
                                // timestamped offset. Either way, we have the guarantee tha we will never see a message with a < timestamp
                                // again
                                // We can close the timestamp (on this partition) and remove the associated metadata
                                cp_info.partition_metadata.get_mut(&pid).unwrap().ts = *ts;
                                entries.pop_front();
                                changed = true;
                            } else {
                                // Offset isn't at a timestamp boundary, we take no action
                                break;
                            }
                        }
                    }
                }
                _ => panic!("Unexpected timestamp message format. Expected BYO update."),
            }
        }

        //  Next, we determine the maximum timestamp that is fully closed. This corresponds to the minimum
        //  timestamp across partitions.
        let min = cp_info
            .partition_metadata
            .iter()
            .map(|(_, cons_info)| cons_info.ts)
            .min()
            .unwrap_or(0);

        // Downgrade capability to new minimum open timestamp (which corresponds to min + 1).
        if changed && min > 0 {
            dp_info.source_metrics.capability.set(min);
            cap.downgrade(&(&min + 1));
            cp_info.last_closed_ts = min;
        }
    } else {
        // This a RT source. It is always possible to close the timestamp and downgrade the
        // capability
        if let Some(entries) = timestamp_histories.borrow_mut().get_mut(id) {
            match entries {
                TimestampDataUpdate::RealTime(partition_count) => {
                    dp_info.ensure_partition(cp_info, *partition_count)
                }
                _ => panic!("Unexpected timestamp message. Expected RT update."),
            }
        }

        if cp_info.time_since_downgrade.elapsed().as_millis()
            > cp_info.downgrade_capability_frequency.try_into().unwrap()
        {
            let ts = cp_info.generate_next_timestamp();
            if let Some(ts) = ts {
                dp_info.source_metrics.capability.set(ts);
                cap.downgrade(&(&ts + 1));
            }
            cp_info.time_since_downgrade = Instant::now();
        }
    }
}

/// An implementation of [`ConsumerContext`] that unparks the wrapped thread
/// when the message queue switches from nonempty to empty.
struct GlueConsumerContext(Arc<Mutex<SyncActivator>>);

impl ClientContext for GlueConsumerContext {
    fn stats(&self, statistics: Statistics) {
        info!("Client stats: {:#?}", statistics);
    }
}

impl GlueConsumerContext {
    fn activate(&self) {
        let activator = self.0.lock().unwrap();
        activator
            .activate()
            .expect("timely operator hung up while Kafka source active");
    }
}

impl ConsumerContext for GlueConsumerContext {
    fn message_queue_nonempty_callback(&self) {
        self.activate();
    }
}
