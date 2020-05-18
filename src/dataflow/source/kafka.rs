// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::iter;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lazy_static::lazy_static;
use log::{error, info, warn};
use prometheus::{register_int_counter, register_int_gauge_vec, IntCounter, IntGaugeVec};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics, TopicPartitionList};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::activate::{Activator, SyncActivator};
use url::Url;

use dataflow_types::{
    ExternalSourceConnector, KafkaOffset, KafkaSourceConnector, MzOffset, Timestamp,
};
use expr::{PartitionId, SourceInstanceId};

use super::util::source;
use super::{SourceConfig, SourceStatus, SourceToken};
use crate::server::TimestampHistories;

lazy_static! {
    static ref BYTES_READ_COUNTER: IntCounter = register_int_counter!(
        "mz_kafka_bytes_read_total",
        "Count of kafka bytes we have read from the wire"
    )
    .unwrap();
    static ref KAFKA_PARTITION_OFFSET_INGESTED: IntGaugeVec = register_int_gauge_vec!(
        "mz_kafka_partition_offset_ingested",
        "The most recent kafka offset that we have ingested into a dataflow. This correspond to \
        data that we have 1)ingested 2) assigned a timestamp",
        &["topic", "source_id", "partition_id"]
    )
    .unwrap();
    static ref KAFKA_PARTITION_OFFSET_RECEIVED: IntGaugeVec = register_int_gauge_vec!(
        "mz_kafka_partition_offset_received",
        "The most recent kafka offset that we have been received by this source.",
        &["topic", "source_id", "partition_id"]
    )
    .unwrap();
    static ref KAFKA_PARTITION_CLOSED_TS: IntGaugeVec = register_int_gauge_vec!(
        "mz_kafka_partition_closed_ts",
        "The highest closed timestamp for each partition in this dataflow",
        &["topic", "source_id", "partition_id"]
    )
    .unwrap();
    static ref KAFKA_CAPABILITY: IntGaugeVec = register_int_gauge_vec!(
        "mz_kafka_capability",
        "The current capability for this dataflow. This corresponds to min(mz_kafka_partition_closed_ts)",
        &["topic", "source_id"]
    )
    .unwrap();
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

/// Creates a Kafka config
fn create_kafka_config(
    name: &str,
    url: &Url,
    group_id_prefix: Option<String>,
    config_options: &HashMap<String, String>,
) -> ClientConfig {
    let mut kafka_config = ClientConfig::new();
    let group_id_prefix = group_id_prefix.unwrap_or_else(String::new);
    kafka_config.set(
        "group.id",
        &format!("{}materialize-{}", group_id_prefix, name),
    );
    kafka_config
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("max.poll.interval.ms", "300000") // 5 minutes
        .set("fetch.message.max.bytes", "134217728")
        .set("enable.sparse.connections", "true")
        .set("bootstrap.servers", &url.to_string())
        .set("partition.assignment.strategy", "roundrobin");

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
    consumer: BaseConsumer<GlueConsumerContext>,
}

impl PartitionConsumer {
    /// Creates a new partition consumer from underlying Kafka consumer
    fn new(pid: i32, consumer: BaseConsumer<GlueConsumerContext>) -> Self {
        PartitionConsumer {
            pid,
            buffer: None,
            consumer,
        }
    }

    /// Returns the next message to process for this partition (if any).
    /// Either reads from the buffer or polls from the consumer
    fn get_next_message(&mut self) -> Option<MessageParts> {
        if let Some(message) = self.buffer.take() {
            assert_eq!(message.partition, self.pid);
            Some(message)
        } else {
            match self.consumer.poll(Duration::from_millis(0)) {
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
    /// Kafka Configuration Parameters
    kafka_config: ClientConfig,
    /// List of consumers. A consumer should be assigned per partition to guarantee fairness
    consumers: VecDeque<PartitionConsumer>,
    /// The expected number of partitions. If needs_refresh is set to false, then
    /// expected_partition_count == consumers.len() == partition_metadata.len()
    expected_partition_count: i32,
    /// Field is set to true when we have detected that metadata is out-of-date and needs
    /// to be refreshed
    needs_refresh: bool,
    /// Activator shared across consumers. Anytime a consumer receives a new message, the source
    /// will get scheduled.
    consumer_activator: Arc<Mutex<SyncActivator>>,
}

impl DataPlaneInfo {
    fn new(
        topic_name: String,
        source_name: String,
        kafka_config: ClientConfig,
        consumer_activator: Arc<Mutex<SyncActivator>>,
    ) -> DataPlaneInfo {
        DataPlaneInfo {
            topic_name,
            source_name,
            kafka_config,
            consumers: VecDeque::new(),
            needs_refresh: true,
            expected_partition_count: 1,
            consumer_activator,
        }
    }

    /// Returns a count of total number of consumers for this source
    fn get_consumer_count(&self) -> i32 {
        // Note: the number of consumers is guaranteed to always be smaller than
        // expected_partition_count (i32)
        self.consumers.len().try_into().unwrap()
    }

    /// Refreshes source information for the expected partition count
    /// 1) creates new consumers and assigns each to an individual partition
    /// 2) creates appropriate source metadata
    pub fn refresh_source_information(&mut self, cp_info: &mut ControlPlaneInfo) {
        if self.needs_refresh {
            info!(
                "Refreshing Source Metadata for Source {} Partition Count: {}",
                self.source_name, self.expected_partition_count
            );
            self.update_consumer_list();
            cp_info.update_partition_metadata(self.expected_partition_count);
            self.needs_refresh = false;
        } // else take no action

        // Verify that invariants hold
        assert!(!self.needs_refresh);
        assert_eq!(cp_info.get_partition_count(), self.expected_partition_count);

        assert_eq!(self.get_consumer_count(), self.expected_partition_count);
    }

    /// Marks a source as needing refresh, along with the expected partition count
    pub fn needs_refresh(&mut self, expected_pcount: i32) {
        self.needs_refresh = true;
        self.expected_partition_count = expected_pcount
    }

    /// Update the list of Kafka consumers to match the number of partitions
    /// We currently create one consumer per partition
    fn update_consumer_list(&mut self) {
        let next_pid = self.get_consumer_count();
        let to_add = self.expected_partition_count - next_pid;
        // Kafka Partitions are assigned Ids in a monotonically increasing fashion,
        // starting from 0
        for i in 0..to_add {
            let pid: i32 = next_pid + i;
            self.create_partition_consumer(pid);
        }
        assert_eq!(self.expected_partition_count, self.get_consumer_count());
    }

    /// Create a new kafka consumer and adds it to the list of existing consumers
    fn create_partition_consumer(&mut self, partition_id: i32) {
        let cx = GlueConsumerContext(self.consumer_activator.clone());
        let consumer: BaseConsumer<GlueConsumerContext> = self
            .kafka_config
            .create_with_context(cx)
            .expect("Failed to create Kafka Consumer");
        let mut partition_list = TopicPartitionList::with_capacity(1);
        partition_list.add_partition(&self.topic_name, partition_id);
        consumer.assign(&partition_list).unwrap();
        self.consumers
            .push_front(PartitionConsumer::new(partition_id, consumer))
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
        assert_eq!(self.get_consumer_count(), self.expected_partition_count);
        let mut next_message = None;
        let consumer_count = self.get_consumer_count();
        let mut attempts = 0;
        while attempts < consumer_count {
            let mut consumer = self.consumers.pop_front().unwrap();
            let message = consumer.get_next_message();
            if let Some(message) = message {
                let partition = message.partition;
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

                let mut last_offset = cp_info.partition_metadata[partition as usize].offset;

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
                    // Seek to the *next* offset (ak last_offset + 1) that we have not yet processed
                    last_offset.offset += 1;
                    self.fast_forward_consumer(&consumer, partition, last_offset.into());
                    // We explicitly should not consume the message as we have already processed it
                    // However, we make sure to activate the source to make sure that we get a chance
                    // to read from this consumer again (even if no new data arrives)
                    activator.activate();
                } else {
                    next_message = Some(message);
                }
            }
            self.consumers.push_back(consumer);
            if next_message.is_some() {
                // Found a message, exit the loop and return message
                break;
            } else {
                attempts += 1;
            }
        }
        assert_eq!(self.get_consumer_count(), self.expected_partition_count);
        next_message
    }

    /// Buffer message that could not be timestamped
    /// Assumption: this message necessarily belongs to the last consumer that we tried to
    /// read from.
    fn buffer_message(&mut self, msg: MessageParts) {
        // Guaranteed to exist as we just read from this consumer
        let mut consumer = self.consumers.back_mut().unwrap();
        assert_eq!(msg.partition, consumer.pid);
        consumer.buffer = Some(msg);
    }

    /// Fast-forward consumer to specified Kafka Offset. Prints a warning if failed to do so
    /// Assumption: if offset does not exist (for instance, because of compaction), will seek
    /// to the next available offset
    fn fast_forward_consumer(
        &self,
        consumer: &PartitionConsumer,
        pid: i32,
        next_offset: KafkaOffset,
    ) {
        let res = consumer.consumer.seek(
            &self.topic_name,
            pid,
            Offset::Offset(next_offset.offset),
            Duration::from_secs(1),
        );
        match res {
            Ok(_) => {
                let res = consumer
                    .consumer
                    .position()
                    .unwrap_or_default()
                    .to_topic_map();
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
    last_closed_ts: u64,
    /// Per partition (a partition ID in Kafka is an i32), keep track of the last closed offset
    /// and the last closed timestamp
    partition_metadata: Vec<ConsInfo>,
    /// Optional: Materialize Offset from which source should start reading (default is 0)
    start_offset: MzOffset,
}

impl ControlPlaneInfo {
    fn new(start_offset: MzOffset) -> ControlPlaneInfo {
        ControlPlaneInfo {
            last_closed_ts: 0,
            partition_metadata: vec![ConsInfo {
                offset: MzOffset {
                    offset: start_offset.offset,
                },
                ts: 0,
            }],
            start_offset,
        }
    }

    /// Returns the current number of partitions for which we have metadata entries
    fn get_partition_count(&self) -> i32 {
        // The number of partitions is always guaranteed to be smaller or equal than
        // expected_partition_count (i32)
        self.partition_metadata.len().try_into().unwrap()
    }

    /// Returns true if we currently know of particular partition. We know (and have updated the
    /// metadata for this partition) if there is an entry for it
    fn knows_of(&self, pid: i32) -> bool {
        pid < self.get_partition_count()
    }

    /// Updates the underlying partition metadata structure ot the expected partition count.
    /// New partitions must always be added with a minimum closed offset of (last_closed_ts)
    /// They are guaranteed to only receive timestamp update greater than last_closed_ts (this
    /// is enforced in [coord::timestamp::is_ts_valid]
    fn update_partition_metadata(&mut self, expected_pcount: i32) {
        self.partition_metadata.extend(
            iter::repeat(ConsInfo {
                offset: self.start_offset,
                ts: self.last_closed_ts,
            })
            .take((expected_pcount - self.get_partition_count()) as usize),
        );
        assert_eq!(expected_pcount, self.get_partition_count());
    }
}

/// This function activates the necessary timestamping information when a source is first created
/// 1) it inserts an entry in the timestamp_history datastructure
/// 2) it notifies the coordinator that timestamping should begin by inserting an entry in the
/// timestamp_tx channel
fn activate_source_timestamping<G>(config: &SourceConfig<G>, connector: KafkaSourceConnector) {
    let prev = config
        .timestamp_histories
        .borrow_mut()
        .insert(config.id.clone(), HashMap::new());
    // Check that this is the first time this source id is registered
    assert!(prev.is_none());
    config.timestamp_tx.as_ref().borrow_mut().push((
        config.id,
        Some((
            ExternalSourceConnector::Kafka(connector),
            config.consistency.clone(),
        )),
    ));
}

/// Creates a Kafka-based timely dataflow source operator.
pub fn kafka<G>(
    config: SourceConfig<G>,
    connector: KafkaSourceConnector,
) -> (
    Stream<G, (Vec<u8>, (Vec<u8>, Option<i64>))>,
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
    } = connector.clone();

    activate_source_timestamping(&config, connector);

    let SourceConfig {
        name,
        id,
        scope,
        active,
        timestamp_histories,
        timestamp_tx,
        ..
    } = config;

    let (stream, capability) = source(
        id,
        if active { Some(timestamp_tx) } else { None },
        scope,
        &name.clone(),
        move |info| {
            // Create activator for source
            let activator = scope.activator_for(&info.address[..]);

            // Create control plane information (Consistency-related information)
            let mut cp_info = ControlPlaneInfo::new(MzOffset {
                offset: start_offset,
            });

            // Create dataplane information (Kafka-related information)
            let mut dp_info = if active {
                let mut dp_info = DataPlaneInfo::new(
                    topic.clone(),
                    name.clone(),
                    create_kafka_config(&name, &url, group_id_prefix, &config_options),
                    Arc::new(Mutex::new(scope.sync_activator_for(&info.address[..]))),
                );
                dp_info.refresh_source_information(&mut cp_info);
                Some(dp_info)
            } else {
                None
            };

            move |cap, output| {
                // Accumulate updates to BYTES_READ_COUNTER;
                let mut bytes_read = 0;
                if let Some(mut dp_info) = dp_info.as_mut() {
                    // Repeatedly interrogate Kafka for messages. Cease when
                    // Kafka stops returning new data, or after 10 milliseconds.
                    let timer = std::time::Instant::now();

                    // Check if the capability can be downgraded (this is independent of whether
                    // there are new messages that can be processed) as timestamps can become
                    // closed in the absence of messages
                    downgrade_capability(
                        &id,
                        cap,
                        &mut cp_info,
                        &mut dp_info,
                        &timestamp_histories,
                    );

                    while let Some(message) = dp_info.get_next_message(&cp_info, &activator) {
                        let partition = message.partition;
                        let offset = MzOffset::from(message.offset);

                        KAFKA_PARTITION_OFFSET_RECEIVED
                            .with_label_values(&[
                                &dp_info.topic_name,
                                &id.to_string(),
                                &partition.to_string(),
                            ])
                            .set(offset.offset);

                        // Determine the timestamp to which we need to assign this message
                        let ts =
                            find_matching_timestamp(&id, partition, offset, &timestamp_histories);
                        match ts {
                            None => {
                                // We have not yet decided on a timestamp for this message,
                                // we need to buffer the message
                                dp_info.buffer_message(message);
                                activator.activate();
                                return SourceStatus::Alive;
                            }
                            Some(ts) => {
                                // Note: empty and null payload/keys are currently
                                // treated as the same thing.
                                let key = message.key.unwrap_or_default();
                                let out = message.payload.unwrap_or_default();
                                cp_info.partition_metadata[partition as usize].offset = offset;
                                bytes_read += key.len() as i64;
                                bytes_read += out.len() as i64;
                                let ts_cap = cap.delayed(&ts);
                                output.session(&ts_cap).give((
                                    key,
                                    (out, Some(Into::<KafkaOffset>::into(offset).offset)),
                                ));

                                KAFKA_PARTITION_OFFSET_INGESTED
                                    .with_label_values(&[
                                        &dp_info.topic_name,
                                        &id.to_string(),
                                        &partition.to_string(),
                                    ])
                                    .set(offset.offset);

                                downgrade_capability(
                                    &id,
                                    cap,
                                    &mut cp_info,
                                    &mut dp_info,
                                    &timestamp_histories,
                                );
                            }
                        }

                        if timer.elapsed().as_millis() > 10 {
                            // We didn't drain the entire queue, so indicate that we
                            // should run again. We suppress the activation when the
                            // queue is drained, as in that case librdkafka is
                            // configured to unpark our thread when a new message
                            // arrives.
                            activator.activate();
                            return SourceStatus::Alive;
                        }
                    }
                }
                // Ensure that we poll kafka more often than the eviction timeout
                activator.activate_after(Duration::from_secs(60));
                if bytes_read > 0 {
                    BYTES_READ_COUNTER.inc_by(bytes_read);
                }
                SourceStatus::Alive
            }
        },
    );

    if config.active {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}

/// For a given offset, returns an option type returning the matching timestamp or None
/// if no timestamp can be assigned. The timestamp history contains a sequence of
/// (partition_count, timestamp, offset) tuples. A message with offset x will be assigned the first timestamp
/// for which offset>=x.
fn find_matching_timestamp(
    id: &SourceInstanceId,
    partition: i32,
    offset: MzOffset,
    timestamp_histories: &TimestampHistories,
) -> Option<Timestamp> {
    match timestamp_histories.borrow().get(id) {
        None => None,
        Some(entries) => match entries.get(&PartitionId::Kafka(partition)) {
            Some(entries) => {
                for (_, ts, max_offset) in entries {
                    if offset <= *max_offset {
                        return Some(ts.clone());
                    }
                }
                None
            }
            None => None,
        },
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
    timestamp_histories: &TimestampHistories,
) {
    let mut changed = false;

    // Determine which timestamps have been closed. A timestamp is closed once we have processed
    // all messages that we are going to process for this timestamp across all partitions
    // In practice, the following happens:
    // Per partition, we iterate over the data structure to remove (ts,offset) mappings for which
    // we have seen all records <= offset. We keep track of the last "closed" timestamp in that partition
    // in next_partition_ts
    if let Some(entries) = timestamp_histories.borrow_mut().get_mut(id) {
        // Iterate over each partition that we know about
        for (pid, entries) in entries {
            let pid = match pid {
                PartitionId::Kafka(pid) => *pid,
                _ => unreachable!(
                    "timestamp.rs should never send messages with non-Kafka partitions \
                                   to Kafka sources."
                ),
            };

            // There is an entry for a partition for which we have no metadata. Must refresh before
            // continuing
            if pid >= cp_info.get_partition_count() {
                // PIDs in Kafka are monotonically increasing and 0-indexed Finding a PID of x means there
                // must be at least (x+1) partitions
                dp_info.needs_refresh(pid + 1);
                dp_info.refresh_source_information(cp_info);
            }

            let last_offset = cp_info.partition_metadata[pid as usize].offset;

            // Check whether timestamps can be closed on this partition
            while let Some((partition_count, ts, offset)) = entries.first() {
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
                // than we know about. We have to refresh metadata before continuing
                if *partition_count > cp_info.get_partition_count() {
                    // New partition has been added, must refresh metadata before continuing
                    dp_info.needs_refresh(*partition_count);
                    dp_info.refresh_source_information(cp_info);
                }

                // This assertion makes sure that if we ever fast-forwarded the empty stream
                // (any message of the form "PID,TS,0"), we have correctly removed the (_,_,0) entry
                // even if data has subsequently been added to the stream
                assert!(offset.offset != 0 || last_offset.offset == 0);

                KAFKA_PARTITION_CLOSED_TS
                    .with_label_values(&[&dp_info.topic_name, &id.to_string(), &pid.to_string()])
                    .set(
                        (cp_info.partition_metadata[pid as usize].ts)
                            .try_into()
                            .unwrap(),
                    );

                if last_offset >= *offset {
                    // We have now seen all messages corresponding to this timestamp for this
                    // partition. We
                    // can close the timestamp (on this partition) and remove the associated metadata
                    cp_info.partition_metadata[pid as usize].ts = *ts;
                    entries.remove(0);
                    changed = true;
                } else {
                    // Offset isn't at a timestamp boundary, we take no action
                    break;
                }
            }
        }
    }

    //  Next, we determine the maximum timestamp that is fully closed. This corresponds to the minimum
    //  timestamp across partitions.
    let min = cp_info
        .partition_metadata
        .iter()
        .map(|cons_info| cons_info.ts)
        .min()
        .expect("There should never be 0 partitions!");
    // Downgrade capability to new minimum open timestamp (which corresponds to min + 1).
    if changed && min > 0 {
        KAFKA_CAPABILITY
            .with_label_values(&[&dp_info.topic_name, &id.to_string()])
            .set(min.try_into().unwrap());
        cap.downgrade(&(&min + 1));
        cp_info.last_closed_ts = min;
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
