// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, VecDeque};
use std::convert::From;
use std::convert::Into;
use std::convert::TryInto;
use std::iter;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lazy_static::lazy_static;
use log::{error, info, warn};
use prometheus::{register_int_counter, register_int_gauge_vec, IntCounter, IntGaugeVec};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::{Offset, TopicPartitionListElem};
use rdkafka::{ClientConfig, ClientContext, Message, Statistics};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::activate::{Activator, SyncActivator};
use url::Url;

use dataflow_types::{ExternalSourceConnector, KafkaSourceConnector, Timestamp};
use expr::{MzOffset, PartitionId, SourceInstanceId};

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

#[derive(Clone, Copy)]
struct KafkaOffset {
    offset: i64,
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

/// Convert from KafkaOffset to MzOffset (1-indexed)
impl From<KafkaOffset> for MzOffset {
    fn from(kafka_offset: KafkaOffset) -> Self {
        MzOffset {
            offset: kafka_offset.offset + 1,
        }
    }
}

/// Convert from MzOffset (1-indexed) to KafkaOffset (0-indexed)
impl Into<KafkaOffset> for MzOffset {
    fn into(self) -> KafkaOffset {
        KafkaOffset {
            offset: self.offset - 1,
        }
    }
}

// Refresh our metadata from Kafka, which is necessary in order to start receiving messages
// for new partitions. Return whether the check succeeded and returned expectedly many partitions.
#[must_use]
fn refresh_kafka_metadata(
    consumers: &mut VecDeque<BaseConsumer<GlueConsumerContext>>,
    expected_partitions: usize,
    topic: &str,
    activator: &Activator,
) -> bool {
    let mut result = true;

    for c in consumers {
        match c.fetch_metadata(Some(topic), Duration::from_secs(1)) {
            Ok(md) => match md.topics().iter().find(|mdt| mdt.name() == topic) {
                None => {
                    warn!("Topic {} not found in Kafka metadata", topic);
                    result = false;
                    break;
                }
                Some(mdt) => {
                    let partitions = mdt.partitions().len();
                    if partitions < expected_partitions {
                        warn!(
                            "Topic {} does not have as many partitions as expected ({} < {})",
                            topic, partitions, expected_partitions
                        );
                        result = false;
                        break;
                    }
                }
            },
            Err(e) => {
                warn!("Error refreshing Kafka metadata: {}", e);
                result = false;
                break;
            }
        }
    }

    if !result {
        warn!("Descheduling for 1s to wait for Kafka metadata to catch up");
        activator.activate_after(Duration::from_secs(1));
    } else {
        warn!("Successfully refreshed metadata for {} {}", topic, expected_partitions)
    }
    result
}

/// Create a new kafka consumer
fn create_kafka_consumer(
    activator: Arc<Mutex<SyncActivator>>,
    topic: &str,
    kafka_config: &ClientConfig,
) -> BaseConsumer<GlueConsumerContext> {
    let cx = GlueConsumerContext(activator);
    let consumer: BaseConsumer<GlueConsumerContext> = kafka_config
        .create_with_context(cx)
        .expect("Failed to create Kafka Consumer");
    consumer.subscribe(&[topic]).unwrap();
    consumer
}

/// Update the list of Kafka consumers to match the number of partitions
/// We currently create one consumer per partition
fn update_consumer_list(
    consumers: &mut VecDeque<BaseConsumer<GlueConsumerContext>>,
    topic: &str,
    to_add: usize,
    activator: Arc<Mutex<SyncActivator>>,
    kafka_config: &ClientConfig,
) {
    for _ in 0..to_add {
        consumers.push_front(create_kafka_consumer(
            activator.clone(),
            topic,
            kafka_config,
        ));
    }
}

/// Poll from the next consumer for which a message is available. This function polls the set
/// round-robin: when a consumer is polled, it is placed at the back of the queue.
fn get_next_message_from_consumers(
    name: &str,
    consumers: &mut VecDeque<BaseConsumer<GlueConsumerContext>>,
) -> Option<(MessageParts, BaseConsumer<GlueConsumerContext>)> {
    let consumer_count = consumers.len();
    let mut attempts = 0;
    while attempts < consumer_count {
        let consumer = consumers.pop_front().unwrap();
        let message = match consumer.poll(Duration::from_millis(0)) {
            Some(Ok(msg)) => Some(MessageParts::from(&msg)),
            Some(Err(err)) => {
                error!("kafka error: {}: {}", name, err);
                None
            }
            _ => None,
        };
        if let Some(message) = message {
            return Some((message, consumer));
        }
        consumers.push_back(consumer);
        attempts += 1;
    }
    None
}

/// Creates a Kafka config
fn create_kafka_config(name: &str, url: &Url, group_id_prefix: Option<String>, config_options: &HashMap<String,String>) -> ClientConfig {
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

/// Consistency information
#[derive(Copy, Clone)]
struct ConsInfo {
    pub ts: Timestamp,
    pub offset: MzOffset,
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

    let ts = if config.active {
        let prev = config
            .timestamp_histories
            .borrow_mut()
            .insert(config.id.clone(), HashMap::new());
        assert!(prev.is_none());
        config.timestamp_tx.as_ref().borrow_mut().push((
            config.id,
            Some((
                ExternalSourceConnector::Kafka(connector),
                config.consistency,
            )),
        ));
        Some(config.timestamp_tx)
    } else {
        None
    };

    let SourceConfig {
        name,
        id,
        scope,
        active,
        timestamp_histories,
        ..
    } = config;

    let (stream, capability) = source(config.id, ts, scope, &name.clone(), move |info| {

        let activator = scope.activator_for(&info.address[..]);

        let kafka_config = create_kafka_config(&name,&url, group_id_prefix, &config_options);


        let mut consumers: Option<VecDeque<BaseConsumer<GlueConsumerContext>>> =
            if active { Some(VecDeque::new()) } else { None };
        // Buffer placeholder for buffering messages for which we did not have a timestamp
        // INVARIANT: consumer is EITHER in consumers or in buffer, but cannot be in both
        // Keeping a handle
        let mut buffer: Option<(MessageParts, BaseConsumer<GlueConsumerContext>)> = None;
        // The last timestamp that was closed, or 0 if none has been.
        let mut last_closed_ts: u64 = 0;
        // For each partition, the next offset to process, and the smallest still-open timestamp.
        let mut partition_metadata: Vec<ConsInfo> = vec![ConsInfo {
            offset: MzOffset {
                offset: start_offset,
            },
            ts: 0,
        }];
        // Whether the Kafka metadata should be refreshed the next time this operator is scheduled.
        let mut needs_refresh: bool = true;
        // Activator shared across consumers
        let consumer_activator = Arc::new(Mutex::new(scope.sync_activator_for(&info.address[..])));
        // Optional: Materialize Offset from which stream should start reading
        let start_offset = MzOffset {
            offset: start_offset,
        };

        move |cap, output| {
            // Accumulate updates to BYTES_READ_COUNTER;
            let mut bytes_read = 0;
            if let Some(mut consumers) = consumers.as_mut() {
                if needs_refresh {
                    let to_create = if buffer.is_some() {
                        partition_metadata.len() - (consumers.len() + 1)
                    } else {
                        partition_metadata.len() - consumers.len()
                    };
                    update_consumer_list(
                        consumers,
                        &topic,
                        to_create,
                        consumer_activator.clone(),
                        &kafka_config,
                    );
                    // Attempt to pick up new partitions.
                    if !refresh_kafka_metadata(
                        &mut consumers,
                        partition_metadata.len(),
                        &topic,
                        &activator,
                    ) {
                        return SourceStatus::Alive;
                    }
                    needs_refresh = false;
                }
                // Repeatedly interrogate Kafka for messages. Cease when
                // Kafka stops returning new data, or after 10 milliseconds.
                let timer = std::time::Instant::now();

                // Check if the capability can be downgraded (this is independent of whether
                // there are new messages that can be processed) as timestamps can become
                // closed in the absence of messages
                if downgrade_capability(
                    &topic,
                    &id,
                    cap,
                    &mut partition_metadata,
                    &timestamp_histories,
                    start_offset,
                    &mut last_closed_ts,
                ) {
                    let to_create = if buffer.is_some() {
                        partition_metadata.len() - (consumers.len() + 1)
                    } else {
                        partition_metadata.len() - consumers.len()
                    };
                    update_consumer_list(
                        consumers,
                        &topic,
                        to_create,
                        consumer_activator.clone(),
                        &kafka_config,
                    );
                    if !refresh_kafka_metadata(
                        &mut consumers,
                        partition_metadata.len(),
                        &topic,
                        &activator,
                    ) {
                        needs_refresh = true;
                        return SourceStatus::Alive;
                    }
                }

                // Check if there was a message buffered and if we can now process it
                // If we can now process it, clear the buffer and proceed to poll from
                // consumer. Else, exit the function
                let mut next_message = if let Some(message) = buffer.take() {
                    Some(message)
                } else {
                    // No currently buffered message, poll from stream
                    get_next_message_from_consumers(&name, consumers)
                };

                while let Some((message, consumer)) = next_message {

                    assert!(buffer.is_none());

                    let partition = message.partition;
                    let offset = MzOffset::from(message.offset);

                    // Offsets are guaranteed to be 1) monotonically increasing *unless* there is
                    // a network issue or a new partition added, at which point the consumer may
                    // start processing the topic from the beginning, or we may see duplicate offsets
                    // At all times, the guarantee : if we see offset x, we have seen all offsets [0,x-1]
                    // that we are ever going to see holds.
                    // Offsets are guaranteed to be contiguous when compaction is disabled. If compaction
                    // is enabled, there may be gaps in the sequence.

                    KAFKA_PARTITION_OFFSET_RECEIVED
                        .with_label_values(&[&topic, &id.to_string(), &partition.to_string()])
                        .set(offset.offset);

                    if partition as usize >= partition_metadata.len() {
                        partition_metadata.extend(
                            iter::repeat(ConsInfo {
                                offset: start_offset,
                                ts: last_closed_ts,
                            })
                            .take(1 + partition as usize - partition_metadata.len()),
                        );
                        //  When updating the consumer list to the required number of consumers, we
                        // have to include the consumer that we currently hold
                        update_consumer_list(
                            consumers,
                            &topic,
                            partition_metadata.len() - (consumers.len() + 1),
                            consumer_activator.clone(),
                            &kafka_config,
                        );
                        if !refresh_kafka_metadata(
                            &mut consumers,
                            partition_metadata.len(),
                            &topic,
                            &activator,
                        ) {
                            needs_refresh = true;
                            buffer = Some((message, consumer));
                            return SourceStatus::Alive;
                        }
                    }

                    let last_offset = partition_metadata[partition as usize].offset;

                    if offset <= last_offset {
                        let next_offset =
                            Offset::Offset(Into::<KafkaOffset>::into(offset).offset + 1);
                        warn!(
                            "Kafka message before expected offset: \
                             source {} (reading topic {}, partition {}) \
                             received mz offset {} expected mz offset {:?}",
                            name, topic, partition, offset, last_offset.offset + 1
                        );
                        // Seek to the *next* offset (ak offset + 1) that we have not yet processed
                        // This code assumes that if the offset has been GC-ed, Kafka will seek
                        // to the next non-compacted offset
                        let res =
                            consumer.seek(&topic, partition, next_offset, Duration::from_secs(1));
                        match res {
                            Ok(_) => warn!(
                                "Fast-forwarded consumer on partition {} to Kafka offset {:?}",
                                partition, next_offset
                            ),
                            Err(e) => error!("Failed to fast-forward consumer: {}", e),
                        }
                        // Message has been discarded, we return the consumer to the consumer queue
                        consumers.push_back(consumer);
                        activator.activate();
                        return SourceStatus::Alive;
                    }

                    // Determine the timestamp to which we need to assign this message
                    let ts = find_matching_timestamp(&id, partition, offset, &timestamp_histories);
                    match ts {
                        None => {
                            // We have not yet decided on a timestamp for this message,
                            // we need to buffer the message
                            buffer = Some((message, consumer));
                            assert_eq!(consumers.len()+1, partition_metadata.len());
                            activator.activate();
                            return SourceStatus::Alive;
                        }
                        Some(ts) => {
                            // Note: empty and null payload/keys are currently
                            // treated as the same thing.
                            let key = message.key.unwrap_or_default();
                            let out = message.payload.unwrap_or_default();
                            partition_metadata[partition as usize].offset = offset;
                            bytes_read += key.len() as i64;
                            bytes_read += out.len() as i64;
                            let ts_cap = cap.delayed(&ts);
                            output
                                .session(&ts_cap)
                                .give((key, (out, Some(Into::<KafkaOffset>::into(offset).offset))));
                            // Message processed, return the consumer
                            consumers.push_back(consumer);

                            assert_eq!(consumers.len(), partition_metadata.len());

                            KAFKA_PARTITION_OFFSET_INGESTED
                                .with_label_values(&[
                                    &topic,
                                    &id.to_string(),
                                    &partition.to_string(),
                                ])
                                .set(offset.offset);

                            if downgrade_capability(
                                &topic,
                                &id,
                                cap,
                                &mut partition_metadata,
                                &timestamp_histories,
                                start_offset,
                                &mut last_closed_ts,
                            ) {
                                // We have just returned the consumer to the consumer queue, so no
                                // need for maths
                                update_consumer_list(
                                    consumers,
                                    &topic,
                                    partition_metadata.len() - consumers.len(),
                                    consumer_activator.clone(),
                                    &kafka_config,
                                );
                                if !refresh_kafka_metadata(
                                    &mut consumers,
                                    partition_metadata.len(),
                                    &topic,
                                    &activator,
                                ) {
                                    needs_refresh = true;
                                    return SourceStatus::Alive;
                                }
                            }
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

                    // Try and poll for next message
                    next_message = get_next_message_from_consumers(&name, consumers);
                }
            }
            // Ensure that we poll kafka more often than the eviction timeout
            activator.activate_after(Duration::from_secs(60));
            if bytes_read > 0 {
                BYTES_READ_COUNTER.inc_by(bytes_read);
            }
            SourceStatus::Alive
        }
    });

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
/// partitions
///
/// Returns true if we learned about a new partition and Kafka metadata needs to be refreshed.
#[allow(clippy::too_many_arguments)]
#[must_use]
fn downgrade_capability(
    topic: &str,
    id: &SourceInstanceId,
    cap: &mut Capability<Timestamp>,
    partition_metadata: &mut Vec<ConsInfo>,
    timestamp_histories: &TimestampHistories,
    start_offset: MzOffset,
    last_closed_ts: &mut u64,
) -> bool {
    let mut changed = false;
    let mut needs_md_refresh = false;

    // Determine which timestamps have been closed. A timestamp is closed once we have processed
    // all messages that we are going to process for this timestamp across all partitions
    // In practice, the following happens:
    // Per partition, we iterate over the data structure to remove (ts,offset) mappings for which
    // we have seen all records <= offset. We keep track of the last "closed" timestamp in that partition
    // in next_partition_ts
    if let Some(entries) = timestamp_histories.borrow_mut().get_mut(id) {
        for (pid, entries) in entries {
            let pid = match pid {
                PartitionId::Kafka(pid) => *pid,
                _ => unreachable!(
                    "timestamp.rs should never send messages with non-Kafka partitions \
                                   to Kafka sources."
                ),
            };
            if pid as usize >= partition_metadata.len() {
                partition_metadata.extend(
                    iter::repeat(ConsInfo {
                        offset: start_offset,
                        ts: *last_closed_ts,
                    })
                    .take(1 + pid as usize - partition_metadata.len()),
                );
                needs_md_refresh = true;
            }
            let last_offset = partition_metadata[pid as usize].offset;

            // Check whether timestamps can be closed on this partition
            while let Some((partition_count, ts, offset)) = entries.first() {
                assert!(
                    *offset >= start_offset,
                    "Internal error! Timestamping offset went below start: {} < {}. Materialize will now crash.",
                    offset, start_offset
                );

                assert!(
                    *ts > 0,
                    "Internal error! Received a zero-timestamp. Materialize will crash now."
                );

                if *partition_count as usize > partition_metadata.len() {
                    // A new partition has been added, we need to update the appropriate
                    // entries before we continue.
                    partition_metadata.extend(
                        iter::repeat(ConsInfo {
                            offset: start_offset,
                            ts: *last_closed_ts,
                        })
                        .take(*partition_count as usize - partition_metadata.len()),
                    );
                    needs_md_refresh = true;
                }
                // This assertion makes sure that if we ever fast-forwarded the empty stream
                // (any message of the form "PID,TS,0"), we have correctly removed the (_,_,0) entry
                // even if data has subsequently been added to the stream
                assert!(offset.offset != 0 || last_offset.offset == 0);

                KAFKA_PARTITION_CLOSED_TS
                    .with_label_values(&[&topic, &id.to_string(), &pid.to_string()])
                    .set((partition_metadata[pid as usize].ts).try_into().unwrap());

                if last_offset >= *offset {
                    // We have now seen all messages corresponding to this timestamp for this
                    // partition. We
                    // can close the timestamp (on this partition) and remove the associated metadata
                    partition_metadata[pid as usize].ts = *ts;
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
    let min = partition_metadata
        .iter()
        .map(|cons_info| cons_info.ts)
        .min()
        .expect("There should never be 0 partitions!");
    // Downgrade capability to new minimum open timestamp (which corresponds to min + 1).
    if changed && min > 0 {
        KAFKA_CAPABILITY
            .with_label_values(&[topic, &id.to_string()])
            .set(min.try_into().unwrap());
        cap.downgrade(&(&min + 1));
        *last_closed_ts = min;
    }

    needs_md_refresh
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
