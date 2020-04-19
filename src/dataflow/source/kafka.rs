// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::sync::Mutex;
use std::time::Duration;

use crate::server::{TimestampChanges, TimestampHistories};
use dataflow_types::{Consistency, ExternalSourceConnector, KafkaSourceConnector, Timestamp};
use lazy_static::lazy_static;
use log::{error, info, warn};
use prometheus::{register_int_counter, IntCounter};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::Offset::Offset;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::activate::SyncActivator;

use super::util::source;
use super::{SourceStatus, SourceToken};
use expr::{PartitionId, SourceInstanceId};
use itertools::Itertools;
use rdkafka::message::BorrowedMessage;

lazy_static! {
    static ref BYTES_READ_COUNTER: IntCounter = register_int_counter!(
        "mz_kafka_bytes_read_total",
        "Count of kafka bytes we have read from the wire"
    )
    .unwrap();
}

// There is other stuff in librdkafka messages, e.g. headers.
// But this struct only contains what we actually use, to avoid unnecessary cloning.
struct MessageParts {
    payload: Option<Vec<u8>>,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
}

impl<'a> From<&BorrowedMessage<'a>> for MessageParts {
    fn from(msg: &BorrowedMessage<'a>) -> Self {
        Self {
            payload: msg.payload().map(|p| p.to_vec()),
            partition: msg.partition(),
            offset: msg.offset(),
            key: msg.key().map(|k| k.to_vec()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn kafka<G>(
    scope: &G,
    name: String,
    connector: KafkaSourceConnector,
    id: SourceInstanceId,
    timestamp_histories: TimestampHistories,
    timestamp_tx: TimestampChanges,
    consistency: Consistency,
    read_kafka: bool,
) -> (
    Stream<G, ((Vec<u8>, Vec<u8>), Option<i64>)>,
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let KafkaSourceConnector {
        url,
        topic,
        config_options,
    } = connector.clone();

    let ts = if read_kafka {
        let prev = timestamp_histories
            .borrow_mut()
            .insert(id.clone(), HashMap::new());
        assert!(prev.is_none());
        timestamp_tx.as_ref().borrow_mut().push((
            id,
            Some((ExternalSourceConnector::Kafka(connector), consistency)),
        ));
        Some(timestamp_tx)
    } else {
        None
    };

    let (stream, capability) = source(id, ts, scope, &name.clone(), move |info| {
        let activator = scope.activator_for(&info.address[..]);

        let mut config = ClientConfig::new();
        config
            .set("group.id", &format!("materialize-{}", name))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &url.to_string());
        for (k, v) in &config_options {
            config.set(k, v);
        }

        let mut consumer: Option<BaseConsumer<GlueConsumerContext>> = if read_kafka {
            let cx = GlueConsumerContext(Mutex::new(scope.sync_activator_for(&info.address[..])));
            Some(
                config
                    .create_with_context(cx)
                    .expect("Failed to create Kafka Consumer"),
            )
        } else {
            None
        };

        // The next smallest timestamp that isn't closed
        let mut last_closed_ts: u64 = 0;
        // Buffer place older for buffering messages for which we did not have a timestamp
        let mut buffer: Option<MessageParts> = None;
        // Index of the last offset that we have already processed for each partition
        let mut last_processed_offsets: HashMap<PartitionId, i64> = HashMap::new();
        // Records closed timestamps for each partition. It corresponds to smallest timestamp
        // that is still open for this partition
        let mut next_partition_ts: HashMap<PartitionId, u64> = HashMap::new();
        // The list of partitions for this topic
        let mut partitions: HashSet<i32> = HashSet::new();
        // The current number of partitions that we expect for this topic initially 1)
        let mut expected_partition_count = 1;

        if let Some(consumer) = consumer.as_mut() {
            consumer.subscribe(&[&topic]).unwrap();
            // Obtain initial information about partitions
            partitions = update_partition_list(
                consumer,
                &topic,
                expected_partition_count,
                &mut last_processed_offsets,
                &mut next_partition_ts,
                last_closed_ts,
            );
            expected_partition_count = i32::try_from(partitions.len()).unwrap();
        }

        move |cap, output| {
            // Accumulate updates to BYTES_READ_COUNTER;
            let mut bytes_read = 0;
            if let Some(consumer) = consumer.as_mut() {
                // Repeatedly interrogate Kafka for messages. Cease when
                // Kafka stops returning new data, or after 10 milliseconds.
                let timer = std::time::Instant::now();

                // Check if the capability can be downgraded (this is independent of whether
                // there are new messages that can be processed) as timestamps can become
                // closed in the absence of messages
                downgrade_capability(
                    &id,
                    cap,
                    &mut last_processed_offsets,
                    &mut next_partition_ts,
                    &timestamp_histories,
                    &mut expected_partition_count,
                    consumer,
                    &topic,
                    &mut last_closed_ts,
                );

                // Check if there was a message buffered and if we can now process it
                // If we can now process it, clear the buffer and proceed to poll from
                // consumer. Else, exit the function
                let mut next_message = if let Some(message) = buffer.take() {
                    Some(message)
                } else {
                    // No currently buffered message, poll from stream
                    match consumer.poll(Duration::from_millis(0)) {
                        Some(Ok(msg)) => Some(MessageParts::from(&msg)),
                        Some(Err(err)) => {
                            error!("kafka error: {}: {}", name, err);
                            None
                        }
                        _ => None,
                    }
                };

                while let Some(message) = next_message {
                    let partition = message.partition;
                    let offset = message.offset + 1;
                    if !partitions.contains(&partition) {
                        // We have received a message for a partition for which we do not yet
                        // have any metadata. Buffer the message and wait until we get the
                        // necessary information
                        partitions = update_partition_list(
                            consumer,
                            &topic,
                            expected_partition_count,
                            &mut last_processed_offsets,
                            &mut next_partition_ts,
                            last_closed_ts,
                        );
                        expected_partition_count = i32::try_from(partitions.len()).unwrap();
                        buffer = Some(message);
                        activator.activate();
                        return SourceStatus::Alive;
                    }

                    // Determine what the last processed message for this stream partition
                    let last_processed_offset =
                        match last_processed_offsets.get(&PartitionId::Kafka(partition)) {
                            Some(offset) => *offset,
                            None => 0,
                        };

                    if offset <= last_processed_offset {
                        warn!("duplicate Kakfa message: souce {} (reading topic {}, partition {}) received offset {} max processed offset {}", name, topic, partition, offset, last_processed_offset);
                        let res = consumer.seek(
                            &topic,
                            partition,
                            Offset(last_processed_offset),
                            Duration::from_secs(1),
                        );
                        match res {
                            Ok(_) => warn!(
                                "Fast-forwarding consumer on partition {} to offset {}",
                                partition, last_processed_offset
                            ),
                            Err(e) => error!("Failed to fast-forward consumer: {}", e),
                        };
                        activator.activate();
                        return SourceStatus::Alive;
                    }

                    // Determine the timestamp to which we need to assign this message
                    let ts = find_matching_timestamp(&id, partition, offset, &timestamp_histories);
                    match ts {
                        None => {
                            // We have not yet decided on a timestamp for this message,
                            // we need to buffer the message
                            buffer = Some(message);
                            activator.activate();
                            return SourceStatus::Alive;
                        }
                        Some(_) => {
                            let key = message.key.unwrap_or_default();
                            let out = message.payload.unwrap_or_default();
                            last_processed_offsets.insert(PartitionId::Kafka(partition), offset);
                            bytes_read += key.len() as i64;
                            bytes_read += out.len() as i64;
                            output
                                .session(&cap)
                                .give(((key, out), Some(message.offset)));

                            downgrade_capability(
                                &id,
                                cap,
                                &mut last_processed_offsets,
                                &mut next_partition_ts,
                                &timestamp_histories,
                                &mut expected_partition_count,
                                consumer,
                                &topic,
                                &mut last_closed_ts,
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

                    // Try and poll for next message
                    next_message = match consumer.poll(Duration::from_millis(0)) {
                        Some(Ok(msg)) => Some(MessageParts::from(&msg)),
                        Some(Err(err)) => {
                            error!("kafka error: {}: {}", name, err);
                            None
                        }
                        _ => None,
                    };
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

    if read_kafka {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}

/// For a given offset, returns an option type returning the matching timestamp or None
/// if no timestamp can be assigned. The timestamp history contains a sequence of
/// (timestamp, offset) tuples. A message with offset x will be assigned the first timestamp
/// for which offset>=x.
fn find_matching_timestamp(
    id: &SourceInstanceId,
    partition: i32,
    offset: i64,
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

/// This function updates the list of partitions for a given topic
/// and updates the appropriate metadata
fn update_partition_list(
    consumer: &BaseConsumer<GlueConsumerContext>,
    topic: &str,
    expected_partition_count: i32,
    last_processed_offsets: &mut HashMap<PartitionId, i64>,
    next_partition_ts: &mut HashMap<PartitionId, u64>,
    closed_ts: u64,
) -> HashSet<i32> {
    let mut partitions = HashSet::new();

    while i32::try_from(partitions.len()).unwrap() < expected_partition_count {
        let result = consumer.fetch_metadata(Some(&topic), Duration::from_secs(1));
        partitions = match &result {
            Ok(meta) => match meta.topics().iter().find(|t| t.name() == topic) {
                Some(topic) => {
                    HashSet::from_iter(topic.partitions().iter().map(|x| x.id()).collect_vec())
                }
                None => HashSet::new(),
            },
            Err(e) => {
                error!("Failed to obtain partition information: {} {}", topic, e);
                HashSet::new()
            }
        };
    }

    for p in &partitions {
        // When a new timestamp is added, it will necessarily have a timestamp that is
        // greater than the last closed timestamp. We therefore set the
        let partition_id = PartitionId::Kafka(*p);
        if next_partition_ts.get(&partition_id).is_none() {
            next_partition_ts.insert(partition_id.clone(), closed_ts);
        }
        // The last processed offset is always 0
        if last_processed_offsets.get(&partition_id).is_none() {
            last_processed_offsets.insert(partition_id.clone(), 0);
        }
    }
    partitions
}

/// Timestamp history map is of format [pid1: (ts1, offset1), (ts2, offset2), pid2: (ts1, offset)...].
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
#[allow(clippy::too_many_arguments)]
fn downgrade_capability(
    id: &SourceInstanceId,
    cap: &mut Capability<Timestamp>,
    last_processed_offset: &mut HashMap<PartitionId, i64>,
    next_partition_ts: &mut HashMap<PartitionId, u64>,
    timestamp_histories: &TimestampHistories,
    current_partition_count: &mut i32,
    consumer: &BaseConsumer<GlueConsumerContext>,
    topic: &str,
    last_closed_ts: &mut u64,
) {
    let mut changed = false;
    let mut min = std::u64::MAX;

    // Determine which timestamps have been closed. A timestamp is closed once we have processed
    // all messages that we are going to process for this timestamp across all partitions
    // In practice, the following happens:
    // Per partition, we iterate over the datastructure to remove (ts,offset) mappings for which
    // we have seen all records <= offset. We keep track of the last "closed" timestamp in that partition
    // in next_partition_ts
    match timestamp_histories.borrow_mut().get_mut(id) {
        None => {}
        Some(entries) => {
            for (pid, entries) in entries {
                // Obtain the last offset processed (or -1 if no messages have yet been processed)
                let last_offset = match last_processed_offset.get(pid) {
                    Some(offs) => *offs,
                    None => 0,
                };
                // Check whether timestamps can be closed on this partition
                while let Some((partition_count, ts, offset)) = entries.first() {
                    if partition_count > current_partition_count {
                        // A new partition has been added, we need to update the appropriate
                        // entries before we continue. This will also update the last_processed_offset
                        // and next_partition_ts datastructures
                        let partitions = update_partition_list(
                            consumer,
                            topic,
                            *partition_count,
                            last_processed_offset,
                            next_partition_ts,
                            *last_closed_ts,
                        );
                        *current_partition_count = i32::try_from(partitions.len()).unwrap();
                    }
                    if last_offset == *offset {
                        // We have now seen all messages corresponding to this timestamp for this
                        // partition. We
                        // can close the timestamp (on this partition) and remove the associated metadata
                        next_partition_ts.insert(pid.clone(), *ts);
                        entries.remove(0);
                        changed = true;
                    } else {
                        // Offset isn't at a timestamp boundary, we take no action
                        break;
                    }
                }
            }
        }
    }
    //  Next, we determine the maximum timestamp that is fully closed. This corresponds to the minimum
    //  timestamp across partitions. This value is stored in next_partition_ts
    for next_ts in next_partition_ts.values() {
        if *next_ts < min {
            min = *next_ts
        }
    }
    // Downgrade capability to new minimum open timestamp (which corresponds to min + 1).
    if changed && min > 0 {
        cap.downgrade(&(&min + 1));
        *last_closed_ts = min;
    }
}

/// An implementation of [`ConsumerContext`] that unparks the wrapped thread
/// when the message queue switches from nonempty to empty.
struct GlueConsumerContext(Mutex<SyncActivator>);

impl ClientContext for GlueConsumerContext {
    fn stats(&self, statistics: Statistics) {
        info!("Client stats: {:#?}", statistics);
    }
}

impl ConsumerContext for GlueConsumerContext {
    fn message_queue_nonempty_callback(&self) {
        let activator = self.0.lock().unwrap();
        activator
            .activate()
            .expect("timely operator hung up while Kafka source active");
    }
}
