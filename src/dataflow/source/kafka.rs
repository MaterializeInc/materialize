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

use crate::server::TimestampHistories;
use dataflow_types::{ExternalSourceConnector, KafkaSourceConnector, Timestamp};
use lazy_static::lazy_static;
use log::{error, info, warn};
use prometheus::{register_int_counter, register_int_gauge_vec, IntCounter, IntGaugeVec};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::activate::SyncActivator;

use super::util::source;
use super::{SourceConfig, SourceStatus, SourceToken};
use expr::{PartitionId, SourceInstanceId};
use rdkafka::message::BorrowedMessage;
use std::iter;

lazy_static! {
    static ref BYTES_READ_COUNTER: IntCounter = register_int_counter!(
        "mz_kafka_bytes_read_total",
        "Count of kafka bytes we have read from the wire"
    )
    .unwrap();
    static ref KAFKA_PARTITION_OFFSET_INGESTED: IntGaugeVec = register_int_gauge_vec!(
        "mz_kafka_partition_offset_ingested",
        "The most recent kafka offset that we have ingested into a dataflow",
        &["topic", "source_id", "partition_id"]
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

pub fn kafka<G>(
    source_config: SourceConfig<G>,
    name: String,
    connector: KafkaSourceConnector,
    read_kafka: bool,
) -> (
    Stream<G, (Vec<u8>, (Vec<u8>, Option<i64>))>,
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let (id, scope, timestamp_histories, timestamp_tx, consistency) = source_config.into_parts();

    let KafkaSourceConnector {
        url,
        topic,
        config_options,
        start_offset,
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

        // Buffer place older for buffering messages for which we did not have a timestamp
        let mut buffer: Option<MessageParts> = None;
        // For each partition, the next offset to process, and the smallest still-open timestamp.
        let mut partition_metadata: Vec<(i64, u64)> = vec![(start_offset, 0)];

        if let Some(consumer) = consumer.as_mut() {
            consumer.subscribe(&[&topic]).unwrap();
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
                    &mut partition_metadata,
                    &timestamp_histories,
                    start_offset,
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
                    if partition_metadata.len() <= partition as usize {
                        // We have received a message for a partition that the timestamping logic
                        // doesn't know about yet. Buffer it and wait for the appropriate information.
                        buffer = Some(message);
                        activator.activate();
                        return SourceStatus::Alive;
                    }

                    // Determine what the last processed message for this stream partition
                    let next_offset = partition_metadata[partition as usize].0;

                    if offset <= next_offset {
                        warn!(
                            "Kafka message before expected offset: \
                             source {} (reading topic {}, partition {}) \
                             received offset {} expected offset {}",
                            name, topic, partition, offset, next_offset
                        );
                        let res = consumer.seek(
                            &topic,
                            partition,
                            Offset::Offset(next_offset),
                            Duration::from_secs(1),
                        );
                        match res {
                            Ok(_) => warn!(
                                "Fast-forwarding consumer on partition {} to offset {}",
                                partition, next_offset
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
                            partition_metadata[partition as usize].0 = offset;
                            bytes_read += key.len() as i64;
                            bytes_read += out.len() as i64;
                            output.session(&cap).give((key, (out, Some(offset - 1))));

                            let id_str = id.to_string();
                            KAFKA_PARTITION_OFFSET_INGESTED
                                .with_label_values(&[&topic, &id_str, &partition.to_string()])
                                .set(offset);

                            downgrade_capability(
                                &id,
                                cap,
                                &mut partition_metadata,
                                &timestamp_histories,
                                start_offset,
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
/// (partition_count, timestamp, offset) tuples. A message with offset x will be assigned the first timestamp
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
#[allow(clippy::too_many_arguments)]
fn downgrade_capability(
    id: &SourceInstanceId,
    cap: &mut Capability<Timestamp>,
    partition_metadata: &mut Vec<(i64, u64)>,
    timestamp_histories: &TimestampHistories,
    start_offset: i64,
) {
    let mut changed = false;

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
            let last_offset = partition_metadata[pid as usize].0;
            // Check whether timestamps can be closed on this partition
            while let Some((partition_count, ts, offset)) = entries.first() {
                assert!(
                    *offset >= start_offset,
                    "Internal error! Timestamping offset went below start: {} < {}. Materialize will now crash.",
                    offset, start_offset
                );
                if *partition_count as usize > partition_metadata.len() {
                    // A new partition has been added, we need to update the appropriate
                    // entries before we continue.
                    partition_metadata.extend(
                        iter::repeat((start_offset, 0))
                            .take(*partition_count as usize - partition_metadata.len()),
                    );
                }
                if last_offset == *offset {
                    // We have now seen all messages corresponding to this timestamp for this
                    // partition. We
                    // can close the timestamp (on this partition) and remove the associated metadata
                    partition_metadata[pid as usize].1 = *ts;
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
        .map(|(_, ts)| *ts)
        .min()
        .expect("There should never be 0 partitions!");
    // Downgrade capability to new minimum open timestamp (which corresponds to min + 1).
    if changed && min > 0 {
        cap.downgrade(&(&min + 1));
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
