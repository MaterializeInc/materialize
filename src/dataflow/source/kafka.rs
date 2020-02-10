// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;
use std::time::Duration;

use crate::server::{TimestampChanges, TimestampHistories};
use dataflow_types::{Consistency, KafkaSourceConnector, Timestamp};
use lazy_static::lazy_static;
use log::{error, warn};
use prometheus::{register_int_counter, IntCounter};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::{ClientConfig, ClientContext};
use rdkafka::{Message, Timestamp as KafkaTimestamp};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::activate::SyncActivator;

use super::util::source;
use super::{SourceStatus, SourceToken};
use expr::SourceInstanceId;
use rdkafka::message::OwnedMessage;

lazy_static! {
    static ref BYTES_READ_COUNTER: IntCounter = register_int_counter!(
        "mz_kafka_bytes_read_total",
        "Count of kafka bytes we have read from the wire"
    )
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
pub fn kafka<G>(
    scope: &G,
    name: String,
    connector: KafkaSourceConnector,
    id: SourceInstanceId,
    advance_timestamp: bool,
    timestamp_histories: TimestampHistories,
    timestamp_tx: TimestampChanges,
    consistency: Consistency,
    read_kafka: bool,
) -> (Stream<G, (Vec<u8>, Option<i64>)>, Option<SourceToken>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let KafkaSourceConnector { addr, topic } = connector.clone();

    let ts = if read_kafka {
        let prev = timestamp_histories.borrow_mut().insert(id.clone(), vec![]);
        assert!(prev.is_none());
        timestamp_tx
            .as_ref()
            .borrow_mut()
            .push((id, Some((connector, consistency))));
        Some(timestamp_tx)
    } else {
        None
    };

    let (stream, capability) = source(id, ts, scope, &name.clone(), move |info| {
        let activator = scope.activator_for(&info.address[..]);

        let mut config = ClientConfig::new();
        config
            .set("auto.offset.reset", "smallest")
            .set("group.id", &format!("materialize-{}", name))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &addr.to_string());

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

        if let Some(consumer) = consumer.as_mut() {
            consumer.subscribe(&[&topic]).unwrap();
        }

        // Buffer place older for buffering messages for which we did not have a timestamp
        let mut buffer: Option<OwnedMessage> = None;
        // Index of the last offset that we have already processed
        let mut last_processed_offset: i64 = -1;

        move |cap, output| {
            if advance_timestamp {
                if let Some(consumer) = consumer.as_mut() {
                    // Repeatedly interrogate Kafka for messages. Cease when
                    // Kafka stops returning new data, or after 10 milliseconds.
                    let timer = std::time::Instant::now();

                    // Check if the capability can be downgraded (this is independent of whether
                    // there are new messages that can be processed)
                    downgrade_capability(&id, cap, last_processed_offset, &timestamp_histories);

                    // Check if there was a message buffered and if we can now process it
                    // If we can now process it, clear the buffer and proceed to poll from
                    // consumer. Else, exit the function
                    let mut next_message = if let Some(message) = buffer.take() {
                        Some(message)
                    } else {
                        // No currently buffered message, poll from stream
                        match consumer.poll(Duration::from_millis(0)) {
                            Some(Ok(msg)) => Some(msg.detach()),
                            Some(Err(err)) => {
                                error!("kafka error: {}: {}", name, err);
                                None
                            }
                            _ => None,
                        }
                    };

                    while let Some(message) = next_message {
                        let payload = message.payload();

                        let offset = message.offset();
                        let ts = find_matching_timestamp(&id, offset, &timestamp_histories);

                        if offset <= last_processed_offset {
                            error!("duplicate Kakfa message received");
                            activator.activate();
                            return SourceStatus::Alive;
                        }

                        match ts {
                            None => {
                                // We have not yet decided on a timestamp for this message,
                                // we need to buffer the message
                                buffer = Some(message);
                                activator.activate();
                                return SourceStatus::Alive;
                            }
                            Some(_) => {
                                last_processed_offset = offset;

                                if let Some(payload) = payload {
                                    let out = payload.to_vec();
                                    BYTES_READ_COUNTER.inc_by(out.len() as i64);
                                    output.session(&cap).give((out, Some(message.offset())));
                                }

                                downgrade_capability(
                                    &id,
                                    cap,
                                    last_processed_offset,
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

                        // Try and poll for next message
                        next_message = match consumer.poll(Duration::from_millis(0)) {
                            Some(Ok(msg)) => Some(msg.detach()),
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
                SourceStatus::Alive
            } else {
                if let Some(consumer) = consumer.as_mut() {
                    // Repeatedly interrogate Kafka for messages. Cease when
                    // Kafka stops returning new data, or after 10 milliseconds.
                    let timer = std::time::Instant::now();

                    while let Some(result) = consumer.poll(Duration::from_millis(0)) {
                        match result {
                            Ok(message) => {
                                let payload = match message.payload() {
                                    Some(p) => p,
                                    // Null payloads are expected from Debezium.
                                    // See https://github.com/MaterializeInc/materialize/issues/439#issuecomment-534236276
                                    None => continue,
                                };

                                let ms = match message.timestamp() {
                                    KafkaTimestamp::NotAvailable => {
                                        // TODO(benesch): do we need to do something
                                        // else?
                                        error!("dropped kafka message with no timestamp");
                                        continue;
                                    }
                                    KafkaTimestamp::CreateTime(ms)
                                    | KafkaTimestamp::LogAppendTime(ms) => ms as u64,
                                };
                                let cur = *cap.time();
                                if ms >= *cap.time() {
                                    cap.downgrade(&ms)
                                } else {
                                    warn!(
                                        "{}: fast-forwarding out-of-order Kafka timestamp {}ms ({} -> {})",
                                        name,
                                        cur - ms,
                                        ms,
                                        cur,
                                    );
                                };

                                let out = payload.to_vec();
                                BYTES_READ_COUNTER.inc_by(out.len() as i64);
                                output.session(&cap).give((out, Some(message.offset())));
                            }
                            Err(err) => error!("kafka error: {}: {}", name, err),
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
                SourceStatus::Alive
            }
        }
    });

    if read_kafka {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}

/// For a given offset, returns an option type returning the matching timestamp or None
fn find_matching_timestamp(
    id: &SourceInstanceId,
    offset: i64,
    timestamp_histories: &TimestampHistories,
) -> Option<Timestamp> {
    match timestamp_histories.borrow().get(id) {
        None => None,
        Some(entries) => {
            for (ts, max_offset) in entries {
                if offset <= *max_offset {
                    return Some(ts.clone());
                }
            }
            None
        }
    }
}

/// Timestamp history map is of format [(ts1, offset1), (ts2, offset2)].
/// All messages in interval [0,offset1] get assigned ts1, all messages in interval [offset1+1,offset2]
/// get assigned ts2, etc.
/// When receive message with offset1, it is safe to downgrade the capability to the next
/// timestamp, which is either
/// 1) the timestamp associated with the next highest offset if it exists
/// 2) max(timestamp, offset1) + 1. The timestamp_history map can contain multiple timestamps for
/// the same offset. We pick the greatest one + 1
/// (the next message we generate will necessarily have timestamp timestamp + 1)
fn downgrade_capability(
    id: &SourceInstanceId,
    cap: &mut Capability<Timestamp>,
    last_processed_offset: i64,
    timestamp_histories: &TimestampHistories,
) {
    match timestamp_histories.borrow_mut().get_mut(id) {
        None => {}
        Some(entries) => {
            while let Some((ts, offset)) = entries.first() {
                let next_ts = ts + 1;
                if last_processed_offset == *offset {
                    entries.remove(0);
                    cap.downgrade(&next_ts);
                } else {
                    // Offset isn't at a timestamp boundary, we take no action
                    break;
                }
            }
        }
    }
}

/// An implementation of [`ConsumerContext`] that unparks the wrapped thread
/// when the message queue switches from nonempty to empty.
struct GlueConsumerContext(Mutex<SyncActivator>);

impl ClientContext for GlueConsumerContext {}

impl ConsumerContext for GlueConsumerContext {
    fn message_queue_nonempty_callback(&self) {
        let activator = self.0.lock().unwrap();
        activator
            .activate()
            .expect("timely operator hung up while Kafka source active");
    }
}
