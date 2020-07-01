// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::iter::Iterator;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::operators::arrange::ShutdownButton;
use lazy_static::lazy_static;
use log::error;
use prometheus::{
    register_int_counter_vec, register_uint_gauge_vec, IntCounter, IntCounterVec, UIntGauge,
    UIntGaugeVec,
};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::{Scope, Stream};
use timely::progress::frontier::MutableAntichain;

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use interchange::avro::{DiffPair, Encoder};
use repr::{RelationDesc, Row};

use super::util::sink_reschedule;

/// Per-Kafka sink metrics.
#[derive(Clone)]
pub struct SinkMetrics {
    messages_sent_counter: IntCounter,
    message_send_errors_counter: IntCounter,
    message_delivery_errors_counter: IntCounter,
    rows_queued: UIntGauge,
    messages_in_flight: UIntGauge,
}

impl SinkMetrics {
    fn new(topic_name: &str, sink_id: &str, worker_id: &str) -> SinkMetrics {
        lazy_static! {
            static ref MESSAGES_SENT_COUNTER: IntCounterVec = register_int_counter_vec!(
                "mz_kafka_messages_sent_total",
                "The number of messages the Kafka producer successfully sent for this sink",
                &["topic", "sink_id", "worker_id"]
            )
            .unwrap();
            static ref MESSAGE_SEND_ERRORS_COUNTER: IntCounterVec = register_int_counter_vec!(
                "mz_kafka_message_send_errors_total",
                "The number of times the Kafka producer encountered an error on send",
                &["topic", "sink_id", "worker_id"]
            )
            .unwrap();
            static ref MESSAGE_DELIVERY_ERRORS_COUNTER: IntCounterVec = register_int_counter_vec!(
                "mz_kafka_message_delivery_errors_total",
                "The number of messages that the Kafka producer could not deliver to the topic",
                &["topic", "sink_id", "worker_id"]
            )
            .unwrap();
            static ref ROWS_QUEUED: UIntGaugeVec = register_uint_gauge_vec!(
                "mz_kafka_sink_rows_queued",
                "The current number of rows queued by the Kafka sink operator (note that one row can generate multiple Kafka messages)",
                &["topic", "sink_id", "worker_id"]
            )
            .unwrap();
            static ref MESSAGES_IN_FLIGHT: UIntGaugeVec = register_uint_gauge_vec!(
                "mz_kafka_sink_messages_in_flight",
                "The current number of messages waiting to be delivered by the Kafka producer",
                &["topic", "sink_id", "worker_id"]
            )
            .unwrap();
        }
        let labels = &[topic_name, sink_id, worker_id];
        SinkMetrics {
            messages_sent_counter: MESSAGES_SENT_COUNTER.with_label_values(labels),
            message_send_errors_counter: MESSAGE_SEND_ERRORS_COUNTER.with_label_values(labels),
            message_delivery_errors_counter: MESSAGE_DELIVERY_ERRORS_COUNTER
                .with_label_values(labels),
            rows_queued: ROWS_QUEUED.with_label_values(labels),
            messages_in_flight: MESSAGES_IN_FLIGHT.with_label_values(labels),
        }
    }
}

#[derive(Clone)]
pub struct SinkProducerContext {
    metrics: SinkMetrics,
    shutdown: Arc<AtomicBool>,
}

impl SinkProducerContext {
    pub fn new(metrics: &SinkMetrics, shutdown: &Arc<AtomicBool>) -> Self {
        SinkProducerContext {
            metrics: metrics.clone(),
            shutdown: shutdown.clone(),
        }
    }
}

impl ClientContext for SinkProducerContext {}
impl ProducerContext for SinkProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult, _: Self::DeliveryOpaque) {
        match result {
            Ok(_) => (),
            Err((e, msg)) => {
                self.metrics.message_delivery_errors_counter.inc();
                error!(
                    "received error while writing to kafka sink topic {}: {}",
                    msg.topic(),
                    e
                );
                self.shutdown.store(true, Ordering::SeqCst);
            }
        }
    }
}

#[derive(Debug)]
pub enum SinkConsistencyState {
    Begin,
    End,
}

#[derive(Debug)]
pub struct SinkConsistencyInfo {
    // Name of the consistency topic
    topic: String,
    // Schema id for the consistency topic
    // Note that multiple consistency topics with the same schema will have different
    // schema ids
    schema_id: i32,
    // The total number of data messages that will be generated for each timestamp
    timestamp_counts: BTreeMap<Timestamp, i64>,
    // Pending consistency messages that need to be sent to the consistency topic
    queue: VecDeque<(SinkConsistencyState, Timestamp, Option<i64>)>,
}

impl SinkConsistencyInfo {
    pub fn new(topic: String, schema_id: i32) -> Self {
        SinkConsistencyInfo {
            topic,
            schema_id,
            timestamp_counts: BTreeMap::new(),
            queue: VecDeque::new(),
        }
    }

    // Updates the number of messages associated with `time` by `count`, and returns
    // true if this was the first time we observed time.
    pub fn update_timestamp_count(&mut self, time: Timestamp, count: i64) -> bool {
        if let Some(c) = self.timestamp_counts.get_mut(&time) {
            *c += count;
            return false;
        } else {
            self.timestamp_counts.insert(time, count);
        }

        true
    }

    pub fn get_complete_timestamps(
        &mut self,
        frontier: &MutableAntichain<Timestamp>,
    ) -> Vec<(Timestamp, i64)> {
        // A timestamp is closed if there is no element in the frontier that is
        // less than or equal to that timestamp.
        let closed_timestamps: Vec<(Timestamp, i64)> = self
            .timestamp_counts
            .iter()
            .filter(|(k, _)| !frontier.less_equal(k))
            .map(|(&k, &v)| (k, v))
            .collect();

        for (time, _) in closed_timestamps.iter() {
            self.timestamp_counts.remove(time);
        }

        closed_timestamps
    }
}

// TODO@jldlaughlin: What guarantees does this sink support? #1728
pub fn kafka<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: KafkaSinkConnector,
    desc: RelationDesc,
) -> ShutdownButton<ThreadedProducer<SinkProducerContext>>
where
    G: Scope<Timestamp = Timestamp>,
{
    // We want exactly one worker to send all the data to the sink topic. We
    // achieve that by using an Exchange channel before the sink and mapping
    // all records for the sink to the sink's hash, which has the neat property
    // of also distributing sinks amongst workers
    let sink_hash = id.hashed();

    let encoder = Encoder::new(desc);
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &connector.url.to_string());

    // Increase limits for the Kafka producer's internal buffering of messages
    // Currently we don't have a great backpressure mechanism to tell indexes or
    // views to slow down, so the only thing we can do with a message that we
    // can't immediately send is to put it in a buffer and there's no point
    // having buffers within the dataflow layer and Kafka
    // If the sink starts falling behind and the buffers start consuming
    // too much memory the best thing to do is to drop the sink
    // Sets the buffer size to be 16 GB (note that this setting is in KB)
    config.set("queue.buffering.max.kbytes", &format!("{}", 16 << 20));

    // Set the max messages buffered by the producer at any time to 10MM which
    // is the maximum allowed value
    config.set("queue.buffering.max.messages", &format!("{}", 10_000_000));

    // Make the Kafka producer wait at least 10 ms before sending out MessageSets
    // TODO(rkhaitan): experiment with different settings for this value to see
    // if it makes a big difference
    config.set("queue.buffering.max.ms", &format!("{}", 10));
    let sink_metrics = SinkMetrics::new(
        &connector.topic,
        &id.to_string(),
        &stream.scope().index().to_string(),
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let producer = Rc::new(RefCell::new(Some(
        config
            .create_with_context::<_, ThreadedProducer<_>>(SinkProducerContext::new(
                &sink_metrics,
                &shutdown,
            ))
            .expect("creating kafka producer for kafka sinks failed"),
    )));
    let mut queue: VecDeque<(Row, Timestamp, Diff)> = VecDeque::new();
    let mut vector = Vec::new();
    let mut encoded_buffer = None;

    let mut consistency = if let Some(consistency) = &connector.consistency {
        Some(SinkConsistencyInfo::new(
            consistency.topic.clone(),
            consistency.schema_id,
        ))
    } else {
        None
    };

    let name = format!("kafka-{}", id);
    sink_reschedule(
        &stream,
        Exchange::new(move |_| sink_hash),
        name.clone(),
        |info| {
            // Setup activator and shutdown buttons for this operator
            let activator = stream.scope().activator_for(&info.address[..]);
            let shutdown_button = ShutdownButton::new(
                producer.clone(),
                stream.scope().activator_for(&info.address[..]),
            );

            let ret = move |input: &mut FrontieredInputHandle<_, (Row, Timestamp, Diff), _>| {
                if shutdown.load(Ordering::SeqCst) {
                    error!(
                        "encountered irrecoverable error. shutting down sink: {}",
                        name
                    );
                    *producer.borrow_mut() = None;
                    return false;
                }

                let producer = &*producer.borrow();

                let producer = match producer {
                    Some(producer) => producer,
                    None => return false,
                };

                // Grab all of the available Rows and put them in a queue before we
                // send it over to Kafka. Even though we want to do bounded work
                // per sink invocation, we still need to remember all inputs as we
                // receive them.
                input.for_each(|_, rows| {
                    rows.swap(&mut vector);

                    for (row, time, diff) in vector.drain(..) {
                        let should_emit = if connector.strict {
                            connector.frontier.less_than(&time)
                        } else {
                            connector.frontier.less_equal(&time)
                        };

                        if !should_emit {
                            continue;
                        }

                        queue.push_back((row, time, diff));
                        if let Some(consistency) = &mut consistency {
                            // Note that since a single differential message
                            // turns into |diff| messages we need to increment
                            // message counts by |diff| instead of just 1 for
                            // the consistency topic
                            let insert =
                                consistency.update_timestamp_count(time, diff.abs() as i64);

                            if insert {
                                // Send a BEGIN message for a timestamp the first
                                // time we encounter it
                                consistency.queue.push_back((
                                    SinkConsistencyState::Begin,
                                    time,
                                    None,
                                ));
                            }
                        }
                    }
                });

                if let Some(consistency) = &mut consistency {
                    // Find the timestamps that are now complete (meaning all
                    // timestamps t !<= input_frontier. For each closed timestamp
                    // send a END message in the consistency topic
                    consistency
                        .get_complete_timestamps(input.frontier())
                        .iter()
                        .for_each(|(k, v)| {
                            consistency
                                .queue
                                .push_back((SinkConsistencyState::End, *k, Some(*v)));
                        });

                    // Send a bounded number of queued consistency messages to
                    // the consistency topic
                    for _ in 0..connector.fuel {
                        let (encoded, state, time, count) =
                            if let Some((state, time, count)) = consistency.queue.pop_front() {
                                let state_str = match state {
                                    SinkConsistencyState::Begin => "BEGIN",
                                    SinkConsistencyState::End => "END",
                                };

                                let transaction_id = time.to_string();
                                (
                                    encoder.encode_transaction_unchecked(
                                        consistency.schema_id,
                                        &transaction_id,
                                        state_str,
                                        count,
                                    ),
                                    state,
                                    time,
                                    count,
                                )
                            } else {
                                // Nothing more to do here
                                break;
                            };

                        let record =
                            BaseRecord::<&Vec<u8>, _>::to(&consistency.topic).payload(&encoded);
                        if let Err((e, _)) = producer.send(record) {
                            error!("unable to produce consistency message in {}: {}", name, e);

                            if let KafkaError::MessageProduction(RDKafkaError::QueueFull) = e {
                                // Repopulate the queue with the data we just took
                                // out so we can retry later
                                consistency.queue.push_front((state, time, count));
                                activator.activate_after(Duration::from_secs(60));
                                return true;
                            } else {
                                // We've received an error that is not transient
                                shutdown.store(true, Ordering::SeqCst);
                                return false;
                            }
                        }
                    }
                }

                // Send a bounded number of records to Kafka from the queue. This
                // loop has explicitly been designed so that each iteration sends
                // at most one record to Kafka
                for _ in 0..connector.fuel {
                    let (encoded, count) = if let Some((encoded, count)) = encoded_buffer.take() {
                        // We still need to send more copies of this record.
                        (encoded, count)
                    } else if let Some((row, time, diff)) = queue.pop_front() {
                        // Convert a previously queued (Row, Diff) to a Avro diff
                        // envelope record
                        if diff == 0 {
                            // Explicitly refuse to send no-op records
                            continue;
                        };

                        let time = match consistency {
                            Some(_) => Some(time.to_string()),
                            None => None,
                        };

                        let diff_pair = if diff < 0 {
                            DiffPair {
                                before: Some(&row),
                                after: None,
                            }
                        } else {
                            DiffPair {
                                before: None,
                                after: Some(&row),
                            }
                        };

                        let buf = encoder.encode_unchecked(connector.schema_id, diff_pair, time);
                        // For diffs other than +/- 1, we send repeated copies of the
                        // Avro record [diff] times. Since the format and envelope
                        // capture the "polarity" of the update, we need to remember
                        // how many times to send the data.
                        (buf, diff.abs())
                    } else {
                        // Nothing left for us to do
                        break;
                    };

                    let record = BaseRecord::<&Vec<u8>, _>::to(&connector.topic).payload(&encoded);
                    if let Err((e, _)) = producer.send(record) {
                        sink_metrics.message_send_errors_counter.inc();
                        error!("unable to produce in {}: {}", name, e);
                        if let KafkaError::MessageProduction(RDKafkaError::QueueFull) = e {
                            // We are overloading Kafka by sending too many records
                            // retry sending this record at a later time.
                            // Note that any other error will result in dropped
                            // data as we will not attempt to resend it.
                            // https://github.com/edenhill/librdkafka/blob/master/examples/producer.c#L188-L208
                            // only retries on QueueFull so we will keep that
                            // convention here.
                            encoded_buffer = Some((encoded, count));
                            activator.activate_after(Duration::from_secs(60));
                            return true;
                        } else {
                            // We've received an error that is not transient
                            shutdown.store(true, Ordering::SeqCst);
                            return false;
                        }
                    } else {
                        sink_metrics.messages_sent_counter.inc();
                    }

                    // Cache the Avro encoded data if we need to send again and
                    // remember how many more times we need to send it
                    if count > 1 {
                        encoded_buffer = Some((encoded, count - 1));
                    }
                }

                let in_flight = producer.in_flight_count();

                sink_metrics.rows_queued.set(queue.len() as u64);
                sink_metrics.messages_in_flight.set(in_flight as u64);
                if encoded_buffer.is_some() || !queue.is_empty() {
                    // We need timely to reschedule this operator as we have pending
                    // items that we need to send to Kafka
                    activator.activate();
                    return true;
                }

                if let Some(consistency) = &consistency {
                    if !consistency.queue.is_empty() {
                        // We still have pending consistency messages to send to
                        // Kafka and need to reschedule this operator
                        activator.activate();
                        return true;
                    }
                }

                if in_flight > 0 {
                    // We still have messages that need to be flushed out to Kafka
                    // Let's make sure to keep the sink operator around until
                    // we flush them out
                    activator.activate_after(Duration::from_secs(5));
                    return true;
                }

                false
            };

            (ret, shutdown_button)
        },
    )
}
