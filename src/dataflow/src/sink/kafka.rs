// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::hashable::Hashable;
use lazy_static::lazy_static;
use log::error;
use prometheus::{
    register_int_counter_vec, register_uint_gauge_vec, IntCounter, IntCounterVec, UIntGauge,
    UIntGaugeVec,
};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::{Scope, Stream};

use dataflow_types::KafkaSinkConnector;
use expr::GlobalId;
use interchange::avro::{self, DiffPair, Encoder};
use repr::{Diff, RelationDesc, Row, Timestamp};

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
    shutdown_flag: Arc<AtomicBool>,
}

impl SinkProducerContext {
    pub fn new(metrics: SinkMetrics, shutdown_flag: Arc<AtomicBool>) -> Self {
        SinkProducerContext {
            metrics,
            shutdown_flag,
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
                self.shutdown_flag.store(true, Ordering::SeqCst);
            }
        }
    }
}

struct KafkaSinkToken {
    shutdown_flag: Arc<AtomicBool>,
}

impl Drop for KafkaSinkToken {
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
    }
}

struct KafkaSink {
    name: String,
    shutdown_flag: Arc<AtomicBool>,
    metrics: SinkMetrics,
    encoder: Encoder,
    producer: ThreadedProducer<SinkProducerContext>,
    activator: timely::scheduling::Activator,
}

impl KafkaSink {
    fn send(&self, record: BaseRecord<Vec<u8>, Vec<u8>>) -> Result<(), bool> {
        if let Err((e, _)) = self.producer.send(record) {
            error!("unable to produce message in {}: {}", self.name, e);
            self.metrics.message_send_errors_counter.inc();

            if let KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) = e {
                self.activator.activate_after(Duration::from_secs(60));
                Err(true)
            } else {
                // We've received an error that is not transient
                self.shutdown_flag.store(true, Ordering::SeqCst);
                Err(false)
            }
        } else {
            self.metrics.messages_sent_counter.inc();
            Ok(())
        }
    }
}

enum SendState {
    // State to write BEGIN consistency record
    Begin,
    // State to flush pending rows for closed timestamps
    Draining {
        // row_index points to the current flushing row within the closed timestamp
        // we're processing
        row_index: usize,
        // multiple copies of a row may need to be sent if its cardinality is >1
        repeat_counter: usize,
        // a count of all rows sent, accounting for the repeat counter
        total_sent: i64,
    },
    // State to write END consistency record
    End(i64),
}

#[derive(Debug)]
struct EncodedRow {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
    count: usize,
}

// TODO@jldlaughlin: What guarantees does this sink support? #1728
pub fn kafka<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    mut connector: KafkaSinkConnector,
    desc: RelationDesc,
) -> Box<dyn Any>
where
    G: Scope<Timestamp = Timestamp>,
{
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &connector.addrs.to_string());

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

    for (k, v) in connector.config_options.iter() {
        config.set(k, v);
    }

    let name = format!("kafka-{}", id);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let mut builder = OperatorBuilder::new(name.clone(), stream.scope());

    let s = {
        let metrics = SinkMetrics::new(
            &connector.topic,
            &id.to_string(),
            &stream.scope().index().to_string(),
        );

        let encoder = Encoder::new(
            desc,
            connector.consistency.is_some(),
            connector.key_indices.take(),
        );

        let producer = config
            .create_with_context::<_, ThreadedProducer<_>>(SinkProducerContext::new(
                metrics.clone(),
                shutdown_flag.clone(),
            ))
            .expect("creating kafka producer for kafka sinks failed");

        let activator = stream
            .scope()
            .activator_for(&builder.operator_info().address[..]);

        KafkaSink {
            name,
            shutdown_flag: shutdown_flag.clone(),
            metrics,
            encoder,
            producer,
            activator,
        }
    };

    let mut pending_rows: HashMap<Timestamp, Vec<EncodedRow>> = HashMap::new();
    let mut ready_rows: VecDeque<(Timestamp, Vec<EncodedRow>)> = VecDeque::new();
    let mut state = SendState::Begin;
    let mut vector = Vec::new();

    let mut sink_logic = move |input: &mut FrontieredInputHandle<_, (Row, Timestamp, Diff), _>| {
        if s.shutdown_flag.load(Ordering::SeqCst) {
            error!("shutdown requested for sink: {}", &s.name);
            return false;
        }

        // Encode and queue all pending rows waiting to be sent to kafka
        input.for_each(|_, rows| {
            rows.swap(&mut vector);
            for (row, time, diff) in vector.drain(..) {
                let should_emit = if connector.strict {
                    connector.frontier.less_than(&time)
                } else {
                    connector.frontier.less_equal(&time)
                };

                if !should_emit {
                    // Skip stale data for already published timestamps
                    continue;
                }

                if diff == 0 {
                    // Explicitly refuse to send no-op records
                    continue;
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

                let transaction_id = match connector.consistency {
                    Some(_) => Some(time.to_string()),
                    None => None,
                };

                let (key, value) = s.encoder.encode_unchecked(
                    connector.key_schema_id,
                    connector.value_schema_id,
                    diff_pair,
                    transaction_id,
                );

                // For diffs other than +/- 1, we send repeated copies of the
                // Avro record [diff] times. Since the format and envelope
                // capture the "polarity" of the update, we need to remember
                // how many times to send the data.
                let rows = pending_rows.entry(time).or_default();
                rows.push(EncodedRow {
                    key,
                    value,
                    count: diff.abs() as usize,
                });
                s.metrics.rows_queued.inc();
            }
        });

        // Move any newly closed timestamps from pending to ready
        let mut closed_ts: Vec<u64> = pending_rows
            .iter()
            .filter(|(ts, _)| !input.frontier.less_equal(*ts))
            .map(|(&ts, _)| ts)
            .collect();
        closed_ts.sort_unstable();
        closed_ts.into_iter().for_each(|ts| {
            let rows = pending_rows.remove(&ts).unwrap();
            ready_rows.push_back((ts, rows));
        });

        // Send a bounded number of records to Kafka from the ready queue.
        // This loop has explicitly been designed so that each iteration sends
        // at most one record to Kafka
        for _ in 0..connector.fuel {
            if let Some((ts, rows)) = ready_rows.front() {
                state = match state {
                    SendState::Begin => {
                        if let Some(consistency) = &connector.consistency {
                            let encoded = avro::encode_debezium_transaction_unchecked(
                                consistency.schema_id,
                                &ts.to_string(),
                                "BEGIN",
                                None,
                            );

                            let record = BaseRecord::to(&consistency.topic).payload(&encoded);
                            if let Err(retry) = s.send(record) {
                                return retry;
                            }
                        }
                        SendState::Draining {
                            row_index: 0,
                            repeat_counter: 0,
                            total_sent: 0,
                        }
                    }
                    SendState::Draining {
                        mut row_index,
                        mut repeat_counter,
                        mut total_sent,
                    } => {
                        let encoded_row = &rows[row_index];
                        let record = BaseRecord::to(&connector.topic).payload(&encoded_row.value);
                        let record = if encoded_row.key.is_some() {
                            record.key(encoded_row.key.as_ref().unwrap())
                        } else {
                            record
                        };
                        if let Err(retry) = s.send(record) {
                            return retry;
                        }

                        // advance to the next repetition of this row, or the next row if all
                        // reptitions are exhausted
                        total_sent += 1;
                        repeat_counter += 1;
                        if repeat_counter == encoded_row.count {
                            repeat_counter = 0;
                            row_index += 1;
                            s.metrics.rows_queued.dec();
                        }

                        // move to the end state if we've finished all rows in this timestamp
                        if row_index == rows.len() {
                            SendState::End(total_sent)
                        } else {
                            SendState::Draining {
                                row_index,
                                repeat_counter,
                                total_sent,
                            }
                        }
                    }
                    SendState::End(total_count) => {
                        if let Some(consistency) = &connector.consistency {
                            let encoded = avro::encode_debezium_transaction_unchecked(
                                consistency.schema_id,
                                &ts.to_string(),
                                "END",
                                Some(total_count),
                            );

                            let record = BaseRecord::to(&consistency.topic).payload(&encoded);
                            if let Err(retry) = s.send(record) {
                                return retry;
                            }
                        }
                        ready_rows.pop_front();
                        SendState::Begin
                    }
                }
            } else {
                break;
            }
        }

        let in_flight = s.producer.in_flight_count();
        s.metrics.messages_in_flight.set(in_flight as u64);

        if !ready_rows.is_empty() {
            // We need timely to reschedule this operator as we have pending
            // items that we need to send to Kafka
            s.activator.activate();
            return true;
        }

        if in_flight > 0 {
            // We still have messages that need to be flushed out to Kafka
            // Let's make sure to keep the sink operator around until
            // we flush them out
            s.activator.activate_after(Duration::from_secs(5));
            return true;
        }

        false
    };

    // We want exactly one worker to send all the data to the sink topic. We
    // achieve that by using an Exchange channel before the sink and mapping
    // all records for the sink to the sink's hash, which has the neat property
    // of also distributing sinks amongst workers
    let mut input = builder.new_input(stream, Exchange::new(move |_| id.hashed()));
    builder.build_reschedule(|_capabilities| {
        move |frontiers| {
            let mut input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);
            sink_logic(&mut input_handle)
        }
    });

    Box::new(KafkaSinkToken { shutdown_flag })
}
