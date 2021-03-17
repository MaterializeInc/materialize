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

use differential_dataflow::Collection;
use lazy_static::lazy_static;
use log::{error, info};
use prometheus::{
    register_int_counter_vec, register_uint_gauge_vec, IntCounter, IntCounterVec, UIntGauge,
    UIntGaugeVec,
};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::Producer;
use rdkafka::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::Scope;

use dataflow_types::{KafkaSinkConnector, SinkAsOf};
use expr::GlobalId;
use interchange::avro::{self, Encoder};
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
    txn_timeout: Duration,
}

impl KafkaSink {
    fn transition_on_txn_error(
        &self,
        current_state: SendState,
        ts: u64,
        e: KafkaError,
    ) -> SendState {
        error!(
            "encountered error during kafka interaction. {} in state {:?} at time {} : {}",
            &self.name, current_state, ts, e
        );

        match e {
            KafkaError::Transaction(e) => {
                if e.txn_requires_abort() {
                    SendState::AbortTxn
                } else if e.is_retriable() {
                    current_state
                } else {
                    SendState::Shutdown
                }
            }
            _ => SendState::Shutdown,
        }
    }

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

#[derive(Debug, Copy, Clone)]
enum SendState {
    // Initialize ourselves as a transactional producer with Kafka
    // Note that this only runs once across all workers - it should only execute
    // for the worker that will actually be publishing to kafka
    Init,
    // Corresponds to a Kafka begin_transaction call
    BeginTxn,
    // Write BEGIN consistency record
    Begin,
    // Flush pending rows for closed timestamps
    Draining {
        // row_index points to the current flushing row within the closed timestamp
        // we're processing
        row_index: usize,
        // multiple copies of a row may need to be sent if its cardinality is >1
        repeat_counter: usize,
        // a count of all rows sent, accounting for the repeat counter
        total_sent: i64,
    },
    // Write END consistency record
    End(i64),
    // Corresponds to a Kafka commit_transaction call
    CommitTxn,
    // Transitioned to when an error in a previous transactional state requires an abort
    AbortTxn,
    // Transitioned to when the sink needs to be closed
    Shutdown,
}

#[derive(Debug)]
struct EncodedRow {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    count: usize,
}

// TODO@jldlaughlin: What guarantees does this sink support? #1728
pub fn kafka<G>(
    collection: Collection<G, (Option<Row>, Option<Row>)>,
    id: GlobalId,
    connector: KafkaSinkConnector,
    key_desc: Option<RelationDesc>,
    value_desc: RelationDesc,
    as_of: SinkAsOf,
) -> Box<dyn Any>
where
    G: Scope<Timestamp = Timestamp>,
{
    let stream = &collection.inner;
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &connector.addrs.to_string());

    // Ensure that messages are sinked in order and without duplicates. Note that
    // this only applies to a single instance of a producer - in the case of restarts,
    // all bets are off and full exactly once support is required.
    config.set("enable.idempotence", "true");

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

    // TODO(eli): replace with https://github.com/fede1024/rust-rdkafka/pull/333
    let transactional = connector.config_options.contains_key("transactional.id");

    let name = format!("kafka-{}", id);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let mut builder = OperatorBuilder::new(name.clone(), stream.scope());

    let s = {
        let metrics = SinkMetrics::new(
            &connector.topic,
            &id.to_string(),
            &stream.scope().index().to_string(),
        );

        let encoder = Encoder::new(key_desc, value_desc, connector.consistency.is_some());

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
            txn_timeout: Duration::from_secs(5),
        }
    };

    let mut pending_rows: HashMap<Timestamp, Vec<EncodedRow>> = HashMap::new();
    let mut ready_rows: VecDeque<(Timestamp, Vec<EncodedRow>)> = VecDeque::new();
    let mut state = SendState::Init;
    let mut vector = Vec::new();

    let mut sink_logic = move |input: &mut FrontieredInputHandle<
        _,
        ((Option<Row>, Option<Row>), Timestamp, Diff),
        _,
    >| {
        if s.shutdown_flag.load(Ordering::SeqCst) {
            info!("shutting down sink: {}", &s.name);
            return false;
        }

        // Encode and queue all pending rows waiting to be sent to kafka
        input.for_each(|_, rows| {
            rows.swap(&mut vector);
            for ((key, value), time, diff) in vector.drain(..) {
                let should_emit = if as_of.strict {
                    as_of.frontier.less_than(&time)
                } else {
                    as_of.frontier.less_equal(&time)
                };

                let previously_published = match &connector.consistency {
                    Some(consistency) => match consistency.gate_ts {
                        Some(ts) => as_of.frontier.less_equal(&ts),
                        None => false,
                    },
                    None => false,
                };

                if !should_emit || previously_published {
                    // Skip stale data for already published timestamps
                    continue;
                }

                assert!(diff >= 0, "can't sink negative multiplicities");
                if diff == 0 {
                    // Explicitly refuse to send no-op records
                    continue;
                };
                let diff = diff as usize;

                let key = key.map(|key| {
                    s.encoder
                        .encode_key_unchecked(connector.key_schema_id.unwrap(), key)
                });
                let value = value.map(|value| {
                    s.encoder
                        .encode_value_unchecked(connector.value_schema_id, value)
                });

                let rows = pending_rows.entry(time).or_default();
                rows.push(EncodedRow {
                    key,
                    value,
                    count: diff,
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
                    SendState::Init => {
                        let result = if transactional {
                            s.producer.init_transactions(s.txn_timeout)
                        } else {
                            Ok(())
                        };

                        match result {
                            Ok(()) => SendState::BeginTxn,
                            Err(e) => s.transition_on_txn_error(state, *ts, e),
                        }
                    }
                    SendState::BeginTxn => {
                        let result = if transactional {
                            s.producer.begin_transaction()
                        } else {
                            Ok(())
                        };

                        match result {
                            Ok(()) => SendState::Begin,
                            Err(e) => s.transition_on_txn_error(state, *ts, e),
                        }
                    }
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
                        let record = BaseRecord::to(&connector.topic);
                        let record = if encoded_row.value.is_some() {
                            record.payload(encoded_row.value.as_ref().unwrap())
                        } else {
                            record
                        };
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
                        SendState::CommitTxn
                    }
                    SendState::CommitTxn => {
                        let result = if transactional {
                            s.producer.commit_transaction(s.txn_timeout)
                        } else {
                            Ok(())
                        };

                        match result {
                            Ok(()) => {
                                ready_rows.pop_front();
                                SendState::BeginTxn
                            }
                            Err(e) => s.transition_on_txn_error(state, *ts, e),
                        }
                    }
                    SendState::AbortTxn => {
                        let result = if transactional {
                            s.producer.abort_transaction(s.txn_timeout)
                        } else {
                            Ok(())
                        };

                        match result {
                            Ok(()) => SendState::BeginTxn,
                            Err(e) => s.transition_on_txn_error(state, *ts, e),
                        }
                    }
                    SendState::Shutdown => {
                        s.shutdown_flag.store(false, Ordering::SeqCst);
                        break;
                    }
                };
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

    // We want exactly one worker to send all the data to the sink topic.
    // This should already have been handled upstream (in render/mod.rs),
    // so we can use `Pipeline` here.
    let mut input = builder.new_input(stream, Pipeline);
    builder.build_reschedule(|_capabilities| {
        move |frontiers| {
            let mut input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);
            sink_logic(&mut input_handle)
        }
    });

    Box::new(KafkaSinkToken { shutdown_flag })
}
