// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::time::Duration;

use differential_dataflow::hashable::Hashable;
use log::error;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use interchange::avro::{DiffPair, Encoder};
use repr::{RelationDesc, Row};

use super::util::sink_reschedule;

#[derive(Clone)]
pub struct SinkProducerContext;

impl ClientContext for SinkProducerContext {}
impl ProducerContext for SinkProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult, _: Self::DeliveryOpaque) {
        match result {
            Ok(_) => (),
            Err((e, msg)) => error!(
                "received error while writing to kafka sink topic {}: {}",
                msg.topic(),
                e
            ),
        }
    }
}

// TODO@jldlaughlin: What guarantees does this sink support? #1728
pub fn kafka<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: KafkaSinkConnector,
    desc: RelationDesc,
) where
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
    let producer: ThreadedProducer<_> = config
        .create_with_context(SinkProducerContext)
        .expect("creating kafka producer for kafka sinks failed");
    let mut queue: VecDeque<(Row, Diff)> = VecDeque::new();
    let mut vector = Vec::new();
    let mut encoded_buffer = None;

    let name = format!("kafka-{}", id);
    sink_reschedule(
        &stream,
        Exchange::new(move |_| sink_hash),
        &name.clone(),
        |info| {
            let activator = stream.scope().activator_for(&info.address[..]);
            move |input| {
                // Grab all of the available Rows and put them in a queue before we
                // send it over to Kafka. We Even though we want to do bounded work
                // per sink invocation, we still need to remember all inputs as we
                // receive them
                input.for_each(|_, rows| {
                    rows.swap(&mut vector);

                    for (row, _time, diff) in vector.drain(..) {
                        queue.push_back((row, diff));
                    }
                });

                // Send a bounded number of records to Kafka from the queue. This
                // loop has explicitly been designed so that each iteration sends
                // at most one record to Kafka
                for _ in 0..connector.fuel {
                    let (encoded, count) = if let Some((encoded, count)) = encoded_buffer.take() {
                        // We still need to send more copies of this record.
                        (encoded, count)
                    } else if let Some((row, diff)) = queue.pop_front() {
                        // Convert a previously queued (Row, Diff) to a Avro diff
                        // envelope record
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

                        let buf = encoder.encode_unchecked(connector.schema_id, diff_pair);
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
                        }
                    }

                    // Cache the Avro encoded data if we need to send again and
                    // remember how many more times we need to send it
                    if count > 1 {
                        encoded_buffer = Some((encoded, count - 1));
                    }
                }

                if encoded_buffer.is_some() || !queue.is_empty() {
                    // We need timely to reschedule this operator as we have pending
                    // items that we need to send to Kafka
                    activator.activate();
                    return true;
                }

                if producer.in_flight_count() > 0 {
                    // We still have messages that need to be flushed out to Kafka
                    // Let's make sure to keep the sink operator around until
                    // we flush them out
                    activator.activate_after(Duration::from_secs(5));
                    return true;
                }

                false
            }
        },
    )
}
