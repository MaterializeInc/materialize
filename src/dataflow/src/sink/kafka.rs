// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::thread;

use differential_dataflow::hashable::Hashable;
use log::error;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use interchange::avro::{DiffPair, Encoder};
use repr::{RelationDesc, Row};

static KAFKA_SINK_FUEL: usize = 10000;

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
    let producer = FlushingProducer::new(
        config
            .create_with_context(SinkProducerContext)
            .expect("creating kafka producer for kafka sinks failed"),
    );
    let mut queue: VecDeque<(Row, Diff)> = VecDeque::new();
    let mut vector = Vec::new();
    let mut encoded_buffer = None;

    let name = format!("kafka-{}", id);
    stream.sink(
        Exchange::new(move |_| sink_hash),
        &name.clone(),
        move |input| {
            input.for_each(|_, rows| {
                rows.swap(&mut vector);

                for (row, _time, diff) in vector.drain(..) {
                    queue.push_back((row, diff));
                }
            });

            for _ in 0..KAFKA_SINK_FUEL {
                let (encoded, count) = if let Some((encoded, count)) = encoded_buffer.take() {
                    (encoded, count)
                } else if queue.is_empty() {
                    return;
                } else {
                    let (row, diff) = queue.pop_front().expect("queue known to be nonempty");

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
                    (buf, diff.abs())
                };

                let record = BaseRecord::<&Vec<u8>, _>::to(&connector.topic).payload(&encoded);
                if let Err((e, _)) = producer.inner().send(record) {
                    error!("unable to produce in {}: {}", name, e);
                    if let KafkaError::MessageProduction(RDKafkaError::QueueFull) = e {
                        encoded_buffer = Some((encoded, count));
                        return;
                    }
                }

                if count > 1 {
                    encoded_buffer = Some((encoded, count - 1));
                }
            }
        },
    )
}

struct FlushingProducer<C>
where
    C: ProducerContext + 'static,
{
    inner: Option<ThreadedProducer<C>>,
}

impl<C> FlushingProducer<C>
where
    C: ProducerContext + 'static,
{
    fn new(inner: ThreadedProducer<C>) -> FlushingProducer<C> {
        FlushingProducer { inner: Some(inner) }
    }

    fn inner(&self) -> &ThreadedProducer<C> {
        self.inner.as_ref().unwrap()
    }
}

impl<C> Drop for FlushingProducer<C>
where
    C: ProducerContext,
{
    fn drop(&mut self) {
        let producer = self.inner.take().unwrap();
        thread::spawn(move || producer.flush(None));
    }
}
