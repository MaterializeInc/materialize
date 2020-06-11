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
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use interchange::avro::{DiffPair, Encoder};
use repr::{RelationDesc, Row};

use sink::util::sink_reschedule;

pub fn persist_source<G>(
    stream: &Stream<G, ((Row, Option<i64>), Timestamp, Diff)>,
    id: GlobalId,
    output_path: PathBuf,
    startup_time: u64,
    nonce: u64,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let mut queue: VecDeque<((Row, i64), Timestamp, Diff)> = VecDeque::new();
    let mut vector = Vec::new();
    let mut encoded_buffer = None;
    let file_number = 0;
    let worker_index = stream.scope().index(); 
    let file_name_base = format!("materialized-persist-source-{}-{}-{}-{}", id, worker_index, startup_time, nonce);
    let mut file = None;

    let name = format!("source-persist-{}", id);
    sink_reschedule(
        &stream,
        Pipeline,
        &name.clone(),
        |info| {
            let activator = stream.scope().activator_for(&info.address[..]);
            move |input| {
                // Grab all of the available Rows and put them in a queue before we
                // write them down
                input.for_each(|_, rows| {
                    rows.swap(&mut vector);

                    for ((row, position), time, diff) in vector.drain(..) {
                        if position.is_none() {
                            panic!("Received a none position while trying to persist source");
                        }
                        queue.push_back((row, position.unwrap()), time, diff));
                    }
                });

                if let None = file {
                    let file_name = format!("{}-{}", file_name_base, file_number);
                    file_number += 1;
                    let path = output_path.clone().set_file_name(file_name);
                    file = Some(std::fs::File::open(path));
                }

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
