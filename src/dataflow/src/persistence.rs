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
    let file_number = 0;
    let worker_index = stream.scope().index(); 
    let file_name_base = format!("materialized-persist-source-{}-{}-{}-{}", id, worker_index, startup_time, nonce);
    let mut file = None;
    let mut records_sent_to_file = 0;

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
                    records_sent_to_file = 0;
                    let path = output_path.clone().set_file_name(file_name);
                    // TODO set the O_APPEND flag
                    file = Some(std::fs::File::open(path));
                }

                let file = file.expect("file known to exist");

                // Send a bounded number of records to the file from the queue. This
                // loop has explicitly been designed so that each iteration sends
                // at most one record to Kafka
                for _ in 0..connector.fuel {
                    let encoded = if let Some(((row, position), time, diff)) = queue.pop_front() {
                        let mut buf = Vec:new();
                        // First let's write down the data
                        row.encode(&mut buf);

                        // Now lets create a Row for the metadata
                        let metadata_row = Row::pack(&[Datum::Int64(position as i64), Datum::Int64(timestamp as i64), Datum::Int64(diff as i64)]);
                        metadata_row.encode(&mut buff);

                        buf
                    } else {
                        // Nothing left for us to do
                        break;
                    };

                    // TODO neater error handling
                    file.write_all(encoded).unwrap();
                    records_sent_to_file += 1;

                }

                if encoded_buffer.is_some() || !queue.is_empty() {
                    // We need timely to reschedule this operator as we have pending
                    // items that we need to send to Kafka
                    activator.activate();
                    return true;
                }

                false
            }
        },
    )
}
