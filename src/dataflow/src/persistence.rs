// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

//use differential_dataflow::hashable::Hashable;
//use log::error;
use differential_dataflow::operators::arrange::ShutdownButton;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::{Scope, Stream};

use dataflow_types::Timestamp;
use expr::{Diff, GlobalId};
use repr::{Datum, Row};

use crate::sink::sink_reschedule;

pub fn persist_source<G>(
    stream: &Stream<G, ((Row, Option<i64>), Timestamp, Diff)>,
    id: GlobalId,
    output_path: PathBuf,
    startup_time: u64,
    nonce: u64,
) -> ShutdownButton<bool>
where
    G: Scope<Timestamp = Timestamp>,
{
    let mut queue: VecDeque<((Row, i64), Timestamp, Diff)> = VecDeque::new();
    let mut vector = Vec::new();
    let mut file_number = 0;
    let worker_index = stream.scope().index();
    let file_name_base = format!(
        "materialized-persist-source-{}-{}-{}-{}",
        id, worker_index, startup_time, nonce
    );
    let mut file = None;
    let mut records_sent_to_file = 0;
    let shutdown = Rc::new(RefCell::new(Some(false)));

    let name = format!("source-persist-{}", id);
    sink_reschedule(&stream, Pipeline, name.clone(), |info| {
        let activator = stream.scope().activator_for(&info.address[..]);
        let shutdown_button = ShutdownButton::new(
            shutdown.clone(),
            stream.scope().activator_for(&info.address[..]),
        );

        let ret = move |input: &mut FrontieredInputHandle<
            _,
            ((Row, Option<i64>), Timestamp, Diff),
            _,
        >| {
            // Grab all of the available Rows and put them in a queue before we
            // write them down
            input.for_each(|_, rows| {
                rows.swap(&mut vector);

                for ((row, position), time, diff) in vector.drain(..) {
                    if position.is_none() {
                        panic!("Received a none position while trying to persist source");
                    }
                    queue.push_back(((row, position.unwrap()), time, diff));
                }
            });

            if file.is_none() {
                let file_name = format!("{}-{}", file_name_base, file_number);
                file_number += 1;
                records_sent_to_file = 0;
                let path = output_path.with_file_name(file_name);
                // TODO set the O_APPEND flag
                // TODO handle file creation errors
                file = Some(std::fs::File::open(&path).expect("opening file expected to succeed"));
            }

            // Send a bounded number of records to the file from the queue. This
            // loop has explicitly been designed so that each iteration sends
            // at most one record to the persistence file
            // TODO: separate toggleable fuel
            for _ in 0..10000 {
                let encoded = if let Some(((row, position), time, diff)) = queue.pop_front() {
                    let mut buf = Vec::new();
                    // First let's write down the data
                    row.encode(&mut buf);

                    // Now lets create a Row for the metadata
                    let metadata_row = Row::pack(&[
                        Datum::Int64(position as i64),
                        Datum::Int64(time as i64),
                        Datum::Int64(diff as i64),
                    ]);
                    metadata_row.encode(&mut buf);

                    buf
                } else {
                    // Nothing left for us to do
                    break;
                };

                // TODO neater error handling
                if let Some(file) = file.as_mut() {
                    file.write_all(&encoded).unwrap();
                    records_sent_to_file += 1;
                }
            }

            // Switch to a new file once this one gets too big
            if records_sent_to_file > 10000 {
                file = None;
                records_sent_to_file = 0;
            }

            if !queue.is_empty() {
                // We need timely to reschedule this operator as we have pending
                // items that we need to send to Kafka
                activator.activate();
                return true;
            }

            false
        };

        (ret, shutdown_button)
    })
}
