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
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

//use differential_dataflow::hashable::Hashable;
//use log::error;
use byteorder::{ByteOrder, NetworkEndian};
use differential_dataflow::operators::arrange::ShutdownButton;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::{source, FrontieredInputHandle};
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
                let mut path = output_path.clone();
                path.push(file_name);
                // TODO handle file creation errors
                file = Some(
                    OpenOptions::new()
                        .append(true)
                        .create_new(true)
                        .open(&path)
                        .expect("creating persistence file must succeed"),
                );
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

// TODO reuse justin's code for this
#[derive(Debug, Clone)]
pub struct RecordIter {
    data: Vec<u8>,
    idx: usize,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub row: Row,
    pub position: i64,
    pub time: i64,
    pub diff: i64,
}

impl RecordIter {
    fn next_rec(&mut self) -> Row {
        let (_, data) = self.data.split_at(self.idx);
        let len = NetworkEndian::read_u32(data) as usize;
        let (_, data) = data.split_at(4);
        let (row, _) = data.split_at(len);
        self.idx += 4 + len;
        Row::decode(row.to_vec().clone())
    }
}

impl Iterator for RecordIter {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        if self.data.len() <= self.idx {
            return None;
        }
        let row = self.next_rec();
        let meta_row = self.next_rec();
        let meta = meta_row.unpack();
        let position = meta[0].unwrap_int64();
        let time = meta[1].unwrap_int64();
        let diff = meta[2].unwrap_int64();
        Some(Record {
            row,
            position,
            time,
            diff,
        })
    }
}

pub fn read_persisted_source<G>(
    scope: &G,
    id: GlobalId,
    files: Vec<PathBuf>,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let name = format!("source-persist-reread-{}", id);
    source(scope, &name, |capability, info| {
        let activator = scope.activator_for(&info.address[..]);

        let mut cap = Some(capability);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                for f in files.iter() {
                    // TODO handle file read errors
                    // TODO this operator insists on reading everything in one shot
                    // TODO this operator is not safe for multiple threads at the moment
                    let data = fs::read(f).expect("reading from file cannot fail");

                    let iter = RecordIter { data, idx: 0 };

                    let records: Vec<Record> = iter.collect();
                    for r in records.into_iter() {
                        let ts = r.time as u64;
                        let ts_cap = cap.delayed(&ts);
                        output.session(&ts_cap).give((r.row, ts, r.diff as isize));
                    }
                }

                done = true;
            }
            if done {
                cap = None;
            } else {
                activator.activate();
            }
        }
    })
}
