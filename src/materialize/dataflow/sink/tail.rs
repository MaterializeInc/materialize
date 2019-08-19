// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::dataflow::{DataflowResults, Diff, TailSinkConnector, Timestamp, Update};
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::BatchReader;
use repr::Datum;
use std::cell::RefCell;
use std::rc::Rc;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

fn push_tail_update(
    c: &TailSinkConnector,
    k: Vec<Update>,
    rpc_client: &Rc<RefCell<reqwest::Client>>,
) {
    let encoded = bincode::serialize(&DataflowResults::Tailed(k)).unwrap();
    rpc_client
        .borrow_mut()
        .post(&c.handler)
        .header("X-Materialize-Query-UUID", c.connection_uuid.to_string())
        .body(encoded)
        .send()
        .unwrap();
}

pub fn tail<G, B>(
    stream: &Stream<G, B>,
    name: &str,
    connector: TailSinkConnector,
    rpc_client: Rc<RefCell<reqwest::Client>>,
) where
    G: Scope<Timestamp = Timestamp>,
    B: Data + BatchReader<Vec<Datum>, (), Timestamp, Diff>,
{
    stream.sink(Pipeline, &name, move |input| {
        input.for_each(|_, batches| {
            let mut result: Vec<Update> = Vec::new();
            for batch in batches.iter() {
                let mut cur = batch.cursor();
                while let Some(key) = cur.get_key(&batch) {
                    cur.map_times(&batch, |time, diff| {
                        result.push(Update {
                            row: key.clone(),
                            timestamp: *time,
                            diff: *diff,
                        });
                    });
                    cur.step_key(&batch);
                }
            }
            push_tail_update(&connector, result, &rpc_client);
        });
    })
}
