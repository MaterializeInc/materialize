// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::exfiltrate::Exfiltrator;
use crate::{Diff, TailSinkConnector, Timestamp, Update};
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::BatchReader;
use repr::Datum;
use std::rc::Rc;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub fn tail<G, B>(
    stream: &Stream<G, B>,
    name: &str,
    connector: TailSinkConnector,
    exfiltrator: Rc<Exfiltrator>,
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
            exfiltrator.send_tail(connector.conn_id, result);
        });
    })
}
