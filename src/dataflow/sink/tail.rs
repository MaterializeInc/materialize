// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use dataflow_types::{Diff, Exfiltration, TailSinkConnector, Timestamp, Update};
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::BatchReader;
use repr::Datum;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub fn tail<G, B>(stream: &Stream<G, B>, name: &str, connector: TailSinkConnector)
where
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
            use futures::{Future, Sink};
            let sink = connector.tx.connect().wait().unwrap();
            sink.send(Exfiltration::Tail(result)).wait().unwrap();
        });
    })
}
