// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::BatchReader;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::order::PartialOrder;
use timely::Data;

use futures::executor::block_on;
use futures::sink::SinkExt;

use dataflow_types::{Diff, TailSinkConnector, Timestamp, Update};
use expr::GlobalId;
use repr::Row;

pub fn tail<G, B>(stream: &Stream<G, B>, id: GlobalId, connector: TailSinkConnector)
where
    G: Scope<Timestamp = Timestamp>,
    B: Data + BatchReader<Row, Row, Timestamp, Diff>,
{
    let mut tx = block_on(connector.tx.connect()).expect("tail transmitter failed");
    stream.sink(Pipeline, &format!("tail-{}", id), move |input| {
        input.for_each(|_, batches| {
            let mut results: Vec<Update> = Vec::new();
            for batch in batches.iter() {
                let mut cur = batch.cursor();
                while let Some(_key) = cur.get_key(&batch) {
                    while let Some(row) = cur.get_val(&batch) {
                        cur.map_times(&batch, |time, diff| {
                            if connector.since.less_than(time) {
                                results.push(Update {
                                    row: row.clone(),
                                    timestamp: *time,
                                    diff: *diff,
                                });
                            }
                        });
                        cur.step_val(&batch)
                    }
                    cur.step_key(&batch)
                }
            }

            // TODO(benesch): this blocks the Timely thread until the send
            // completes. Hopefully it's just a quick write to a kernel buffer,
            // but perhaps not if the batch gets too large? We may need to do
            // something smarter, like offloading to a networking thread.
            block_on(tx.send(results)).expect("tail send failed");
        });
    })
}
