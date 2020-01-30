// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::order::PartialOrder;

use futures::executor::block_on;
use futures::sink::SinkExt;

use dataflow_types::{Diff, TailSinkConnector, Timestamp, Update};
use expr::GlobalId;
use repr::Row;

pub fn tail<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: TailSinkConnector,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let mut tx = block_on(connector.tx.connect()).expect("tail transmitter failed");
    stream.sink(Pipeline, &format!("tail-{}", id), move |input| {
        input.for_each(|_, rows| {
            let mut results: Vec<Update> = Vec::new();
            for (row, time, diff) in rows.iter() {
                if connector.since.less_than(time) {
                    results.push(Update {
                        row: row.clone(),
                        timestamp: *time,
                        diff: *diff,
                    });
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
