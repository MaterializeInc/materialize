// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
