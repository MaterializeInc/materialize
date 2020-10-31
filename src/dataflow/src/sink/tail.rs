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

use futures::executor::block_on;
use futures::sink::SinkExt;

use dataflow_types::TailSinkConnector;
use expr::GlobalId;
use repr::adt::decimal::Significand;
use repr::{Datum, Diff, Row, RowPacker, Timestamp};

pub fn tail<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: TailSinkConnector,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let mut tx = block_on(connector.tx.connect()).expect("tail transmitter failed");
    let mut packer = RowPacker::new();
    stream.sink(Pipeline, &format!("tail-{}", id), move |input| {
        input.for_each(|_, rows| {
            let mut results: Vec<Row> = Vec::new();
            for (row, time, diff) in rows.iter() {
                let should_emit = if connector.strict {
                    connector.frontier.less_than(time)
                } else {
                    connector.frontier.less_equal(time)
                };
                if should_emit {
                    packer.push(Datum::Decimal(Significand::new(*time as i128)));
                    packer.push(Datum::Int64(*diff as i64));
                    packer.extend_by_row(row);
                    results.push(packer.finish_and_reuse());
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
