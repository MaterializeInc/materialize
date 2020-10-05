// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BinaryHeap;

use differential_dataflow::AsCollection;
use differential_dataflow::{operators::consolidate::Consolidate, Hashable};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use futures::executor::block_on;
use futures::sink::SinkExt;

use dataflow_types::{TailSinkConnector, Update};
use expr::GlobalId;
use repr::{Diff, Row, Timestamp};

#[derive(PartialEq, Eq, Debug)]
struct TsOrder(Row, Timestamp, Diff);

impl Ord for TsOrder {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.1.cmp(&self.1)
    }
}

impl PartialOrd for TsOrder {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn tail<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: TailSinkConnector,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let mut tx = block_on(connector.tx.connect()).expect("tail transmitter failed");
    let hash = id.hashed();
    let consolidated = stream.as_collection().consolidate();

    let mut stash = BinaryHeap::new();
    consolidated.inner.sink(
        Exchange::new(move |_| hash),
        &format!("tail-{}", id),
        move |input| {
            input.for_each(|_, rows| {
                for (row, time, diff) in rows.replace(vec![]) {
                    let should_emit = if connector.strict {
                        connector.frontier.less_than(&time)
                    } else {
                        connector.frontier.less_equal(&time)
                    };
                    if should_emit {
                        stash.push(TsOrder(row, time, diff));
                    }
                }
            });
            let mut results = Vec::new();
            while stash
                .peek()
                .map(|elt| !input.frontier().less_equal(&elt.1))
                .unwrap_or(false)
            {
                let TsOrder(row, timestamp, diff) = stash.pop().unwrap();
                results.push(Update {
                    row,
                    timestamp,
                    diff,
                })
            }
            if !results.is_empty() {
                // TODO(benesch): this blocks the Timely thread until the send
                // completes. Hopefully it's just a quick write to a kernel buffer,
                // but perhaps not if the batch gets too large? We may need to do
                // something smarter, like offloading to a networking thread.
                block_on(tx.send(results)).expect("tail send failed");
            }
        },
    )
}
