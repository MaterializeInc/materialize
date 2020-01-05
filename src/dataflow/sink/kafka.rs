// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use repr::Row;

pub fn kafka<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    _connector: KafkaSinkConnector,
) where
    G: Scope<Timestamp = Timestamp>,
{
    stream.sink(Pipeline, &format!("kafka-{}", id), move |input| {
        input.for_each(|_, rows| {
            for (row, _time, _diff) in rows.iter() {
                // TODO(benesch): send these tuples to Kafka.
                println!("sinking {:?}", row);
            }
        })
    })
}
