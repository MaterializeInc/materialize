// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::fmt;

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::BatchReader;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;

pub fn kafka<G, B, K, V>(stream: &Stream<G, B>, id: GlobalId, _connector: KafkaSinkConnector)
where
    G: Scope<Timestamp = Timestamp>,
    B: Data + BatchReader<K, V, Timestamp, Diff>,
    K: fmt::Debug,
{
    stream.sink(Pipeline, &format!("kafka-{}", id), move |input| {
        input.for_each(|_, batches| {
            for batch in batches.iter() {
                let mut cur = batch.cursor();
                while let Some(key) = cur.get_key(&batch) {
                    // TODO(benesch): send these tuples to Kafka.
                    println!("sinking {:?}", key);
                    cur.step_key(&batch);
                }
            }
        })
    })
}
