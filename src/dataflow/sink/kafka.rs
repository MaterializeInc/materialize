// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::hashable::Hashable;
use log::error;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, ThreadedProducer};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use interchange::avro::{DiffPair, Encoder};
use repr::{RelationDesc, Row};

// TODO@jldlaughlin: What guarantees does this sink support? #1728
pub fn kafka<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: KafkaSinkConnector,
    desc: RelationDesc,
) where
    G: Scope<Timestamp = Timestamp>,
{
    // We want exactly one worker to send all the data to the sink topic. We
    // achieve that by using an Exchange channel before the sink and mapping
    // all records for the sink to the sink's hash, which has the neat property
    // of also distributing sinks amongst workers
    let sink_hash = id.hashed();

    let encoder = Encoder::new(desc);
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &connector.url.to_string());
    let producer: ThreadedProducer<_> = config.create().unwrap();

    let name = format!("kafka-{}", id);
    stream.sink(
        Exchange::new(move |_| sink_hash),
        &name.clone(),
        move |input| {
            input.for_each(|_, rows| {
                for (row, _time, diff) in rows.iter() {
                    let diff_pair = if *diff < 0 {
                        DiffPair {
                            before: Some(row),
                            after: None,
                        }
                    } else {
                        DiffPair {
                            before: None,
                            after: Some(row),
                        }
                    };
                    let buf = encoder.encode(connector.schema_id, diff_pair);
                    for _ in 0..diff.abs() {
                        let record = BaseRecord::<&Vec<u8>, _>::to(&connector.topic).payload(&buf);
                        if let Err((e, _)) = producer.send(record) {
                            error!("unable to produce in {}: {}", name, e);
                        }
                    }
                    producer.flush(None);
                }
            })
        },
    )
}
