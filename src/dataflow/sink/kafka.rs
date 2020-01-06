// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use repr::{RelationDesc, Row};

use interchange::avro::Encoder;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

pub fn kafka<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: KafkaSinkConnector,
    relation_desc: RelationDesc,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let schema = interchange::avro::encode_schema(&relation_desc).unwrap();

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &connector.addr.to_string());
    let producer: FutureProducer = config.create().unwrap();

    stream.sink(Pipeline, &format!("kafka-{}", id), move |input| {
        let encoder = Encoder::new(&schema.to_string());
        input.for_each(|_, rows| {
            for (row, _time, _diff) in rows.iter() {
                let buf = encoder.encode(connector.schema_id, row);
                let record: FutureRecord<&Vec<u8>, _> =
                    FutureRecord::to(&connector.topic).payload(&buf);
                producer.send(record, 1000 /* block_ms */);
            }
        })
    })
}
