// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::BatchReader;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use repr::Row;

use interchange::avro::Encoder;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

pub fn kafka<G, B>(stream: &Stream<G, B>, id: GlobalId, connector: KafkaSinkConnector)
where
    G: Scope<Timestamp = Timestamp>,
    B: Data + BatchReader<Row, Row, Timestamp, Diff>,
{
    let ccsr_client = ccsr::Client::new(
        connector
            .schema_registry_url
            .clone()
            .to_string()
            .parse()
            .unwrap(),
    );
    let schema = ccsr_client.get_schema_by_id(connector.schema_id).unwrap();

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &connector.addr.to_string());
    let producer: FutureProducer = config.create().unwrap();

    stream.sink(Pipeline, &format!("kafka-{}", id), move |input| {
        let encoder = Encoder::new(&schema.raw);
        input.for_each(|_, batches| {
            for batch in batches.iter() {
                let mut cur = batch.cursor();
                while let Some(_key) = cur.get_key(&batch) {
                    while let Some(row) = cur.get_val(&batch) {
                        let buf = encoder.encode(connector.schema_id, row);
                        let record: FutureRecord<&Vec<u8>, _> =
                            FutureRecord::to(&connector.topic).payload(&buf);
                        producer.send(record, 1000 /* block_ms */);
                        cur.step_val(&batch);
                    }
                    cur.step_key(&batch);
                }
            }
        })
    })
}
