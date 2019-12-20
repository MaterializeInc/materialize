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

use avro_rs::types::{Record, Value};
use byteorder::{NetworkEndian, WriteBytesExt};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

pub fn kafka<G, B>(stream: &Stream<G, B>, id: GlobalId, connector: KafkaSinkConnector)
where
    G: Scope<Timestamp = Timestamp>,
    B: Data + BatchReader<Row, Row, Timestamp, Diff>,
{
    let ccsr_client = ccsr::Client::new(connector.schema_registry_url.clone().parse().unwrap());
    let raw_schema = ccsr_client.get_schema_by_id(connector.schema_id).unwrap();
    let schema = interchange::avro::parse_schema(&raw_schema.raw).unwrap();

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &connector.addr.to_string());
    let producer: FutureProducer = config.create().unwrap();

    stream.sink(Pipeline, &format!("kafka-{}", id), move |input| {
        input.for_each(|_, batches| {
            for batch in batches.iter() {
                let mut cur = batch.cursor();
                while let Some(key) = cur.get_key(&batch) {
                    while let Some(row) = cur.get_val(&batch) {
                        // The first byte is a magic byte (0) that indicates the Confluent
                        // serialization format version, and the next four bytes are a
                        // 32-bit schema ID.
                        //
                        // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
                        let mut buf = Vec::new();
                        buf.write_u8(0).unwrap();
                        buf.write_i32::<NetworkEndian>(connector.schema_id).unwrap();
                        buf.extend(
                            avro_rs::to_avro_datum(&schema, row_to_avro(row))
                                .map_err(|e| e.to_string())
                                .expect("Converting Row to avro failed"),
                        );

                        let mut record: FutureRecord<&Vec<u8>, _> =
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

fn row_to_avro(row: &Row) -> Value {
    let datums = row.unpack();

    // hardcoding for example.
    let datum = datums[0];
    avro_rs::types::Value::Record(vec![(
        String::from("quote"),
        avro_rs::types::Value::Union(Box::new(avro_rs::types::Value::String(
            datum.unwrap_str().to_owned(),
        ))),
    )])
}
