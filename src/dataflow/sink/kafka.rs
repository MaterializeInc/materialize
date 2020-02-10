// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use log::error;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use interchange::avro::Encoder;
use repr::{RelationDesc, Row};

// TODO@jldlaughlin: What guarantess does this sink support? #1728

// TODO@jldlaughlin: Progress tracking for kafka sinks #1442
//
// Right now, every time Materialize crashes and recovers these sinks
// will resend each record to Kafka. This is not entirely horrible, but also
// obviously not ideal! But until we have a more concrete idea of what
// people will require from Kafka sinks, we're punting on implementing this.
//
// For posterity, here are some of the options we discussed to
// implement this:
//      - Use differential's Consolidate operator on the batches we are
//        iterating through. Track our progress in sending batches to Kafka
//        using the "watermarks" from the consolidated batch (inclusive on the
//        lower bound, exclusive on the upper bound). Store these timestamps
//        persistently (in mzdata/catalog) in order to either:
//            - Resend everything including and after the last successfully sent batch.
//              This assumes the Kafka topic we are sending to handles duplicates.
//            - First, send a negative diff of each record in the last successful
//              batch. Then, resend everything after.
//
//     - Append something like a "Materialize start up timestamp" to the
//       end of the Kafka topic name. This accepts resending all of the data,
//       but will not duplicate data in a single topic.
//            - NB: This, like other decisions we've made, assumes that
//              the user has configured their Kafka instance to automatically
//              create new topics.
pub fn kafka<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: KafkaSinkConnector,
    relation_desc: RelationDesc,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let schema = interchange::avro::encode_schema(&relation_desc).expect("");

    // Send new schema to registry, get back the schema id for the sink.
    // TODO(benesch): don't block the worker thread here.
    let ccsr_client = ccsr::Client::new(connector.schema_registry_url.clone());
    match ccsr_client.publish_schema(&connector.topic, &schema.to_string()) {
        Ok(schema_id) => {
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", &connector.addr.to_string());
            let producer: FutureProducer = config.create().unwrap();

            stream.sink(Pipeline, &format!("kafka-{}", id), move |input| {
                let encoder = Encoder::new(&schema.to_string());
                input.for_each(|_, rows| {
                    for (row, _time, _diff) in rows.iter() {
                        let buf = encoder.encode(schema_id, row);
                        let record: FutureRecord<&Vec<u8>, _> =
                            FutureRecord::to(&connector.topic).payload(&buf);
                        producer.send(record, 1000 /* block_ms */);
                    }
                })
            })
        }
        Err(e) => error!("unable to publish schema to registry in kafka sink: {}", e),
    }
}
