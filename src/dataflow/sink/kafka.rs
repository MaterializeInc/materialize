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
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
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
    // Send new schema to registry, get back the schema id for the sink.
    // TODO(benesch): don't block the worker thread here.
    let ccsr_client = ccsr::Client::new(connector.schema_registry_url.clone());
    let schema_name = format!("{}-value", connector.topic);
    let schema = encoder.writer_schema().canonical_form();
    match ccsr_client.publish_schema(&schema_name, &schema) {
        Ok(schema_id) => {
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", &connector.url.to_string());
            let producer: FutureProducer = config.create().unwrap();

            stream.sink(
                Exchange::new(move |_| sink_hash),
                &format!("kafka-{}", id),
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
                            let buf = encoder.encode(schema_id, diff_pair);
                            for _ in 0..diff.abs() {
                                producer.send(
                                    FutureRecord::<&Vec<u8>, _>::to(&connector.topic).payload(&buf),
                                    1000, /* block_ms */
                                );
                            }
                        }
                    })
                },
            )
        }
        Err(e) => error!("unable to publish schema to registry in kafka sink: {}", e),
    }
}
