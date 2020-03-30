// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use differential_dataflow::hashable::Hashable;
use futures::executor::block_on;
use log::error;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, KafkaSinkConnector, Timestamp};
use expr::GlobalId;
use interchange::avro::{DiffPair, Encoder};
use ore::collections::CollectionExt;
use repr::{RelationDesc, Row};

// TODO@jldlaughlin: What guarantees does this sink support? #1728
pub fn kafka<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: KafkaSinkConnector,
    desc: RelationDesc,
    write_to_kafka: bool,
) where
    G: Scope<Timestamp = Timestamp>,
{
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

            if write_to_kafka {
                let admin: AdminClient<DefaultClientContext> =
                    config.create().expect("creating admin kafka client failed");

                let admin_opts =
                    AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
                let new_topic = NewTopic::new(&connector.topic, 1, TopicReplication::Fixed(1));
                let res = block_on(admin.create_topics(&[new_topic], &admin_opts));
                match res {
                    Ok(res) => {
                        if res.len() != 1 {
                            error!(
                "error creating topic {} for sink: kafka topic creation returned {} results, but exactly one result was expected",
                connector.topic,
                res.len()
            );
                            return;
                        }
                        match res.into_element() {
                            Ok(_) => (),
                            Err((_, err)) => {
                                error!(
                                    "error creating topic {} for sink: {}",
                                    connector.topic,
                                    err.to_string()
                                );
                                return;
                            }
                        };
                    }
                    Err(err) => {
                        error!(
                            "error creating new topic {} for sink: {}",
                            connector.topic,
                            err.to_string()
                        );
                        return;
                    }
                }
            }
            let producer: FutureProducer = config.create().unwrap();
            let hash = id.hashed();

            stream.sink(
                Exchange::new(move |_| hash),
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
