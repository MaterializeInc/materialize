// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Validate transaction invariants for Kafka topics written by Debezium
//! when replicating MySQL

use anyhow::Error;
use futures::stream::StreamExt;
use log::{debug, error, info};
use mz_avro::types::Value;
use mz_avro::{from_avro_datum, Schema};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use std::time::Duration;
use structopt::StructOpt;
use url::Url;

#[derive(Clone, Debug, StructOpt)]
pub struct Args {
    #[structopt(long, default_value = "kafka:9092", value_name = "KAFKA_BROKER")]
    pub kafka_brokers: String,
    #[structopt(
        long,
        default_value = "http://schema-registry:8081",
        value_name = "SCHEMA_REGISTRY"
    )]
    pub schema_registry: Url,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    ore::panic::set_abort_on_panic();
    ore::test::init_logging();

    let args: Args = ore::cli::parse_args();

    info!("validating debezium topics!");

    // Read the entire transaction topic and return an ordered list of transaction IDs
    let transaction_ids = get_transaction_ids(args).await?;

    info!("Transactions: {:#?}", transaction_ids);

    // Read the list of database tables, spawning a reader per topic/partition, passing a cloned
    // vec! of transaction IDs and have each reader validate that transactions appear in that order

    Ok(())
}

async fn get_transaction_ids(args: Args) -> Result<Vec<String>, Error> {
    let transaction_topic = "dbserver1.transaction";

    let ccsr = ccsr::ClientConfig::new(args.schema_registry.clone())
        .build()
        .expect("can create schema registry object");
    let key_schema: Schema = ccsr
        .get_schema_by_subject("dbserver1.transaction-key")
        .await
        .expect("can fetch key schema for transactions topic")
        .raw
        .parse()
        .expect("can parse key schema");
    let value_schema: Schema = ccsr
        .get_schema_by_subject("dbserver1.transaction-value")
        .await
        .expect("can fetch key schema for transactions topic")
        .raw
        .parse()
        .expect("can parse value schema");
    debug!(
        "key schema: {:#?}, value schema: {:#?}",
        key_schema, value_schema
    );

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "validate.topics")
        .set("bootstrap.servers", args.kafka_brokers)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("can create transaction topic consumer");

    let (_, num_transactions) = consumer
        .fetch_watermarks(transaction_topic, 0, Duration::from_secs(5))
        .unwrap();

    info!(
        "reading {} transactions from transactions topic",
        num_transactions
    );

    consumer
        .subscribe(&[transaction_topic])
        .expect("can subscribe to transaction topic");

    let mut transactions: Vec<String> = vec![];
    for _ in 0..num_transactions {
        let msg = consumer
            .stream()
            .next()
            .await
            .expect("can read message from topic");

        match msg {
            Ok(m) => {
                debug!(
                    "partition: {}, offset: {}, key: {:#?}, payload: {:#?}",
                    m.partition(),
                    m.offset(),
                    m.key_view::<str>(),
                    m.payload_view::<str>()
                );

                let mut is_end = false;
                match m.payload_view::<[u8]>() {
                    Some(value) => match value {
                        Ok(contents) => {
                            let res = from_avro_datum(&value_schema, &mut &contents[5..]).unwrap();
                            debug!("v: {:?}", res);

                            match res {
                                Value::Record(fields) => {
                                    for (k, v) in fields {
                                        match k.as_str() {
                                            "status" => match v {
                                                Value::String(s) => match s.as_str() {
                                                    "BEGIN" => (),
                                                    "END" => is_end = true,
                                                    other => {
                                                        error!("Unexpected record type: {}", other)
                                                    }
                                                },
                                                other => {
                                                    error!("Unexpected status type: {:?}", other)
                                                }
                                            },
                                            _ => (),
                                        }
                                    }
                                }
                                _ => error!("expected Record type"),
                            }
                        }
                        Err(_) => error!("failed to read value!"),
                    },
                    None => (),
                }

                match m.key_view::<[u8]>() {
                    Some(key) => match key {
                        Ok(contents) => {
                            let res = from_avro_datum(&key_schema, &mut &contents[5..]).unwrap();
                            debug!("k: {:?}", res);

                            match res {
                                Value::Record(fields) => {
                                    for (k, v) in fields {
                                        match k.as_str() {
                                            "id" => match v {
                                                Value::String(s) => {
                                                    if is_end {
                                                        transactions.push(s)
                                                    }
                                                }
                                                other => error!(
                                                    "unexpected Value type for ID: {:?}",
                                                    other
                                                ),
                                            },
                                            _ => (),
                                        }
                                    }
                                }
                                _ => error!("expected Record type!"),
                            }
                        }
                        Err(_) => error!("failed to read key!"),
                    },
                    None => (),
                }

                // let key_reader = Reader::new(m.key_view());
            }
            Err(_) => (),
        }
    }

    return Ok(transactions);
}
