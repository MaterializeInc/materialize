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

use anyhow::{bail, Context};
use futures::stream::StreamExt;
use log::{debug, info};
use mz_avro::types::Value;
use mz_avro::{from_avro_datum, Schema};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use std::collections::HashMap;
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
async fn main() -> anyhow::Result<()> {
    ore::panic::set_abort_on_panic();
    ore::test::init_logging();

    let args: Args = ore::cli::parse_args();

    info!("validating debezium topics!");

    // Read the entire transaction topic and return an ordered list of transaction IDs
    let transactions = get_transaction_ids(args).await?;

    validate_transactions(transactions)?;

    // Read the list of database tables, spawning a reader per topic/partition, passing a cloned
    // vec! of transaction IDs and have each reader validate that transactions appear in that order

    info!("consistency checks passed!");
    Ok(())
}

#[derive(Debug)]
struct DebeziumTransactionInfo {
    event_count: i64,
    collections: HashMap<String, i64>,
}

#[derive(Debug)]
enum DebeziumTransactionStatus {
    BEGIN,
    END { txinfo: DebeziumTransactionInfo },
}

#[derive(Debug)]
struct DebeziumTransaction {
    id: String,
    status: DebeziumTransactionStatus,
}

async fn get_transaction_ids(args: Args) -> anyhow::Result<Vec<DebeziumTransaction>> {
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

    // TODO: Correctly handle this unwrap because Kafka likes to fail this API every once and a while
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

    let mut transactions = vec![];
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

                transactions.push(parse_transaction(&value_schema, m)?);
            }
            Err(_) => (),
        }
    }

    return Ok(transactions);
}

fn parse_transaction(
    reader_schema: &Schema,
    msg: rdkafka::message::BorrowedMessage,
) -> anyhow::Result<DebeziumTransaction> {
    match msg.payload_view::<[u8]>() {
        Some(value) => match value {
            Ok(contents) => {
                let res = from_avro_datum(reader_schema, &mut &contents[5..]).unwrap();
                match res {
                    Value::Record(items) => {
                        let mut fields: HashMap<_, _> = items.into_iter().collect();

                        match parse_status(&fields)?.as_str() {
                            "END" => Ok(DebeziumTransaction {
                                id: parse_transaction_id(&fields)?,
                                status: DebeziumTransactionStatus::END {
                                    txinfo: DebeziumTransactionInfo {
                                        event_count: parse_event_count(&fields)?,
                                        collections: parse_collections(&mut fields)?,
                                    },
                                },
                            }),
                            "BEGIN" => Ok(DebeziumTransaction {
                                id: parse_transaction_id(&fields)?,
                                status: DebeziumTransactionStatus::BEGIN,
                            }),
                            other => bail!("Unexpected value for status: {}", other),
                        }
                    }
                    _ => bail!("expected Record type"),
                }
            }
            Err(_) => bail!("failed to read value!"),
        },
        None => bail!("Got None when trying to parse transaction!"),
    }
}

fn parse_status(fields: &HashMap<String, Value>) -> anyhow::Result<String> {
    match fields.get("status") {
        Some(status) => match status {
            Value::String(s) => Ok(s.to_string()),
            _ => bail!("Expected status to be a String but got: {:?}", status),
        },
        None => bail!("Expected to find a status field"),
    }
}

fn parse_transaction_id(fields: &HashMap<String, Value>) -> anyhow::Result<String> {
    match fields.get("id") {
        Some(transaction) => match transaction {
            Value::String(t) => Ok(t.to_string()),
            _ => bail!(
                "Expected transaction to be a String but got: {:?}",
                transaction
            ),
        },
        None => bail!("Expected to find a id field!"),
    }
}

fn parse_event_count(fields: &HashMap<String, Value>) -> anyhow::Result<i64> {
    match fields.get("event_count") {
        Some(inner) => match inner {
            Value::Union { inner: value, .. } => {
                if let Value::Long(c) = **value {
                    Ok(c)
                } else {
                    bail!("Expect Long type for event, got {:?}", **value)
                }
            }
            other => bail!("Expected union type for event_count, got {:?}", other),
        },
        None => bail!("Expected event count for END transaction message!"),
    }
}

fn parse_collections(fields: &mut HashMap<String, Value>) -> anyhow::Result<HashMap<String, i64>> {
    match fields.remove("data_collections") {
        Some(inner) => match inner {
            Value::Union { inner: value, .. } => {
                if let Value::Array(items) = *value {
                    let mut collections = HashMap::new();
                    for i in items {
                        let (collection, count) = parse_data_collection(i)?;
                        collections.insert(collection, count);
                    }
                    Ok(collections)
                } else {
                    bail!("Expect Array type for event, got {:?}", *value)
                }
            }
            other => bail!("Expected union type for data_collections, got {:?}", other),
        },
        None => bail!("Expected data_collections for END transaction message!"),
    }
}

fn parse_data_collection(collection: Value) -> anyhow::Result<(String, i64)> {
    match collection {
        Value::Record(items) => {
            let fields: HashMap<_, _> = items.into_iter().collect();

            let collection_name = match fields.get("data_collection") {
                Some(value) => match value {
                    Value::String(s) => s.to_string(),
                    _ => bail!(
                        "Expected string type for data collection name, got {:?}",
                        value
                    ),
                },
                None => bail!("Expected data_collection field from data collection"),
            };

            let count = match fields.get("event_count") {
                Some(value) => match value {
                    Value::Long(c) => *c,
                    _ => bail!("Expected long for count, got {:?}", value),
                },
                None => bail!("Expected data_collection field from data collection"),
            };

            Ok((collection_name, count))
        }
        _ => bail!("Expected Record for data collection, got {:?}", collection),
    }
}

fn validate_transactions(transactions: Vec<DebeziumTransaction>) -> anyhow::Result<()> {
    debug!("Transactions: {:#?}", transactions);

    for transaction in transactions {
        validate_transaction_properties(&transaction).with_context(|| {
            format!(
                "Transaction {} failed consistency checks: {:?}",
                transaction.id, transaction
            )
        })?;
    }

    Ok(())
}

fn validate_transaction_properties(transaction: &DebeziumTransaction) -> anyhow::Result<()> {
    match &transaction.status {
        DebeziumTransactionStatus::END { txinfo } => {
            if txinfo.event_count == 0 {
                bail!("Event count is zero");
            }

            let sum_collections_count = txinfo.collections.iter().map(|(_, c)| c).sum::<i64>();

            if txinfo.event_count != sum_collections_count {
                bail!("Event count does not match sum of collections count");
            }
        }
        _ => (),
    }

    Ok(())
}
