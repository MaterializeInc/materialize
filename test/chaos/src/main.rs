// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use md5::{Digest, Md5};
use rand::Rng;
use structopt::StructOpt;

use ore::retry;
use test_util::{generator, kafka, mz_client};

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ERROR: {:#?}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), anyhow::Error> {
    let timer = std::time::Instant::now();
    let args = Args::from_args();
    env_logger::init();

    let seed: u32 = rand::thread_rng().gen();
    let topic = format!("chaos-{}", seed);
    log::info!(
        "starting chaos test with mzd={}:{} kafka={} topic={} message_count={}",
        args.materialized_host,
        args.materialized_port,
        args.kafka_url,
        topic,
        args.message_count
    );

    // Create Kafka topic.
    log::info!("creating Kafka topic=> {}", topic);
    let kafka_client = kafka::kafka_client::KafkaClient::new(
        &args.kafka_url,
        "materialize.chaos",
        &topic,
        &[("enable.idempotence", "true")],
    )?;
    retry::retry_for(Duration::from_secs(10), |_| {
        kafka_client.create_topic(args.kafka_partitions, Some(Duration::from_secs(10)))
    })
    .await?;

    let kafka_task = tokio::spawn({
        let message_count = args.message_count;
        async move { generate_and_push_records(kafka_client, message_count).await }
    });
    let materialize_task = tokio::spawn({
        let materialized_host = args.materialized_host.clone();
        let materialized_port = args.materialized_port;
        let kafka_url = args.kafka_url.clone();
        let message_count = args.message_count;
        async move {
            query_materialize(
                &materialized_host,
                materialized_port,
                &kafka_url,
                &topic,
                message_count,
            )
            .await
        }
    });

    let (kafka_result, materialize_result) = futures::join!(kafka_task, materialize_task);
    let kafka_hash = kafka_result??;
    let materialize_hash = materialize_result??;

    if kafka_hash == materialize_hash {
        log::info!("row hashes matched!");
    } else {
        return Err(anyhow::anyhow!(
            "mismatched row hashes: kafka: {:?}, materialize: {:?}",
            kafka_hash,
            materialize_hash
        ));
    }

    log::info!(
        "Completed chaos testing in {} milliseconds",
        timer.elapsed().as_millis()
    );
    Ok(())
}

/// Generate byte records, push them to Kafka, and return their md5 hash.
async fn generate_and_push_records(
    mut kafka_client: kafka::kafka_client::KafkaClient,
    message_count: usize,
) -> Result<String, anyhow::Error> {
    log::info!("pushing {} records to Kafka", message_count);
    let mut hasher = Md5::new();
    let ten_percent = message_count / 10;
    let mut sent = 0;
    while sent < message_count {
        if sent % ten_percent == 0 {
            log::info!("have sent {} records...", sent);
        }

        let record = generator::bytes::generate_bytes(30);
        match kafka_client.send(&record).await {
            Ok(()) => {
                sent += 1;
                hasher.input(&record);
            }
            Err(e) => {
                log::error!("failed to send bytes to Kafka: {}", e);
            }
        }
    }
    Ok(format!("{:x}", hasher.result()))
}

async fn query_materialize(
    materialized_host: &str,
    materialized_port: u16,
    kafka_url: &str,
    topic: &str,
    message_count: usize,
) -> Result<String, anyhow::Error> {
    let mz_client = mz_client::client(materialized_host, materialized_port).await?;

    // Create Kafka source.
    let query = format!(
        "CREATE SOURCE src FROM KAFKA BROKER '{}' TOPIC '{}' FORMAT BYTES",
        kafka_url, topic
    );
    log::info!("creating source=> {}", query);
    mz_client.execute(&*query, &[]).await?;

    // Materialize two views: a count and a select *.
    let query = "CREATE MATERIALIZED VIEW all AS SELECT * FROM src";
    log::info!("materializing view=> {}", query);
    mz_client.query(&*query, &[]).await?;

    let query = "CREATE MATERIALIZED VIEW count AS SELECT COUNT(*) AS count FROM all";
    log::info!("materializing view=> {}", query);
    mz_client.query(&*query, &[]).await?;

    // Query it.
    let query = "SELECT * FROM count;";
    log::info!("querying view=> {}", query);
    loop {
        let row = mz_client::try_query_one(&mz_client, &*query, Duration::from_secs(1)).await?;
        let count: i64 = row.get("count");
        match count {
            c if c < message_count as i64 => continue,
            c if c > message_count as i64 => {
                return Err(anyhow::Error::msg(format!(
                    "Expected {} rows, found {}",
                    message_count, c,
                )));
            }
            c if c == message_count as i64 => {
                log::info!(
                    "found all {} records, generating md5 hash...",
                    message_count
                );

                let query = "SELECT data, mz_offset FROM all ORDER BY mz_offset;";
                let rows = mz_client::try_query(&mz_client, query, Duration::from_secs(1)).await?;
                if message_count != rows.len() {
                    return Err(anyhow::Error::msg(format!(
                        "Expected {} rows, found {}",
                        message_count,
                        rows.len()
                    )));
                }

                assert_eq!(message_count, rows.len());
                let mut hasher = Md5::new();
                for row in rows {
                    let val: &[u8] = row.get("data");
                    hasher.input(&val);
                }
                return Ok(format!("{:x}", hasher.result()));
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, StructOpt)]
pub struct Args {
    /// The materialized host
    #[structopt(long, default_value = "materialized")]
    pub materialized_host: String,

    /// The materialized port
    #[structopt(long, default_value = "6875")]
    pub materialized_port: u16,

    /// The Kafka URL
    #[structopt(long, default_value = "kafka:9092")]
    pub kafka_url: String,

    /// Number of Kafka partitions
    #[structopt(long, default_value = "1")]
    pub kafka_partitions: i32,

    /// The total number of records to create
    #[structopt(long, default_value = "10_000")]
    pub message_count: usize,
}
