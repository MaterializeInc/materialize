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
        "Starting chaos test with mzd={}:{} kafka={} topic={} message_count={}",
        args.materialized_host,
        args.materialized_port,
        args.kafka_url,
        topic,
        args.message_count
    );

    // Create Kafka topic.
    log::info!("creating Kafka topic=> {}", topic);
    let mut kafka_client =
        kafka::kafka_client::KafkaClient::new(&args.kafka_url, "materialize.chaos", &topic)?;
    retry::retry_for(Duration::from_secs(10), |_| {
        kafka_client.create_topic(args.kafka_partitions)
    })
    .await?;

    // Generate messages and md5 hash.
    log::info!("creating {} records", args.message_count);
    let mut hasher = Md5::new();
    for _ in 0..args.message_count {
        let record = generator::bytes::generate_bytes(30);
        hasher.input(&record);
        kafka_client.send(&record).await?;
    }
    let expected_hashed = format!("{:x}", hasher.result());

    // Set up Materialize.
    let mz_client = mz_client::client(&args.materialized_host, args.materialized_port).await?;

    // Create views and query thread.
    let query = format!(
        "CREATE SOURCE src FROM KAFKA BROKER 'kafka:9092' TOPIC '{}' FORMAT BYTES",
        topic
    );
    log::info!("creating source=> {}", query);
    mz_client.execute(&*query, &[]).await?;

    // Materialize two views: a count and a select *.
    let query = "CREATE MATERIALIZED VIEW all AS SELECT * FROM src";
    log::info!("materializing view=> {}", query);
    mz_client.query(&*query, &[]).await?;

    let query = "CREATE MATERIALIZED VIEW count AS SELECT COUNT(*) AS count FROM src";
    log::info!("materializing view=> {}", query);
    mz_client.query(&*query, &[]).await?;

    // Query it.
    let query = "SELECT * FROM count;";
    log::info!("querying view=> {}", query);
    loop {
        let row = mz_client::try_query_one(&mz_client, &*query, Duration::from_secs(1)).await?;
        let count: i64 = row.get("count");
        if count == args.message_count as i64 {
            log::info!(
                "found all {} records, comparing hashes...",
                args.message_count
            );

            let query = "SELECT data, mz_offset FROM all ORDER BY mz_offset;";
            let rows = mz_client::try_query(&mz_client, query, Duration::from_secs(1)).await?;
            assert_eq!(args.message_count, rows.len());
            let mut hasher = Md5::new();
            for row in rows {
                let val: &[u8] = row.get("data");
                hasher.input(&val);
            }

            let actual_hashed = format!("{:x}", hasher.result());
            if actual_hashed == expected_hashed {
                log::info!("input and output matched!!");
            } else {
                return Err(anyhow::anyhow!(
                    "wrong hash value: expected:{:?} got:{:?}",
                    expected_hashed,
                    actual_hashed
                ));
            }
            break;
        }
    }

    log::info!(
        "Completed chaos testing in {} milliseconds",
        timer.elapsed().as_millis()
    );
    Ok(())
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
    #[structopt(long, default_value = "1_000")]
    pub message_count: usize,
}
