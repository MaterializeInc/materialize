// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Add;
use std::str;
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

    match args.test {
        Tests::BytesToKafka => bytes_to_kafka(args).await?,
        Tests::MysqlDebeziumKafka => mysql_debezium_kafka(args).await?,
    }

    log::info!(
        "Completed chaos testing in {} milliseconds",
        timer.elapsed().as_millis()
    );
    Ok(())
}

async fn mysql_debezium_kafka(args: Args) -> Result<(), anyhow::Error> {
    let run_seconds = args.run_seconds.unwrap_or(864_000);
    let end = std::time::Instant::now().add(Duration::from_secs(run_seconds));
    log::info!(
        "starting chaos test {:#?} with mzd={}:{} kafka={} run seconds={}",
        args.test,
        args.materialized_host,
        args.materialized_port,
        args.kafka_url,
        run_seconds,
    );

    let mz_client = mz_client::client(&args.materialized_host, args.materialized_port).await?;

    // Create Kafka source.
    let src_query = "CREATE SOURCE src_orderline
                           FROM KAFKA BROKER 'kafka:9092' TOPIC 'debezium.tpcch.orderline'
                           FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
                           ENVELOPE DEBEZIUM;";
    log::info!("creating source=> {}", src_query);
    // Retry in case the topic has not been created yet.
    retry::retry_for(Duration::from_secs(30), |_| {
        mz_client.execute(&*src_query, &[])
    })
    .await?;

    // Create count materialized view.
    let count_query = "CREATE MATERIALIZED VIEW orderline_count AS
                             SELECT count(*) FROM src_orderline;";
    log::info!("creating view=> {}", count_query);
    mz_client.execute(&*count_query, &[]).await?;

    // Create Q01 materialized view.
    let qo1_query = "CREATE MATERIALIZED VIEW q01 AS
                           SELECT
                               ol_number,
                               sum(ol_quantity) AS sum_qty,
                               sum(ol_amount) AS sum_amount,
                               avg(ol_quantity) AS avg_qty,
                               avg(ol_amount) AS avg_amount,
                               count(*) AS count_order
                           FROM src_orderline
                           WHERE ol_delivery_d > TIMESTAMP '2007-01-02 00:00:00.000000'
                           GROUP BY ol_number
                           ORDER BY ol_number;";
    log::info!("creating view=> {}", qo1_query);
    mz_client.execute(&*qo1_query, &[]).await?;

    // Query both materialized views until run_seconds has expired, raise any errors.
    while std::time::Instant::now() < end {
        let count = "SELECT * FROM orderline_count;";
        mz_client::try_query_one(&mz_client, &*count, Duration::from_secs(1)).await?;

        let q01 = "SELECT * FROM q01;";
        mz_client::try_query(&mz_client, &q01, Duration::from_secs(1)).await?;
    }

    Ok(())
}

async fn bytes_to_kafka(args: Args) -> Result<(), anyhow::Error> {
    let seed: u32 = rand::thread_rng().gen();
    let topic = format!("chaos-{}", seed);
    let message_count = args.message_count.unwrap_or(10_000);
    log::info!(
        "starting chaos test {:#?} with mzd={}:{} kafka={} topic={} message_count={}",
        args.test,
        args.materialized_host,
        args.materialized_port,
        args.kafka_url,
        topic,
        message_count,
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
        kafka_client.create_topic(
            args.kafka_partitions.unwrap_or(1),
            1,
            &[],
            Some(Duration::from_secs(10)),
        )
    })
    .await?;

    let kafka_task = tokio::spawn({
        async move { generate_and_push_records(kafka_client, message_count).await }
    });
    let materialize_task = tokio::spawn({
        let materialized_host = args.materialized_host.clone();
        let materialized_port = args.materialized_port;
        let kafka_url = args.kafka_url.clone();
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

    Ok(())
}

/// Generate byte records, push them to Kafka, and return their md5 hash.
async fn generate_and_push_records(
    kafka_client: kafka::kafka_client::KafkaClient,
    message_count: usize,
) -> Result<String, anyhow::Error> {
    log::info!("pushing {} records to Kafka", message_count);
    let ten_percent = message_count / 10;
    let mut sent = 0;
    let mut records = Vec::new();
    while sent < message_count {
        if sent % ten_percent == 0 {
            log::info!("have sent {} records...", sent);
        }

        let record = generator::bytes::generate_bytes(30);
        match kafka_client.send(&record)?.await? {
            Ok(_) => {
                sent += 1;
                records.push(record);
            }
            Err((e, _)) => {
                log::error!("failed to send bytes to Kafka: {}", e);
            }
        }
    }

    records.sort();
    let mut hasher = Md5::new();
    for r in records {
        hasher.update(&r);
    }

    Ok(format!("{:x}", hasher.finalize()))
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

                let mut offset: usize = 0;
                let limit: usize = 10_000;
                let mut vals = Vec::new();
                loop {
                    let query = format!(
                        "SELECT data FROM all ORDER BY data LIMIT {} OFFSET {};",
                        limit, offset
                    );
                    let rows =
                        mz_client::try_query(&mz_client, &query, Duration::from_secs(1)).await?;
                    if rows.len() == 0 {
                        break;
                    }
                    offset += rows.len();
                    for row in rows {
                        let val: &[u8] = row.get("data");
                        vals.push(val.to_owned());
                    }
                }
                if vals.len() != message_count {
                    return Err(anyhow::Error::msg(format!(
                        "Expected {} rows, found {}",
                        message_count,
                        vals.len()
                    )));
                }

                vals.sort();

                let mut hasher = Md5::new();
                for val in vals {
                    hasher.update(&val);
                }
                return Ok(format!("{:x}", hasher.finalize()));
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Tests {
    BytesToKafka,
    MysqlDebeziumKafka,
}

impl str::FromStr for Tests {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "bytes-to-kafka" => Ok(Tests::BytesToKafka),
            "mysql-debezium-kafka" => Ok(Tests::MysqlDebeziumKafka),
            _ => Err(anyhow::anyhow!("could not parse test: {}", s)),
        }
    }
}

#[derive(Clone, Debug, StructOpt)]
pub struct Args {
    /// The specific types of chaos test to run
    #[structopt(long)]
    pub test: Tests,

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
    #[structopt(long)]
    pub kafka_partitions: Option<i32>,

    /// The total number of records to create
    #[structopt(long)]
    pub message_count: Option<usize>,

    /// The number of seconds to run
    #[structopt(long)]
    pub run_seconds: Option<u64>,
}
