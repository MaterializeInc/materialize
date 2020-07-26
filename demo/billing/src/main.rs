// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Microservice demo using materialized to build a real-time billing application
//!
//! Specifically, this demo shows off materialized's ability to ingest Protobuf
//! messages, normalize incoming data with jsonb functions, perform joins between
//! a Kafka topic and a local file, and perform time based aggregates.
//!
//! Further details can be found on the Materialize docs:
//! <https://materialize.io/docs/demos/microservice/>

#![deny(missing_debug_implementations, missing_docs)]

use std::process;
use std::time::Duration;

use anyhow::Result;
use protobuf::Message;
use structopt::StructOpt;

use test_util::kafka::kafka_client;
use test_util::mz_client;

use crate::config::{Args, KafkaConfig, MzConfig};

mod config;
mod gen;
mod mz;
mod proto;
mod randomizer;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ERROR: {:#}", e);
        process::exit(1);
    }
}

async fn run() -> Result<()> {
    let config = Args::from_args();
    env_logger::init();

    let k_config = config.kafka_config();
    let mz_config = config.mz_config();

    log::info!(
        "starting up message_count={} mzd={}:{} kafka={} preserve_source={} start_time={} seed={}",
        config.message_count,
        config.materialized_host,
        config.materialized_port,
        config.kafka_url(),
        config.preserve_source,
        k_config.start_time.to_rfc3339(),
        k_config.seed,
    );

    let mz_client = mz_client::client(&mz_config.host, mz_config.port).await?;
    let check_sink = mz_config.check_sink;

    let k = tokio::spawn(async move { create_kafka_messages(k_config).await });

    let mz = tokio::spawn(async move { create_materialized_source(mz_config).await });
    let (k_res, mz_res) = futures::join!(k, mz);
    k_res??;
    mz_res??;

    if check_sink {
        mz::validate_sink(
            &mz_client,
            "check_sink",
            "billing_monthly_statement",
            "invalid_sink_rows",
        )
        .await?;
    }
    Ok(())
}

async fn create_kafka_messages(config: KafkaConfig) -> Result<()> {
    use rand::SeedableRng;
    let rng = &mut rand::rngs::StdRng::seed_from_u64(config.seed);

    let mut recordstate = randomizer::RecordState {
        last_time: config.start_time,
    };

    let mut k_client =
        kafka_client::KafkaClient::new(&config.url, &config.group_id, &config.topic, &[])?;

    if let Some(partitions) = config.partitions {
        k_client
            .create_topic(partitions, 1, &[], Some(Duration::from_secs(5)))
            .await?
    }

    let mut buf = vec![];
    let mut interval = config.message_sleep.map(tokio::time::interval);
    let mut total_size = 0;
    for i in 0..config.message_count {
        if let Some(int) = interval.as_mut() {
            int.tick().await;
        }
        let m = randomizer::random_batch(rng, &mut recordstate);
        m.write_to_vec(&mut buf)?;
        log::trace!("sending: {:?}", m);
        k_client.send(&buf).await?;
        total_size += buf.len();
        buf.clear();
        if i % (config.message_count / 100).max(5) == 0 {
            log::info!(
                "sent message {} average message size: {}B",
                i,
                total_size / i.max(1)
            );
        }
    }
    Ok(())
}

async fn create_materialized_source(config: MzConfig) -> Result<()> {
    let client = mz_client::client(&config.host, config.port).await?;

    if !config.preserve_source {
        mz_client::drop_source(&client, config::KAFKA_SOURCE_NAME).await?;
        mz_client::drop_source(&client, config::CSV_SOURCE_NAME).await?;
        mz_client::drop_source(&client, config::REINGESTED_SINK_SOURCE_NAME).await?;
    }

    let sources = mz_client::show_sources(&client).await?;
    if !any_matches(&sources, config::KAFKA_SOURCE_NAME) {
        mz::create_csv_source(
            &client,
            &config.csv_file_name,
            config::CSV_SOURCE_NAME,
            randomizer::NUM_CLIENTS,
            config.seed,
            config.batch_size,
        )
        .await?;

        mz::create_proto_source(
            &client,
            proto::BILLING_DESCRIPTOR,
            &config.kafka_url,
            &config.kafka_topic,
            config::KAFKA_SOURCE_NAME,
            proto::BILLING_MESSAGE_NAME,
            config.batch_size,
        )
        .await?;

        mz::init_views(&client, config::KAFKA_SOURCE_NAME, config::CSV_SOURCE_NAME).await?;

        if config.low_memory {
            mz::drop_indexes(&client).await?;
        }
        let sink_topic = mz::create_kafka_sink(
            &client,
            &config.kafka_url,
            config::KAFKA_SINK_TOPIC_NAME,
            config::KAFKA_SINK_NAME,
            &config.schema_registry_url,
        )
        .await?;
        if config.check_sink {
            mz::reingest_sink(
                &client,
                &config.kafka_url,
                &config.schema_registry_url,
                config::REINGESTED_SINK_SOURCE_NAME,
                &sink_topic,
            )
            .await?;
            mz::init_sink_views(&client, config::REINGESTED_SINK_SOURCE_NAME).await?;
        }
    } else {
        log::info!(
            "source '{}' already exists, not recreating",
            config::KAFKA_SOURCE_NAME
        );
    }
    Ok(())
}

fn any_matches(haystack: &[String], needle: &str) -> bool {
    haystack.iter().any(|s| s.contains(needle))
}
