// Copyright Materialize, Inc. and contributors. All rights reserved.
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
//! <https://materialize.com/docs/demos/microservice/>

#![warn(missing_debug_implementations, missing_docs)]

use std::io;
use std::process;
use std::sync::Arc;

use anyhow::Result;
use prost::Message;
use tokio::time::{self, Duration};
use tracing::{error, info, trace};

use mz_ore::task;
use mz_test_util::kafka::kafka_client;
use mz_test_util::mz_client;

use crate::config::{Args, KafkaConfig, MzConfig};

mod config;
mod gen;
mod mz;
mod randomizer;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ERROR: {:#}", e);
        process::exit(1);
    }
}

async fn run() -> Result<()> {
    let config: Args = mz_ore::cli::parse_args();

    let k_config = config.kafka_config();
    let mz_config = config.mz_config();

    tracing_subscriber::fmt()
        .with_env_filter(config.log_filter)
        .with_writer(io::stderr)
        .init();

    info!(
        "starting up message_count={} mzd={}:{} kafka={}:{} preserve_source={} start_time={} seed={} enable_persistence={}",
        config.message_count,
        config.materialized_host,
        config.materialized_port,
        config.kafka_host,
        config.kafka_port,
        config.preserve_source,
        k_config.start_time.to_rfc3339(),
        k_config.seed,
        mz_config.enable_persistence,
    );

    let mz_client = mz_client::client(&mz_config.host, mz_config.port).await?;
    let check_sink = mz_config.check_sink;

    create_kafka_messages(k_config).await?;
    create_mz_objects(mz_config).await?;

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

    let k_client = Arc::new(kafka_client::KafkaClient::new(
        &config.url,
        &config.group_id,
        &[],
    )?);

    if let Some(create_topic) = &config.create_topic {
        k_client
            .create_topic(
                &config.topic,
                create_topic.partitions,
                create_topic.replication_factor,
                &[],
                None,
            )
            .await?;
    }

    let mut buf = vec![];
    let mut messages_remaining = config.message_count;
    while messages_remaining > 0 {
        let mut bytes_sent = 0;
        let backoff = time::sleep(Duration::from_secs(1));
        let messages_to_send = std::cmp::min(config.messages_per_second, messages_remaining);
        for _ in 0..messages_to_send {
            let m = randomizer::random_batch(rng, &mut recordstate);
            m.encode(&mut buf)?;
            trace!("sending: {:?}", m);
            let res = k_client.send(&config.topic, &buf);
            match res {
                Ok(fut) => {
                    task::spawn(|| "producer", fut);
                }
                Err(e) => {
                    error!("failed to produce message: {}", e);
                    time::sleep(Duration::from_millis(100)).await;
                }
            };
            bytes_sent += buf.len();
            buf.clear();
        }
        info!(
            "produced {} records ({} bytes / record) ({} remaining)",
            messages_to_send,
            bytes_sent / messages_to_send,
            messages_remaining,
        );
        messages_remaining -= messages_to_send;

        backoff.await;
    }

    Ok(())
}

async fn create_mz_objects(config: MzConfig) -> Result<()> {
    let client = mz_client::client(&config.host, config.port).await?;

    if !config.preserve_source {
        mz_client::drop_source(&client, config::KAFKA_SOURCE_NAME).await?;
        mz_client::drop_table(&client, config::PRICE_TABLE_NAME).await?;
        mz_client::drop_source(&client, config::REINGESTED_SINK_SOURCE_NAME).await?;
    }

    let sources = mz_client::show_sources(&client).await?;
    if !any_matches(&sources, config::KAFKA_SOURCE_NAME) {
        mz::create_price_table(
            &client,
            config::PRICE_TABLE_NAME,
            config.seed,
            randomizer::NUM_CLIENTS,
        )
        .await?;

        mz::create_proto_source(
            &client,
            &include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.pb"))[..],
            &config.kafka_url,
            &config.kafka_topic,
            config::KAFKA_SOURCE_NAME,
            "billing.Batch",
            config.enable_persistence,
        )
        .await?;

        mz::init_views(&client, config::KAFKA_SOURCE_NAME, config::PRICE_TABLE_NAME).await?;

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
        info!(
            "source '{}' already exists, not recreating",
            config::KAFKA_SOURCE_NAME
        );
    }
    Ok(())
}

fn any_matches(haystack: &[String], needle: &str) -> bool {
    haystack.iter().any(|s| s.contains(needle))
}
