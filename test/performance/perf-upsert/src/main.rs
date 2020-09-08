// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Performance test for Materialize Upsert sources

#![deny(missing_debug_implementations, missing_docs)]

use std::process;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rand::Rng;
use structopt::StructOpt;
use tokio_postgres::Client;

use test_util::kafka::kafka_client;
use test_util::mz_client;

use crate::config::{Args, KafkaConfig, MzConfig};

mod config;

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
        "starting up message_count={} mzd={}:{} kafka={} preserve_source={} enable_persistence={} num_peeks={}",
        config.message_count,
        config.materialized_host,
        config.materialized_port,
        config.kafka_url(),
        config.preserve_source,
        config.enable_persistence,
        config.num_peeks,
    );

    let k = tokio::spawn(async move { create_kafka_messages(k_config).await });
    let mz = tokio::spawn(async move { create_and_query_source(mz_config).await });
    let (k_res, mz_res) = futures::join!(k, mz);
    k_res??;
    mz_res??;

    Ok(())
}

async fn create_kafka_messages(config: KafkaConfig) -> Result<()> {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::from_seed(rand::random());
    let mut messages_remaining = config.message_count;

    let val_a: Vec<u8> = "a".repeat(500).into_bytes();

    let k_client = Arc::new(kafka_client::KafkaClient::new(
        &config.url,
        &config.group_id,
        &config.topic,
        &[],
    )?);

    if let Some(create_topic) = &config.create_topic {
        let configs = vec![
            ("cleanup.policy", "delete"),
            ("segment.ms", "86400000"),
            ("segment.bytes", "100000000"),
            ("delete.retention.ms", "86400000"),
        ];
        k_client
            .create_topic(
                create_topic.partitions,
                create_topic.replication_factor,
                &configs,
                None,
            )
            .await?;
    }

    while messages_remaining > 0 {
        let messages_to_send = std::cmp::min(config.messages_per_second, messages_remaining);
        log::info!("producing {} records", messages_to_send);
        let backoff = tokio::time::delay_for(Duration::from_secs(1));
        for i in 0..messages_to_send {
            // Artificially create a skewed distribution of keys where 33% of
            // inserts to the topic are from keys drawn uniformly from [0, 10_000)
            // and the remaining 67% of the time inserts come from keys drawn
            // uniformly from [0, 10_000_000). Effectively, 0.1% of possible keys
            // are "hot" and present in the topic ~500 times more frequently
            // than the "cold" keys.
            let key: i32 = if i % 3 != 0 {
                rng.gen_range(0, 10_000)
            } else {
                rng.gen_range(0, 10_000_000)
            };
            let res = k_client.send_key_value(key.to_string().as_bytes(), val_a.as_slice());
            match res {
                Ok(fut) => {
                    tokio::spawn(fut);
                }
                Err(e) => {
                    log::error!("failed to produce message: {}", e);
                    tokio::time::delay_for(Duration::from_millis(100)).await;
                }
            }
        }

        messages_remaining -= messages_to_send;
        backoff.await;
    }
    Ok(())
}

async fn create_upsert_text_source(
    mz_client: &Client,
    kafka_url: &impl std::fmt::Display,
    kafka_topic_name: &str,
    source_name: &str,
    enable_persistence: bool,
) -> Result<()> {
    let query_prefix = format!(
        "CREATE MATERIALIZED SOURCE {source} FROM KAFKA BROKER '{kafka_url}' TOPIC '{topic}'",
        kafka_url = kafka_url,
        topic = kafka_topic_name,
        source = source_name,
    );

    let query = if enable_persistence {
        format!(
            "{} WITH (persistence = true) FORMAT TEXT ENVELOPE UPSERT",
            query_prefix
        )
    } else {
        format!("{} FORMAT TEXT ENVELOPE UPSERT", query_prefix)
    };
    log::debug!("creating source=> {}", query);

    mz_client.execute(&*query, &[]).await?;

    Ok(())
}

async fn create_and_query_source(config: MzConfig) -> Result<()> {
    let client = mz_client::client(&config.host, config.port).await?;

    if !config.preserve_source {
        mz_client::drop_source(&client, config::KAFKA_SOURCE_NAME).await?;
    }

    let sources = mz_client::show_sources(&client).await?;
    if !any_matches(&sources, config::KAFKA_SOURCE_NAME) {
        create_upsert_text_source(
            &client,
            &config.kafka_url,
            &config.kafka_topic,
            config::KAFKA_SOURCE_NAME,
            config.enable_persistence,
        )
        .await?;
    } else {
        log::info!(
            "source '{}' already exists, not recreating",
            config::KAFKA_SOURCE_NAME
        );
    }

    let peek = format!("SELECT count(*) from {}", config::KAFKA_SOURCE_NAME);
    for i in 0..config.num_peeks {
        if i % 1000 == 0 {
            log::info!("running query {}", i);
        }

        if let Err(e) = client.execute(&*peek, &[]).await {
            log::error!("failed to produce message: {}", e);
            tokio::time::delay_for(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

fn any_matches(haystack: &[String], needle: &str) -> bool {
    haystack.iter().any(|s| s.contains(needle))
}
