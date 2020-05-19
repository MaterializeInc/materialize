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

use std::error::Error as _;
use std::sync::Arc;
use std::process;
use std::time::Duration;

use rand::Rng;
use protobuf::Message;
use structopt::StructOpt;

use crate::config::{Args, KafkaConfig, MzConfig};
use crate::error::Result;

mod config;
mod error;
mod gen;
mod kafka_client;
mod macros;
mod mz_client;
mod proto;
mod randomizer;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        println!("ERROR: {}", e);
        let mut err = e.source();
        while let Some(e) = err {
            println!("    caused by: {}", e);
            err = e.source();
        }
        process::exit(1);
    }
}

async fn run() -> Result<()> {
    let config = Args::from_args();
    env_logger::init();

    log::info!(
        "starting up message_count={} mzd={}:{} kafka={} preserve_source={}",
        config.message_count,
        config.materialized_host,
        config.materialized_port,
        config.kafka_url(),
        config.preserve_source
    );

    let k_config = config.kafka_config();
    let mz_config = config.mz_config();

    let k = tokio::spawn(async move { create_kafka_messages(k_config).await });

    let mz = tokio::spawn(async move { create_materialized_source(mz_config).await });
    let (k_res, mz_res) = futures::join!(k, mz);
    k_res??;
    mz_res??;
    Ok(())
}

async fn create_kafka_messages(config: KafkaConfig) -> Result<()> {
    use rand::SeedableRng;
    let rng = rand::rngs::StdRng::from_seed(rand::random());

    let val_a: Vec<u8> = "a".repeat(500).into_bytes();
    let val_b: Vec<u8> = "b".repeat(500).into_bytes();

    let k_client =
        Arc::new(kafka_client::KafkaClient::new(&config.url, &config.group_id, &config.topic)?);

    if let Some(partitions) = config.partitions {
        k_client.create_topic(partitions).await?;
    }

    loop {
        log::info!("producing 8k records");
        let backoff = tokio::time::delay_for(Duration::from_secs(1));
        for i in 0..8000 {
            let key: i32 = rand::thread_rng().gen_range(0, 100000);
            let res = k_client.send(key.to_string().as_bytes(), val_a.as_slice());
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
        backoff.await;
    }
    Ok(())
}

async fn create_materialized_source(config: MzConfig) -> Result<()> {
    let client = mz_client::MzClient::new(&config.host, config.port).await?;

    if !config.preserve_source {
        let sources = client.show_sources().await?;
        if any_matches(&sources, config::KAFKA_SOURCE_NAME) {
            client.drop_source(config::KAFKA_SOURCE_NAME).await?;
        }
    }

    let sources = client.show_sources().await?;
    if !any_matches(&sources, config::KAFKA_SOURCE_NAME) {
        client
            .create_upsert_text_source(
                &config.kafka_url,
                &config.kafka_topic,
                config::KAFKA_SOURCE_NAME,
            )
            .await?;
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
