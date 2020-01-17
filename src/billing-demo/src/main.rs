// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Demonstrate sticking a whole lot of protobuf messages into materialized

#![deny(missing_debug_implementations, missing_docs)]

use std::error::Error as _;

use prost::Message;
use structopt::StructOpt;
use tokio::runtime;

use crate::config::{Args, KafkaConfig, MzConfig};
use crate::error::Result;

mod config;
mod error;
mod kafka_client;
mod macros;
mod mz_client;
mod proto;
mod randomizer;

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut rt = runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .enable_time()
        .build()?;
    rt.block_on(async {
        if let Err(e) = run().await {
            println!("ERROR: {}", e);
            let mut err = e.source();
            while let Some(e) = err {
                println!("    caused by: {}", e);
                err = e.source();
            }
        }
    });

    Ok(())
}

async fn run() -> Result<()> {
    let config = Args::from_args();
    env_logger::init();

    log::info!(
        "starting up message_count={} mzd={}:{} kafka={}",
        config.message_count,
        config.materialized_host,
        config.materialized_port,
        config.kafka_url()
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
    let rng = &mut rand::rngs::StdRng::from_seed(rand::random());

    let mut recordstate = randomizer::RecordState::new();

    let mut k_client = kafka_client::KafkaClient::new(&config.url, &config.group_id)?;

    let mut buf = vec![];
    let mut interval = config.message_sleep.map(|dur| tokio::time::interval(dur));
    for i in 0..config.message_count {
        if let Some(int) = interval.as_mut() {
            int.tick().await;
        }
        let m = randomizer::random_batch(rng, &mut recordstate);
        m.encode(&mut buf)?;
        log::trace!("sending: {:?}", m);
        k_client.send(&config.topic, &buf).await?;
        buf.clear();
        if i % (config.message_count / 100).max(5) == 0 {
            log::info!("sent message {}", i);
        }
    }
    Ok(())
}

async fn create_materialized_source(config: MzConfig) -> Result<()> {
    let client = mz_client::MzClient::new(&config.host, config.port).await?;

    if !config.preserve_source {
        let sources = client.show_sources().await?;
        if any_matches(&sources, &config.source_name) {
            client.drop_source(&config.source_name).await?;
        }
    }

    let sources = client.show_sources().await?;
    if !any_matches(&sources, &config.source_name) {
        client
            .create_proto_source(
                proto::BILLING_DESCRIPTOR,
                &config.kafka_url,
                &config.kafka_topic,
                &config.source_name,
                proto::BILLING_MESSAGE_NAME,
            )
            .await?;
        let q = format!(
            "CREATE VIEW {} AS SELECT id, interval_start, interval_end FROM {}",
            config.view_name, config.source_name
        );
        if let Err(e) = client.execute(&q, &[]).await {
            log::error!("{} executing query {}", e, q);
        }
        // exec_query!(client, config, "billing_records");
        // exec_query!(client, config, "billing_measurements");
        // exec_query!(client, "sum_by_minute");
        // exec_query!(client, "sum_by_hour");
        exec_query!(client, config, "sum_by_minute_thin");
    } else {
        log::info!(
            "source '{}' already exists, not recreating",
            config.source_name
        );
    }
    Ok(())
}

fn any_matches(haystack: &[String], needle: &str) -> bool {
    haystack.iter().find(|s| s.contains(needle)).is_some()
}
