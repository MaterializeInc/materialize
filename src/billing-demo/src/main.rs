// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Demonstrate sticking a whole lot of protobuf messages into materialized

#![deny(missing_debug_implementations, missing_docs)]

use std::error::Error as _;

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
    let rng = &mut rand::rngs::StdRng::from_seed(rand::random());

    let mut recordstate = randomizer::RecordState::new();

    let mut k_client = kafka_client::KafkaClient::new(&config.url, &config.group_id)?;

    let mut buf = vec![];
    let mut interval = config.message_sleep.map(|dur| tokio::time::interval(dur));
    let mut total_size = 0;
    for i in 0..config.message_count {
        if let Some(int) = interval.as_mut() {
            int.tick().await;
        }
        let m = randomizer::random_batch(rng, &mut recordstate);
        m.write_to_vec(&mut buf)?;
        log::trace!("sending: {:?}", m);
        k_client.send(&config.topic, &buf).await?;
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
    let client = mz_client::MzClient::new(&config.host, config.port).await?;

    if !config.preserve_source {
        let sources = client.show_sources().await?;
        if any_matches(&sources, config::KAFKA_SOURCE_NAME) {
            client.drop_source(config::KAFKA_SOURCE_NAME).await?;
            client.drop_source(config::CSV_SOURCE_NAME).await?;
        }
    }

    let sources = client.show_sources().await?;
    if !any_matches(&sources, config::KAFKA_SOURCE_NAME) {
        client
            .create_csv_source(
                &config.csv_file_name,
                config::CSV_SOURCE_NAME,
                randomizer::NUM_CLIENTS,
            )
            .await?;

        client
            .create_proto_source(
                proto::BILLING_DESCRIPTOR,
                &config.kafka_url,
                &config.kafka_topic,
                config::KAFKA_SOURCE_NAME,
                proto::BILLING_MESSAGE_NAME,
            )
            .await?;

        exec_query!(client, "billing_raw_data");
        exec_query!(client, "billing_prices");
        exec_query!(client, "billing_batches");
        exec_query!(client, "billing_records");
        exec_query!(client, "billing_agg_by_minute");
        exec_query!(client, "billing_agg_by_hour");
        exec_query!(client, "billing_agg_by_day");
        exec_query!(client, "billing_agg_by_month");
        exec_query!(client, "billing_monthly_statement");
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
