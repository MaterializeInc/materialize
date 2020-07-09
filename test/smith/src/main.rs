// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz test Materialize using the Smith fuzzer
//! available at https://api.jibson.dev/smith

#![deny(missing_debug_implementations, missing_docs)]

use std::process;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use crate::config::{Args, FuzzerConfig};

mod config;
mod macros;
mod mz_client;

#[derive(Debug, Serialize)]
struct QueryRequest {
    database: &'static str,
    count: i64,
    tables: String,
    prev: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QueryResponse {
    queries: Vec<String>,
    next: String,
}

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
        "starting up query_count={} mzd={}:{} fuzzer_url={}",
        config.query_count,
        config.materialized_host,
        config.materialized_port,
        config.fuzzer_url,
    );

    let fuzzer_config = config.fuzzer_config();

    send_queries(fuzzer_config).await?;
    Ok(())
}

async fn send_queries(config: FuzzerConfig) -> Result<()> {
    let client = mz_client::MzClient::new(&config.mz_config.host, config.mz_config.port).await?;

    // Seed Materialize with some starting views
    exec_query!(client, "view1");
    exec_query!(client, "view2");

    // Get fuzzer inputs from Smith
    let http_client = reqwest::Client::new();
    let mut remaining: i64 = config.query_count as i64;
    let mut prev = None;

    while remaining > 0 {
        let count = if remaining > 500 { 500 } else { remaining };

        let request = http_client
            .post(config.fuzzer_url.clone())
            .form(&QueryRequest {
                database: "materialize",
                count,
                tables: serde_json::to_string(&client.show_views().await?)?,
                prev,
            })
            .build()?;

        log::info!("querying fuzzer: {:?}", request);
        let response = http_client.execute(request).await?;
        log::info!("received response: {:?}", response);

        let response: QueryResponse = response.json().await?;

        prev = Some(response.next);
        for query in response.queries.iter() {
            if let Err(e) = client.execute(query, &[]).await {
                if !query.starts_with("INSERT INTO") {
                    log::error!("{} ({}) executing query {}", e, e.source().unwrap(), query);
                }
            }

            // Perform a heartbeat via a simple request tp make sure Materialize is still up
            client.execute("SELECT 1", &[]).await?;
        }

        remaining -= count;
    }
    Ok(())
}
