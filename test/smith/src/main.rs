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

use std::error::Error as _;
use std::process;

use structopt::StructOpt;
use urlencoding::encode;

use crate::config::{Args, FuzzerConfig};
use crate::error::Result;

mod config;
mod error;
mod macros;
mod mz_client;

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

    let fuzz = tokio::spawn(async move { send_queries(fuzzer_config).await });
    fuzz.await??;
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

    while remaining > 0 {
        // Get information about current materialized views
        let views_json = client.show_views().await?;
        let views = encode(&views_json);

        let count = if remaining > 50 { 50 } else { remaining };
        let body = format!("database=materialize&count={}&tables={}", count, views);

        let request = http_client
            .post(config.fuzzer_url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .build()?;

        log::info!("querying fuzzer: {:?}", request);
        let response = http_client.execute(request).await?;
        log::info!("received response: {:?}", response);
        let text = response.text().await?;

        let response_json: serde_json::Value =
            serde_json::from_str(&text).expect("parsing response json failed");

        let queries = response_json
            .get("queries")
            .expect("response should contain a set of queries");

        match queries {
            serde_json::Value::Array(v) => {
                for query in v.iter() {
                    let qs = query.to_string();
                    let sanitized = qs.trim_matches('"');
                    if let Err(e) = client.execute(sanitized, &[]).await {
                        if !sanitized.starts_with("INSERT INTO") {
                            log::error!(
                                "{} ({}) executing query {}",
                                e,
                                e.source().unwrap(),
                                sanitized
                            );
                        }
                    }
                }
            }
            _ => panic!("expected an array of queries"),
        }

        remaining -= count;
    }
    Ok(())
}
