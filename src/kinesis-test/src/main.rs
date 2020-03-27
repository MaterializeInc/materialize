// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test Kinesis
//!
//! Questions to answer:
//!     - What is tokio?
//!     - How do runtimes work?
//!

use std::error::Error as _;

use structopt::StructOpt;

use crate::config::{Args, KinesisConfig, MzConfig};
use crate::error::Result;

mod config;
mod error;
mod mz_client;

#[tokio::main]
async fn main() {
    println!("hi");
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
    let mut config = Args::from_args();
    if let None = config.seed {
        config.seed = Some(rand::random());
    };

    dbg!(&config);
    env_logger::init();

    log::info!(
        "starting up mzd={}:{} kinesis={}",
        config.materialized_host,
        config.materialized_port,
        config.kinesis(),
    );

    let k_config = config.kinesis_config();
    let mz_config = config.mz_config();

    dbg!(&k_config);
    dbg!(&mz_config);
    Ok(())
}