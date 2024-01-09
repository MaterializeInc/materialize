// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process;

use anyhow::Context;
use mz_interchange::avro::Decoder;
use mz_interchange::confluent;
use mz_ore::cli;
use mz_ore::cli::CliConfig;
use mz_ore::error::ErrorExt;
use tokio::fs;

/// Decode a single Avro row using Materialize's Avro decoder.
#[derive(clap::Parser)]
struct Args {
    /// The path to a file containing the raw bytes of a single Avro datum.
    data_file: PathBuf,
    /// The path to a file containing the Avro schema.
    schema_file: PathBuf,
    /// Whether the data file uses the Confluent wire format.
    #[clap(long)]
    confluent_wire_format: bool,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig::default());
    if let Err(e) = run(args).await {
        println!("{}", e.display_with_causes());
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let mut data = &*fs::read(&args.data_file)
        .await
        .context("reading data file")?;
    if args.confluent_wire_format {
        let (schema_id, adjusted_data) = confluent::extract_avro_header(data)?;
        data = adjusted_data;
        println!("schema id: {schema_id}");
    }
    let schema = fs::read_to_string(&args.schema_file)
        .await
        .context("reading schema file")?;
    let ccsr_client: Option<mz_ccsr::Client> = None;
    let debug_name = String::from("avro-decode");
    let confluent_wire_format = false;
    let mut decoder = Decoder::new(&schema, ccsr_client, debug_name, confluent_wire_format)
        .context("creating decoder")?;
    let row = decoder.decode(&mut data).await.context("decoding data")?;
    println!("row: {row:?}");
    Ok(())
}
