// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// ! todo: Add something real here

use std::error::Error as _;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use futures::executor::block_on;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::StaticProvider;
use rusoto_kinesis::{
    CreateStreamInput, DeleteStreamInput, Kinesis, KinesisClient, PutRecordError, PutRecordInput,
};
use structopt::StructOpt;

use crate::config::Args;
use crate::error::Result as SecretResult;

mod config;
mod error;

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

async fn run() -> SecretResult<()> {
    let mut config = Args::from_args();
    if let None = config.seed {
        config.seed = Some(rand::random());
    };
    match config.seed {
        Some(seed) => {
            config.kinesis_stream_name = format!("{}-{}", config.kinesis_stream_name, seed);
        }
        None => panic!("this shouldn't happen"),
    }
    env_logger::init();

    log::info!(
        "starting up mzd={}:{}, kinesis stream name={}",
        config.materialized_host,
        config.materialized_port,
        config.kinesis_stream_name,
    );

    let k_config = config.kinesis_config();

    // Create a new KinesisClient
    let provider = StaticProvider::new(k_config.access_key, k_config.secret_access_key, None, None);
    let request_dispatcher = HttpClient::new().unwrap();
    let region: Region = k_config.region.parse().unwrap();
    let client = KinesisClient::new_with(request_dispatcher, provider, region);
    instantiate_kinesis_for_source(&client, &k_config.stream_name)?;

    thread::sleep(Duration::from_secs(5));
    // todo: Create a Kinesis source. Confirm record matches.

    delete_kinesis_stream(&client, &k_config.stream_name)?;

    Ok(())
}

/// Creates a new Kinesis stream and puts a record into it.
fn instantiate_kinesis_for_source(
    client: &KinesisClient,
    stream_name: &String,
) -> SecretResult<()> {
    // Create a new Kinesis stream
    log::info!("creating a new Kinesis stream: {}", stream_name);
    let create_stream_input = CreateStreamInput {
        shard_count: 1,
        stream_name: stream_name.clone(),
    };
    if let Err(e) = block_on(client.create_stream(create_stream_input)) {
        panic!("hit error creating stream: {}", e.to_string());
    }

    log::info!("trying to put a record into the stream");
    let test_string = String::from("test record");
    let put_input = PutRecordInput {
        data: Bytes::from(test_string.clone()),
        explicit_hash_key: None,
        partition_key: String::from("test"), // doesn't matter, only one shard
        sequence_number_for_ordering: None,
        stream_name: stream_name.clone(),
    };

    // The stream might not be immediately available to put records
    // into. Try a few times.
    // todo: have some sort of upper limit for failure here.
    let mut put_record = false;
    while !put_record {
        match block_on(client.put_record(put_input.clone())) {
            Ok(_output) => put_record = true,
            Err(RusotoError::Service(PutRecordError::ResourceNotFound(err_string))) => {
                log::info!("{} trying again in 1 second...", err_string);
                thread::sleep(Duration::from_secs(1));
            }
            Err(err) => {
                panic!("unable to put Kinesis record: {}", err.to_string());
            }
        }
    }
    log::info!("put a record into the stream");

    // Required Kinesis stream has been instantiated.
    Ok(())
}

fn delete_kinesis_stream(client: &KinesisClient, stream_name: &String) -> SecretResult<()> {
    log::info!("Trying to delete the stream: {}", stream_name);

    let delete_input = DeleteStreamInput {
        enforce_consumer_deletion: Some(true),
        stream_name: stream_name.clone(),
    };
    match block_on(client.delete_stream(delete_input)) {
        Ok(()) => {
            log::info!("Successfully deleted the stream!");
        }
        Err(err) => {
            panic!("Unable to delete stream: {}", err.to_string());
        }
    }

    Ok(())
}
