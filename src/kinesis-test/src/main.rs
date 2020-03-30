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
    CreateStreamInput, DeleteStreamInput, DescribeStreamInput, Kinesis, KinesisClient,
    PutRecordError, PutRecordInput,
};
use structopt::StructOpt;

use crate::config::Args;
use crate::error::Result;

mod config;
mod error;
mod mz_client;

static TEST_RECORD_STRING: &str = "TEST RECORD STRING";

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
    let mut args: Args = Args::from_args();
    let seed = match args.seed {
        Some(seed) => seed,
        None => {
            let new_seed = rand::random();
            args.seed = Some(new_seed);
            new_seed
        }
    };
    // Reset Kinesis stream name to include seed.
    args.kinesis_stream_name = format!("{}-{}", args.kinesis_stream_name, seed);

    env_logger::init();
    log::info!(
        "starting up mzd={}:{}, kinesis stream name={}",
        args.materialized_host,
        args.materialized_port,
        args.kinesis_stream_name,
    );

    // Create a new KinesisClient
    let provider = StaticProvider::new(
        args.kinesis_access_key.clone(),
        args.kinesis_secret_access_key.clone(),
        None,
        None,
    );
    let request_dispatcher = HttpClient::new().unwrap();
    let region: Region = args.kinesis_region.clone().parse().unwrap();
    let client = KinesisClient::new_with(request_dispatcher, provider, region);

    // Create Kinesis stream and push a test record
    let stream_arn = instantiate_kinesis_for_source(&client, &args.kinesis_stream_name)?;

    // Test that Materialize can connect to and query a Kinesis source
    test_materialize_source(&args, &stream_arn)?;

    // Delete the newly created Kinesis stream
    delete_kinesis_stream(&client, &args.kinesis_stream_name)?;

    Ok(())
}

fn test_materialize_source(config: &Args, stream_arn: &str) -> Result<()> {
    // Create client to connect to Materialize instance
    let client = match block_on(mz_client::MzClient::new(
        &config.materialized_host,
        config.materialized_port,
    )) {
        Ok(client) => client,
        Err(err) => panic!("hit error creating client: {}", err.to_string()),
    };

    // Create a Kinesis source from the newly created stream
    if let Err(e) = block_on(client.create_kinesis_source(
        &config.kinesis_source_name,
        stream_arn,
        &config.kinesis_access_key,
        &config.kinesis_secret_access_key,
    )) {
        panic!(
            "hit error creating Kinesis source in Materialize: {}",
            e.to_string()
        );
    }

    // Create a materialized view from the Kinesis source
    if let Err(e) = block_on(client.create_view(&config.kinesis_source_name)) {
        panic!("hit error creating materialized view: {}", e.to_string());
    }

    // Query the materialized view
    match block_on(client.query_view(&config.kinesis_source_name)) {
        Ok(result) => {
            assert_eq!(1, result.len());
            let row: &str = result[0].get("data"); // Column is automatically named "data"
            assert_eq!(row, TEST_RECORD_STRING);
        }
        Err(e) => panic!("hit error querying materialized view: {}", e.to_string()),
    };

    // Drop the Kinesis source.
    if let Err(e) = block_on(client.drop_kinesis_source_and_view(&config.kinesis_source_name)) {
        panic!(
            "hit error dropping Kinesis source in Materialize: {}",
            e.to_string()
        );
    }

    Ok(())
}

/// Creates a new Kinesis stream and puts a record into it.
fn instantiate_kinesis_for_source(client: &KinesisClient, stream_name: &String) -> Result<String> {
    // Create a new Kinesis stream
    log::info!("creating a new Kinesis stream: {}", stream_name);
    let create_stream_input = CreateStreamInput {
        shard_count: 1,
        stream_name: stream_name.clone(),
    };
    if let Err(e) = block_on(client.create_stream(create_stream_input)) {
        panic!("hit error creating stream: {}", e.to_string());
    }

    // We need the ARN of the created stream to create a
    // Kinesis source in Materialize.
    let describe_input = DescribeStreamInput {
        exclusive_start_shard_id: None,
        limit: None,
        stream_name: stream_name.clone(),
    };
    let arn = match block_on(client.describe_stream(describe_input)) {
        Ok(output) => output.stream_description.stream_arn,
        Err(err) => panic!(err),
    };

    log::info!("trying to put a record into the stream");
    let test_string = String::from(TEST_RECORD_STRING);
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
    Ok(String::from(arn))
}

fn delete_kinesis_stream(client: &KinesisClient, stream_name: &String) -> Result<()> {
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
