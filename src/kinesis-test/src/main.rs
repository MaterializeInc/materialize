// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// This test tests the ability of Materialize Kinesis sources to
/// dynamically respond to changes in the number of AWS Kinesis shards.
///
/// There are three potential scenarios to cover:
///     1. Shards are added: We should always pick up and read from new
///        shards.
///     2. Shards are closed: We should complete reading all pending records
///        from a closed shard.
///     3. All shards are closed: We should keep trying to get new shards.
///         NOTE: We don't currently do this correctly in Materialize,
///         pending todo for jldlaughlin.
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use futures::executor::block_on;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::StaticProvider;
use rusoto_kinesis::{
    CreateStreamInput, DeleteStreamInput, DescribeStreamInput, Kinesis, KinesisClient,
    PutRecordError, PutRecordInput,
};
use std::collections::HashSet;

mod kinesis;
mod mz_client;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        println!("ERROR: {}", e);
    }
    // todo: delete real AWS Kinesis stream on completion/failure.
}

async fn run() -> Result<(), String> {
    let materialize_client = mz_client::MzClient::new("localhost", 6875).await?;
    let (kinesis_client, kinesis_info) = kinesis::create_client("us-east-2")?;
    // Fake client code only exists so I could test out running most of this locally.
    //    let (kinesis_client, kinesis_info) = kinesis::create_fake_client("custom-region")?;

    let seed: u32 = rand::thread_rng().gen();
    let stream_name: String = format!("test-{}", seed);
    let shard_count: i64 = 1;

    // Create stream with 1 shard.
    block_on(kinesis::create_stream(
        &kinesis_client,
        &stream_name,
        shard_count,
    ))?;

    // Create view from stream.
    let source_name = String::from("foo");
    let view_name = format!("{}_view", &source_name);
    block_on(materialize_client.create_kinesis_source(
        &source_name,
        kinesis_info.aws_region.name(),
        &kinesis_info.aws_account,
        &stream_name,
        kinesis_info.aws_credentials.aws_access_key_id(),
        kinesis_info.aws_credentials.aws_secret_access_key(),
        "http://localhost:4568",
    ))?;
    block_on(materialize_client.create_materialized_view(&source_name, &view_name))?;

    // Push data to the stream and assert it all makes it into the view.
    let data = generate_data(shard_count, None);
    push_data_and_assert(
        &kinesis_client,
        &materialize_client,
        &stream_name,
        &view_name,
        &data,
    );

    // Add shards to the stream.
    kinesis::update_shard_count(&kinesis_client, &stream_name, shard_count * 2);

    // Push data to the stream and assert it all makes it into the view.
    let data = generate_data(shard_count, Some(data));
    push_data_and_assert(
        &kinesis_client,
        &materialize_client,
        &stream_name,
        &view_name,
        &data,
    );

    // Remove shards from the stream.
    kinesis::update_shard_count(&kinesis_client, &stream_name, shard_count);

    // Push data to the stream and assert it all makes it into the view.
    let data = generate_data(shard_count, Some(data));
    push_data_and_assert(
        &kinesis_client,
        &materialize_client,
        &stream_name,
        &view_name,
        &data,
    );

    Ok(())
}

fn generate_data(number: i64, previous_data: Option<HashSet<String>>) -> HashSet<String> {
    let mut data: HashSet<String> = previous_data.unwrap_or(HashSet::new());
    for _i in 0..number {
        data.insert(thread_rng().sample_iter(&Alphanumeric).take(30).collect());
    }
    data
}

fn push_data_and_assert(
    kinesis_client: &KinesisClient,
    materialize_client: &mz_client::MzClient,
    stream_name: &str,
    view_name: &str,
    data: &HashSet<String>,
) -> Result<(), String> {
    block_on(kinesis::put_records(&kinesis_client, &stream_name, &data))?;

    // Assert data in stream.
    for record in block_on(materialize_client.query_view(&view_name))? {
        let val: &str = record.get("data");
        assert!(data.contains(val));
    }

    Ok(())
}
