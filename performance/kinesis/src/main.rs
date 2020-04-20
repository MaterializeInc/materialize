// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Notes for myself:
/// - this should only run against AWS, load testing against localstack doesn't make sense.
/// --> can hardcode some stuff (materialized container, etc)
/// To do:
/// - create a options to read stuff in: shard count, number of records
/// - create a container from this code
/// - run against real AWS... put it in CI to make it easy to test.
/// --> this will likely require permissions changes (look back in slack for the 'i console sys')
/// - alternatively, find the load test doc and start up an ec2 instance
///
/// either way, publish results!
/// then, PR.
/// then, talk with Brandon about this running nightly.
use futures::executor::block_on;
use rand::Rng;

mod kinesis;
mod mz_client;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        println!("ERROR: {}", e);
    }
}

async fn run() -> Result<(), String> {
    let materialize_client = mz_client::MzClient::new("localhost", 6875).await?;
    //    let (kinesis_client, kinesis_info) = kinesis::create_client("us-east-2")?;
    // Fake client code only exists so I could test out running most of this locally.
    let (kinesis_client, kinesis_info) = kinesis::create_fake_client("custom-region")?;

    let seed: u32 = rand::thread_rng().gen();
    let stream_name: String = format!("kinesis-load-test-{}", seed);
    // todo: configurable. default to 50.
    let shard_count: i64 = 50;
    // todo: configurable. default to 150 million
    let num_records: i64 = 1_000;

    block_on(kinesis::create_stream(
        &kinesis_client,
        &stream_name,
        shard_count,
    ))?;
    println!("Created Kinesis stream {}", stream_name);

    let records = block_on(kinesis::generate_kinesis_records(
        &kinesis_client,
        &stream_name,
        num_records,
    ))?;

    // push records
    let timer = std::time::Instant::now();
    let kinesis_client_clone = kinesis_client.clone();
    let stream_name_clone = stream_name.clone();
    let put = tokio::spawn(async move {
        kinesis::put_records(&kinesis_client_clone, &stream_name_clone, records).await
    });

    // create materialized view and 1qps.
    let stream_name_clone = stream_name.clone();
    let mz = tokio::spawn(async move {
        materialize_client
            .query_materialize_for_kinesis_records(
                kinesis_info.aws_region.name(),
                &kinesis_info.aws_account,
                &stream_name_clone,
                kinesis_info.aws_credentials.aws_access_key_id(),
                kinesis_info.aws_credentials.aws_secret_access_key(),
                "http://localhost:4568",
                num_records,
            )
            .await
    });

    // stop when all messages read? or just run forever like chbench?
    let (put_result, mz_result) = futures::join!(put, mz);

    // delete stream
    block_on(kinesis::delete_stream(&kinesis_client, &stream_name))?;

    put_result.map_err(|e| format!("put result err: {}", e))??;
    mz_result.map_err(|e| format!("mz result err: {}", e))??;
    println!(
        "Read all {} records in Materialize from Kinesis source in {} milliseconds",
        num_records,
        timer.elapsed().as_millis()
    );

    Ok(())
}
