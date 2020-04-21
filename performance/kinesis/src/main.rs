// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Generate some amount of data, push it to a Kinesis Data Stream,
/// create a Materialize source from that stream.
/// Monitor performance via the timings and the Grafana dashboard.
use futures::executor::block_on;
use rand::Rng;
use structopt::StructOpt;

mod kinesis;
mod mz_client;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        println!("ERROR: {}", e);
    }
}

async fn run() -> Result<(), String> {
    let timer = std::time::Instant::now();
    let args = Args::from_args();

    let materialize_client =
        mz_client::MzClient::new(&args.materialized_host, args.materialized_port).await?;
    let (kinesis_client, kinesis_info) = block_on(kinesis::create_client("us-east-2"))?;

    let seed: u32 = rand::thread_rng().gen();
    let stream_name: String = format!("kinesis-load-test-{}", seed);
    let record_count = args.record_count;

    block_on(kinesis::create_stream(
        &kinesis_client,
        &stream_name,
        args.shard_count,
    ))?;
    println!("Created Kinesis stream {}", stream_name);

    let kinesis_client_clone = kinesis_client.clone();
    let stream_name_clone = stream_name.clone();
    let put = tokio::spawn(async move {
        kinesis::generate_and_put_records(
            &kinesis_client_clone,
            &stream_name_clone,
            record_count,
            args.records_per_second,
        )
        .await
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
                kinesis_info.aws_credentials.token(),
                record_count,
            )
            .await
    });

    let (put_result, mz_result) = futures::join!(put, mz);

    block_on(kinesis::delete_stream(&kinesis_client, &stream_name))?;

    put_result.map_err(|e| format!("put result err: {}", e))??;
    mz_result.map_err(|e| format!("mz result err: {}", e))??;
    println!(
        "Completed test in {} milliseconds",
        timer.elapsed().as_millis()
    );

    Ok(())
}

#[derive(Clone, Debug, StructOpt)]
pub struct Args {
    /// The materialized host
    #[structopt(long, default_value = "materialized")]
    pub materialized_host: String,

    /// The materialized port
    #[structopt(long, default_value = "6875")]
    pub materialized_port: u16,

    /// The number of shards in the Kinesis stream
    #[structopt(long, default_value = "50")]
    pub shard_count: i64,

    /// The total number of records to create
    #[structopt(long, default_value = "1000000")]
    pub record_count: i64,

    /// The number of records to put to the Kinesis stream per second
    #[structopt(long, default_value = "2000")]
    pub records_per_second: i64,

    // todo: use to rate limit querying.
    /// The number of times to query Materialize per second
    #[structopt(long, default_value = "1")]
    pub queries_per_second: i64,
}
