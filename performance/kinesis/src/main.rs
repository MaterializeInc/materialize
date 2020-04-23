// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// This code is built to load test Kinesis sources.
///
/// Essentially, it:
///     - Generates some amount of data (total_records). Right now, the data are just
///       random strings converted to bytes.
///     - Push the generated data to the target Kinesis stream (at a rate of records_per_second).
///     - Create a source from the Kinesis stream. Create a materialized view of the count
///       of records from the stream.
///
/// The test will end and is considered successful iff all records are pushed to
/// Kinesis, all records are accounted for in materialized, AND the performance seems
/// reasonable.
///
/// To evaluate overall performance, we use the latency metrics in the Grafana dashboard.
/// In general, the server side latencies should be low and consistent over time. Additionally,
/// "Time behind external source," which indicates our lag behind the tip of the Kinesis
/// stream, should not drift over time. (These measurements should become more concrete as
/// we get more experience running this test).
///
/// TODOs:
///     - Get this test to run nightly.
///     - Handle this token error: {"__type":"ExpiredTokenException","message":"The security token included in the request is expired"}
///       This can occur from the producer side (the kinesis thread).
///     - Expand the use cases the test is covering:
///         - Add a new "existing_records" argument that pre-loads that number of records into the
///           target Kinesis stream. This should effectively mock what "catching up" will look like
///           when Materialize connects to an existing stream.
///
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
    env_logger::init();

    let materialize_client =
        mz_client::MzClient::new(&args.materialized_host, args.materialized_port).await?;
    let (kinesis_client, kinesis_info) = kinesis::create_client(&args.aws_region).await?;

    let seed: u32 = rand::thread_rng().gen();
    let stream_name: String = format!("kinesis-load-test-{}", seed);

    log::info!("Starting kinesis load test with mzd={}:{} stream={} shard_count={} total_records={} records_per_second={} queries_per_second={}",
    args.materialized_host, args.materialized_port, &stream_name, args.shard_count, args.total_records, args.records_per_second, args.queries_per_second);

    kinesis::create_stream(&kinesis_client, &stream_name, args.shard_count).await?;
    log::info!("Created Kinesis stream {}", stream_name);

    let kinesis_thread = tokio::spawn({
        let total_records = args.total_records;
        let records_per_second = args.records_per_second;
        let kinesis_client_clone = kinesis_client.clone();
        let stream_name_clone = stream_name.clone();
        async move {
            kinesis::generate_and_put_records(
                &kinesis_client_clone,
                &stream_name_clone,
                total_records,
                records_per_second,
            )
            .await
        }
    });

    let materialize_thread = tokio::spawn({
        let total_records = args.total_records;
        let stream_name_clone = stream_name.clone();
        async move {
            materialize_client
                .query_materialize_for_kinesis_records(
                    kinesis_info.aws_region.name(),
                    &kinesis_info.aws_account,
                    &stream_name_clone,
                    kinesis_info.aws_credentials.aws_access_key_id(),
                    kinesis_info.aws_credentials.aws_secret_access_key(),
                    kinesis_info.aws_credentials.token(),
                    total_records,
                )
                .await
        }
    });

    let (kinesis_result, materialize_result) = futures::join!(kinesis_thread, materialize_thread);

    block_on(kinesis::delete_stream(&kinesis_client, &stream_name))?;

    kinesis_result.map_err(|e| format!("kinesis result err: {}", e))??;
    materialize_result.map_err(|e| format!("materialize result err: {}", e))??;
    log::info!(
        "Completed load test in {} milliseconds",
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

    /// The AWS region of the stream
    #[structopt(long, default_value = "us-east-2")]
    pub aws_region: String,

    /// The number of shards in the Kinesis stream
    #[structopt(long, default_value = "50")]
    pub shard_count: i64,

    /// The total number of records to create
    #[structopt(long, default_value = "150000000")]
    pub total_records: i64,

    /// The number of records to put to the Kinesis stream per second
    #[structopt(long, default_value = "2000")]
    pub records_per_second: i64,

    // todo: use to rate limit querying.
    /// The number of times to query Materialize per second
    #[structopt(long, default_value = "1")]
    pub queries_per_second: i64,
}
