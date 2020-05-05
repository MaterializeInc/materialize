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
///     - Pushes the generated data to the target Kinesis stream (at a rate of records_per_second).
///     - Creates a source from the Kinesis stream. Create a materialized view of the count
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
///     - Parameterize queries_per_second.
///     - Get this test to run nightly.
///     - Handle this token error: {"__type":"ExpiredTokenException","message":"The security token included in the request is expired"}
///     - Expand the use cases the test is covering:
///         - Add a new "existing_records" argument that pre-loads that number of records into the
///           target Kinesis stream. This should effectively mock what "catching up" will look like
///           when Materialize connects to an existing stream.
///
use anyhow::Context;
use rand::Rng;
use rusoto_core::Region;
use structopt::StructOpt;
use tokio_postgres::NoTls;

use aws_util;

mod kinesis;
mod mz;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ERROR: {:#?}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), anyhow::Error> {
    let timer = std::time::Instant::now();
    let args = Args::from_args();
    env_logger::init();

    // Initialize and log test variables.
    let seed: u32 = rand::thread_rng().gen();
    let stream_name = format!("kinesis-load-test-{}", seed);

    // todo: make queries per second configurable. (requires mz_client changes)
    log::info!("Starting kinesis load test with mzd={}:{} \
               stream={} shard_count={} total_records={} records_per_second={} queries_per_second={}",
    args.materialized_host, args.materialized_port, &stream_name, args.shard_count, args.total_records, args.records_per_second, 1);

    // Initialize test resources in Kinesis.
    let region: Region = args.aws_region.parse().context("parsing AWS region")?;
    let kinesis_client =
        aws_util::kinesis::kinesis_client(region.clone(), None, None, None).await?;
    let stream_arn =
        kinesis::create_stream(&kinesis_client, &stream_name, args.shard_count).await?;
    log::info!("Created Kinesis stream {}", stream_name);

    // Push records to Kinesis.
    let kinesis_task = tokio::spawn({
        let kinesis_client_clone = kinesis_client.clone();
        let stream_name_clone = stream_name.clone();
        let total_records = args.total_records;
        let records_per_second = args.records_per_second;
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

    // Initialize connection to materialized instance.
    let (client, _conn) = tokio_postgres::Config::new()
        .user("mzd")
        .host(&args.materialized_host)
        .port(args.materialized_port)
        .connect(NoTls)
        .await
        .context("creating postgres client")?;

    // Create Kinesis source and materialized view.
    mz::create_source_and_views(&client, stream_arn).await?;

    // Query materialized view for all pushed Kinesis records.
    let materialize_task = tokio::spawn({
        let total_records = args.total_records;
        async move { mz::query_materialized_view_until(&client, "foo_count", total_records).await }
    });

    let (kinesis_result, materialize_result) = futures::join!(kinesis_task, materialize_task);

    kinesis::delete_stream(&kinesis_client, &stream_name).await?;

    kinesis_result?.context("kinesis thread failed")?;
    materialize_result.context("materialize thread failed")??;
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
}
