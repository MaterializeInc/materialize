// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(clippy::as_conversions)]

use std::io;
use std::iter;

use anyhow::Context;
use rusoto_core::RusotoError;
use rusoto_s3::{
    CreateBucketConfiguration, CreateBucketError, CreateBucketRequest, PutObjectRequest, S3,
};
use structopt::StructOpt;
use tracing::{error, info, Level};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use ore::cast::CastFrom;

/// Generate meaningless data in S3 to test download speeds
#[derive(StructOpt)]
struct Args {
    /// How large to make each line (record) in Bytes
    #[structopt(short = "l", long)]
    line_bytes: usize,

    /// How large to make each object, e.g. `1 KiB`
    #[structopt(
        short = "s",
        long,
        parse(try_from_str = parse_object_size)
    )]
    object_size: usize,

    /// How many objects to create
    #[structopt(short = "c", long)]
    object_count: usize,

    /// All objects will be inserted into this prefix
    #[structopt(short = "p", long)]
    key_prefix: String,

    /// All objects will be inserted into this bucket
    #[structopt(short = "b", long)]
    bucket: String,

    /// Which region to operate in
    #[structopt(short = "r", long, default_value = "us-east-2")]
    region: String,

    /// Number of copy operations to run concurrently
    #[structopt(long, default_value = "50")]
    concurrent_copies: usize,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        error!("{:#}", e);
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let args: Args = ore::cli::parse_args();
    let env_filter =
        EnvFilter::try_from_env("MZ_LOG_FILTER").or_else(|_| EnvFilter::try_new("info"))?;
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().with_writer(io::stderr))
        .init();

    info!(
        "starting up to create {} of data across {} objects in {}/{}",
        bytefmt::format(u64::cast_from(args.object_size * args.object_count)),
        args.object_count,
        args.bucket,
        args.key_prefix
    );

    let line = iter::repeat('A')
        .take(args.line_bytes)
        .chain(iter::once('\n'))
        .collect::<String>();
    let mut object_size = 0;
    let line_size = line.len();
    let object = iter::repeat(line)
        .take_while(|_| {
            object_size += line_size;
            object_size < args.object_size
        })
        .collect::<String>();

    let conn_info =
        aws_util::aws::ConnectInfo::new(rusoto_core::Region::default(), None, None, None)?;

    let client = aws_util::client::s3(conn_info).context("creating s3 client")?;

    let first_object_key = format!("{}{:>05}", args.key_prefix, 0);

    let progressbar = indicatif::ProgressBar::new(u64::cast_from(args.object_count));

    client
        .create_bucket(CreateBucketRequest {
            bucket: args.bucket.clone(),
            create_bucket_configuration: Some(CreateBucketConfiguration {
                location_constraint: Some(args.region.clone()),
            }),
            ..Default::default()
        })
        .await
        .map(|_| info!("created s3 bucket {}", args.bucket))
        .or_else(|e| {
            if matches!(
                e,
                RusotoError::Service(CreateBucketError::BucketAlreadyOwnedByYou(_))
            ) {
                tracing::event!(Level::INFO, bucket = %args.bucket, "reusing existing bucket");
                Ok(())
            } else {
                Err(e)
            }
        })?;

    let mut total_created = 0;
    client
        .put_object(PutObjectRequest {
            bucket: args.bucket.clone(),
            key: first_object_key.clone(),
            body: Some(object.into_bytes().into()),
            ..Default::default()
        })
        .await?;
    total_created += 1;
    progressbar.inc(1);

    let copy_source = format!("{}/{}", args.bucket, first_object_key.clone());

    let mut copy_reqs = Vec::new();
    let pool_size = args.concurrent_copies;
    for i in 1..pool_size + 1 {
        copy_reqs.push(client.copy_object(rusoto_s3::CopyObjectRequest {
            bucket: args.bucket.clone(),
            copy_source: copy_source.clone(),
            key: format!("{}{:>05}", args.key_prefix, i),
            ..Default::default()
        }));
        progressbar.inc(1);
        total_created += 1;
    }

    for i in (pool_size + 1)..(args.object_count - pool_size) {
        let (_resp, _, copies) = futures::future::select_all(copy_reqs).await;
        copy_reqs = copies;
        copy_reqs.push(client.copy_object(rusoto_s3::CopyObjectRequest {
            bucket: args.bucket.clone(),
            copy_source: copy_source.clone(),
            key: format!("{}{:>05}", args.key_prefix, i),
            ..Default::default()
        }));
        progressbar.inc(1);
        total_created += 1;
    }
    while !copy_reqs.is_empty() {
        let (_resp, _, copies) = futures::future::select_all(copy_reqs).await;
        copy_reqs = copies;
        total_created += 1;
        progressbar.inc(1);
    }
    drop(progressbar);

    info!("created {} objects", total_created);
    assert_eq!(total_created, args.object_count);

    Ok(())
}

fn parse_object_size(s: &str) -> Result<usize, &str> {
    bytefmt::parse(s).map(usize::cast_from)
}
