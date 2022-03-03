// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;
use std::iter;

use aws_sdk_s3::error::{CreateBucketError, CreateBucketErrorKind};
use aws_sdk_s3::model::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_sdk_s3::types::SdkError;
use clap::Parser;
use futures::stream::{self, StreamExt, TryStreamExt};
use tracing::event;
use tracing::{error, info, Level};
use tracing_subscriber::filter::EnvFilter;

use mz_aws_util::config::AwsConfig;
use mz_ore::cast::CastFrom;

/// Generate meaningless data in S3 to test download speeds
#[derive(Parser)]
struct Args {
    /// How large to make each line (record) in Bytes
    #[clap(short = 'l', long)]
    line_bytes: usize,

    /// How large to make each object, e.g. `1 KiB`
    #[clap(
        short = 's',
        long,
        parse(try_from_str = parse_object_size)
    )]
    object_size: usize,

    /// How many objects to create
    #[clap(short = 'c', long)]
    object_count: usize,

    /// All objects will be inserted into this prefix
    #[clap(short = 'p', long)]
    key_prefix: String,

    /// All objects will be inserted into this bucket
    #[clap(short = 'b', long)]
    bucket: String,

    /// Which region to operate in
    #[clap(short = 'r', long, default_value = "us-east-2")]
    region: String,

    /// Number of copy operations to run concurrently
    #[clap(long, default_value = "50")]
    concurrent_copies: usize,

    /// Which log messages to emit.
    ///
    /// See materialized's `--log-filter` option for details.
    #[clap(long, value_name = "FILTER", default_value = "off")]
    log_filter: EnvFilter,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        error!("{:#}", e);
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let args: Args = mz_ore::cli::parse_args();

    tracing_subscriber::fmt()
        .with_env_filter(args.log_filter)
        .with_writer(io::stderr)
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

    let config = AwsConfig::load_from_env().await;
    let client = mz_aws_util::s3::client(&config);

    let first_object_key = format!("{}{:>05}", args.key_prefix, 0);

    let progressbar = indicatif::ProgressBar::new(u64::cast_from(args.object_count));

    let bucket_config = match config.region().map(|r| r.as_ref()) {
        // us-east-1 is special and is not accepted as a location constraint.
        None | Some("us-east-1") => None,
        Some(r) => Some(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(r))
                .build(),
        ),
    };
    client
        .create_bucket()
        .bucket(&args.bucket)
        .set_create_bucket_configuration(bucket_config)
        .send()
        .await
        .map(|_| info!("created s3 bucket {}", args.bucket))
        .or_else(|e| match e {
            SdkError::ServiceError {
                err:
                    CreateBucketError {
                        kind: CreateBucketErrorKind::BucketAlreadyOwnedByYou(_),
                        ..
                    },
                ..
            } => {
                event!(Level::INFO, bucket = %args.bucket, "reusing existing bucket");
                Ok(())
            }
            _ => Err(e),
        })?;

    let mut total_created = 0;
    client
        .put_object()
        .bucket(&args.bucket)
        .key(&first_object_key)
        .body(object.into_bytes().into())
        .send()
        .await?;
    total_created += 1;
    progressbar.inc(1);

    let copy_source = format!("{}/{}", args.bucket, first_object_key.clone());

    let copy_reqs = (1..args.object_count).map(|i| {
        client
            .copy_object()
            .bucket(&args.bucket)
            .copy_source(&copy_source)
            .key(format!("{}{:>05}", args.key_prefix, i))
            .send()
    });
    let mut copy_reqs_stream = stream::iter(copy_reqs).buffer_unordered(args.concurrent_copies);
    while let Some(_) = copy_reqs_stream.try_next().await? {
        progressbar.inc(1);
        total_created += 1;
    }
    drop(progressbar);

    info!("created {} objects", total_created);
    assert_eq!(total_created, args.object_count);

    Ok(())
}

fn parse_object_size(s: &str) -> Result<usize, &'static str> {
    bytefmt::parse(s).map(usize::cast_from)
}
