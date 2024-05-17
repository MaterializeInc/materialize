// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI benchmarking tools for persist

use futures_util::stream::StreamExt;
use futures_util::{stream, TryStreamExt};
use std::sync::Arc;
use std::time::Instant;

use mz_persist::indexed::encoding::BlobTraceBatchPart;

use crate::cli::args::StateArgs;
use crate::internal::state::BatchPart;

/// Commands for read-only inspection of persist state
#[derive(Debug, clap::Args)]
pub struct BenchArgs {
    #[clap(subcommand)]
    command: Command,
}

/// Individual subcommands of bench
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Fetch the blobs in a shard as quickly as possible, repeated some number
    /// of times.
    S3Fetch(S3FetchArgs),
}

/// Fetch the blobs in a shard as quickly as possible, repeated some number of
/// times.
#[derive(Debug, Clone, clap::Parser)]
pub struct S3FetchArgs {
    #[clap(flatten)]
    shard: StateArgs,

    #[clap(long, default_value_t = 1)]
    iters: usize,

    #[clap(long)]
    parse: bool,
}

/// Runs the given bench command.
pub async fn run(command: BenchArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::S3Fetch(args) => bench_s3(&args).await?,
    }

    Ok(())
}

async fn bench_s3(args: &S3FetchArgs) -> Result<(), anyhow::Error> {
    let parse = args.parse;
    let shard_id = args.shard.shard_id();
    let state_versions = args.shard.open().await?;
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;
    let state = state_versions
        .fetch_current_state::<u64>(&shard_id, versions.0)
        .await;
    let state = state.check_ts_codec(&shard_id)?;
    let snap = state
        .snapshot(state.since())
        .expect("since should be available for reads");

    let batch_parts: Vec<_> = stream::iter(&snap)
        .flat_map(|batch| {
            batch.part_stream(shard_id, &*state_versions.blob, &*state_versions.metrics)
        })
        .try_collect()
        .await?;

    println!("iter,key,size_bytes,fetch_secs,parse_secs");
    for iter in 0..args.iters {
        let start = Instant::now();
        let mut fetches = Vec::new();
        for part in &batch_parts {
            let key = match &**part {
                BatchPart::Hollow(x) => x.key.complete(&shard_id),
                _ => continue,
            };
            let blob = Arc::clone(&state_versions.blob);
            let metrics = Arc::clone(&state_versions.metrics);
            let fetch = mz_ore::task::spawn(|| "", async move {
                let buf = blob.get(&key).await.unwrap().unwrap();
                let fetch_elapsed = start.elapsed();
                let buf_len = buf.len();
                let parse_elapsed = mz_ore::task::spawn_blocking(
                    || "",
                    move || {
                        let start = Instant::now();
                        if parse {
                            BlobTraceBatchPart::<u64>::decode(&buf, &metrics.columnar).unwrap();
                        }
                        start.elapsed()
                    },
                )
                .await
                .unwrap();
                (
                    key,
                    buf_len,
                    fetch_elapsed.as_secs_f64(),
                    parse_elapsed.as_secs_f64(),
                )
            });
            fetches.push(fetch);
        }
        for fetch in fetches {
            let (key, size_bytes, fetch_secs, parse_secs) = fetch.await.unwrap();
            println!(
                "{},{},{},{},{}",
                iter, key, size_bytes, fetch_secs, parse_secs
            );
        }
    }

    Ok(())
}
