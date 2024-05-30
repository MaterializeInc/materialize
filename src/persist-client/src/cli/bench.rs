// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI benchmarking tools for persist

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_dyncfg::ConfigUpdates;
use mz_persist::indexed::encoding::{BlobTraceBatchPart, BlobTraceUpdates};
use mz_persist_types::columnar::{PartDecoder, Schema};
use mz_persist_types::dyn_struct::DynStructCol;
use mz_persist_types::Codec;

use crate::cli::args::StateArgs;
use crate::fetch::PART_DECODE_FORMAT;
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

    /// HACK: Decodes the K, V data assuming the schema of lineitem table from
    /// the TPCH load generator source.
    #[clap(long, requires = "parse")]
    decode: bool,
}

/// Runs the given bench command.
pub async fn run<K: Codec + Default + Debug>(
    command: BenchArgs,
    key_schema: Arc<K::Schema>,
) -> Result<(), anyhow::Error> {
    match command.command {
        Command::S3Fetch(args) => bench_s3::<K>(&args, key_schema).await?,
    }

    Ok(())
}

async fn bench_s3<K: Codec + Default + Debug>(
    args: &S3FetchArgs,
    key_schema: Arc<K::Schema>,
) -> Result<(), anyhow::Error> {
    let parse = args.parse;
    let decode = args.decode;
    let shard_id = args.shard.shard_id();
    let state_versions = args.shard.open().await?;
    let mut dyncfg_updates = ConfigUpdates::default();
    dyncfg_updates.add(&PART_DECODE_FORMAT, "row_with_validate");
    dyncfg_updates.apply(&state_versions.cfg);
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

    println!("iter,key,size_bytes,fetch_secs,parse_secs,decode_secs");
    for iter in 0..args.iters {
        let start = Instant::now();
        let mut fetches = Vec::new();
        for part in snap.iter().flat_map(|x| x.parts.iter()) {
            let key = match part {
                BatchPart::Hollow(x) => x.key.complete(&shard_id),
                BatchPart::Inline { .. } => continue,
            };
            let blob = Arc::clone(&state_versions.blob);
            let metrics = Arc::clone(&state_versions.metrics);
            let key_schema = Arc::clone(&key_schema);
            let fetch = mz_ore::task::spawn(|| "", async move {
                let buf = blob.get(&key).await.unwrap().unwrap();
                let fetch_elapsed = start.elapsed();
                let buf_len = buf.len();
                let (parse_elapsed, decode_elapsed) = mz_ore::task::spawn_blocking(
                    || "",
                    move || {
                        if parse {
                            let parse_start = Instant::now();
                            let updates =
                                BlobTraceBatchPart::<u64>::decode(&buf, &metrics.columnar).unwrap();
                            let parse_elapsed = parse_start.elapsed();
                            let decode_elapsed = if decode {
                                let decode_start = Instant::now();
                                match &updates.updates {
                                    BlobTraceUpdates::Row(_) => {}
                                    BlobTraceUpdates::Both(codec, structured) => {
                                        let len = codec.len();
                                        if let Some(key) = &structured.key {
                                            let col =
                                                DynStructCol::from_arrow(key_schema.columns(), key)
                                                    .unwrap();
                                            let decoder = key_schema.decoder(col.as_ref()).unwrap();
                                            let mut k = K::default();
                                            for idx in 0..len {
                                                decoder.decode(idx, &mut k);
                                                std::hint::black_box(&k);
                                            }
                                        }
                                    }
                                }
                                decode_start.elapsed()
                            } else {
                                Duration::ZERO
                            };
                            (parse_elapsed, decode_elapsed)
                        } else {
                            (Duration::ZERO, Duration::ZERO)
                        }
                    },
                )
                .await
                .unwrap();
                (
                    key,
                    buf_len,
                    fetch_elapsed.as_secs_f64(),
                    parse_elapsed.as_secs_f64(),
                    decode_elapsed.as_secs_f64(),
                )
            });
            fetches.push(fetch);
        }
        for fetch in fetches {
            let (key, size_bytes, fetch_secs, parse_secs, decode_secs) = fetch.await.unwrap();
            println!(
                "{},{},{},{},{},{}",
                iter, key, size_bytes, fetch_secs, parse_secs, decode_secs
            );
        }
    }

    Ok(())
}
