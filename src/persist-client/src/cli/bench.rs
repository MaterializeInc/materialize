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
use std::time::Instant;

use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::SeqNo;
use mz_persist_types::Codec;
use timely::dataflow::operators::{Input, Map, Probe};
use timely::progress::Antichain;
use tokio::runtime::Handle;

use crate::async_runtime::IsolatedRuntime;
use crate::cache::StateCache;
use crate::cli::args::StateArgs;
use crate::fetch::{FetchBatchFilter, FetchedBlobBuf, LeasedBatchPart};
use crate::internal::state::BatchPart;
use crate::operators::shard_source::shard_source_fetch;
use crate::read::LeasedReaderId;
use crate::rpc::PubSubClientConnection;
use crate::PersistClient;

/// Commands for read-only inspection of persist state
#[derive(Debug, clap::Args)]
pub struct BenchArgs {
    #[clap(subcommand)]
    command: Command,
}

/// Individual subcommands of bench
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// WIP
    S3Fetch(BenchFetchArgs),
    /// WIP
    ShardSource(BenchFetchArgs),
}

/// WIP bar
#[derive(Debug, Clone, clap::Parser)]
pub struct BenchFetchArgs {
    #[clap(flatten)]
    shard: StateArgs,

    #[clap(long, default_value_t = 1)]
    iters: usize,
}

/// Runs the given bench command.
pub async fn run<K: Codec + Debug, V: Codec + Debug>(
    command: BenchArgs,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
) -> Result<(), anyhow::Error> {
    match command.command {
        Command::S3Fetch(args) => bench_s3(&args).await?,
        Command::ShardSource(args) => {
            bench_shard_source::<K, V>(&args, key_schema, val_schema).await?
        }
    }

    Ok(())
}

async fn bench_s3(args: &BenchFetchArgs) -> Result<(), anyhow::Error> {
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

    println!("iter,key,size_bytes,fetch_secs,decode_secs");
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
            let fetch = mz_ore::task::spawn(|| "", async move {
                let buf = blob.get(&key).await.unwrap().unwrap();
                let fetch_elapsed = start.elapsed();
                let buf_len = buf.len();
                let decode_elapsed = mz_ore::task::spawn_blocking(
                    || "",
                    move || {
                        let start = Instant::now();
                        BlobTraceBatchPart::<u64>::decode(&buf, &metrics.columnar).unwrap();
                        start.elapsed()
                    },
                )
                .await
                .unwrap();
                (
                    key,
                    buf_len,
                    fetch_elapsed.as_secs_f64(),
                    decode_elapsed.as_secs_f64(),
                )
            });
            fetches.push(fetch);
        }
        for fetch in fetches {
            let (key, size_bytes, fetch_secs, decode_secs) = fetch.await.unwrap();
            println!(
                "{},{},{},{},{}",
                iter, key, size_bytes, fetch_secs, decode_secs
            );
        }
    }

    Ok(())
}

async fn bench_shard_source<K: Codec + Debug, V: Codec + Debug>(
    args: &BenchFetchArgs,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
) -> Result<(), anyhow::Error> {
    let shard_id = args.shard.shard_id();
    let state_versions = args.shard.open().await?;
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;
    let state = state_versions
        .fetch_current_state::<u64>(&shard_id, versions.0)
        .await;
    let state = state.check_ts_codec(&shard_id)?;
    let as_of = state.since().clone();
    let snap = state
        .snapshot(&as_of)
        .expect("since should be available for reads");

    let isolated_runtime = Arc::new(IsolatedRuntime::default());
    let pubsub_sender = PubSubClientConnection::noop().sender;
    let shared_states = Arc::new(StateCache::new(
        &state_versions.cfg,
        Arc::clone(&state_versions.metrics),
        Arc::clone(&pubsub_sender),
    ));
    let client = PersistClient::new(
        state_versions.cfg,
        state_versions.blob,
        state_versions.consensus,
        state_versions.metrics,
        isolated_runtime,
        shared_states,
        pubsub_sender,
    )?;
    let tokio_runtime = Handle::current();

    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    println!("iter,key,size_bytes,fetch_secs,decode_secs");
    let iters = args.iters;
    mz_ore::task::spawn_blocking(
        || "bench",
        move || {
            for iter in 0..iters {
                let tokio_runtime = tokio_runtime.clone();
                let client = client.clone();
                let as_of = as_of.clone().into_option().unwrap();
                let (key_schema, val_schema) = (Arc::clone(&key_schema), Arc::clone(&val_schema));
                let snap = snap.clone();
                let reader_id = LeasedReaderId::new();
                timely::execute(timely::Config::process(ncpus_useful), move |worker| {
                    let start = Instant::now();
                    let tokio_runtime = tokio_runtime.clone();
                    let _guard = tokio_runtime.enter();
                    let metrics = Arc::clone(&client.metrics);
                    let client = client.clone();
                    let (key_schema, val_schema) =
                        (Arc::clone(&key_schema), Arc::clone(&val_schema));
                    let snap = snap.clone();
                    let (mut input, probe, token) = worker.dataflow::<u64, _, _>(move |scope| {
                        let worker_idx = scope.index();
                        let (input, descs) = scope.new_input();
                        let (data, _, token) = shard_source_fetch::<K, V, u64, i64, _>(
                            &descs,
                            "bench",
                            std::future::ready(client),
                            shard_id,
                            key_schema,
                            val_schema,
                            start,
                        );
                        let data = data.map(move |(x, fetch_secs)| {
                            let size_bytes = match &x.buf {
                                FetchedBlobBuf::Hollow { buf, .. } => buf.len(),
                                FetchedBlobBuf::Inline { .. } => unreachable!(""),
                            };

                            let start = Instant::now();
                            std::hint::black_box(x.parse());
                            let decode_secs = start.elapsed().as_secs_f64();

                            println!(
                                "{},{},{},{},{}",
                                iter,
                                worker_idx,
                                size_bytes,
                                fetch_secs.as_secs_f64(),
                                decode_secs,
                            );
                        });
                        (input, data.probe(), token)
                    });
                    if worker.index() == 0 {
                        for batch in snap {
                            for part in batch.parts {
                                match &part {
                                    BatchPart::Hollow(_) => {}
                                    BatchPart::Inline { .. } => continue,
                                }
                                let part = LeasedBatchPart {
                                    metrics: Arc::clone(&metrics),
                                    shard_id,
                                    reader_id: reader_id.clone(),
                                    filter: FetchBatchFilter::Snapshot {
                                        as_of: Antichain::from_elem(as_of),
                                    },
                                    desc: batch.desc.clone(),
                                    part,
                                    leased_seqno: SeqNo::minimum(),
                                    lease: None,
                                    filter_pushdown_audit: false,
                                };
                                let (part, _lease) = part.into_exchangeable_part();
                                let worker_idx =
                                    usize::cast_from(Instant::now().hashed()) % worker.peers();
                                input.send((worker_idx, part));
                            }
                        }
                    }
                    input.close();
                    worker.step_or_park_while(None, || probe.less_equal(&as_of));
                    drop(token);
                    drop(_guard);
                })
                .unwrap();
            }
        },
    )
    .await
    .unwrap();

    Ok(())
}
