// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Microbenchmarks for the consensus service actor loop.
//!
//! The "plumbing" and "porcelain" names are from git [1]. Plumbing benchmarks
//! measure the actor directly via the mpsc channel — the fastest we could go
//! in theory. Porcelain benchmarks go through the gRPC layer — how fast we
//! actually are end-to-end.
//!
//! [1]: https://git-scm.com/book/en/v2/Git-Internals-Plumbing-and-Porcelain

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::sync::{mpsc, oneshot};

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::ProtoVersionedData;
use mz_persist_consensus_svc::actor::{Actor, ActorCommand, ActorConfig};
use mz_persist_consensus_svc::metrics::ConsensusMetrics;
use mz_persist_consensus_svc::wal::{LatencyProfile, LatencyWalWriter, NoopWalWriter};

mod plumbing;
mod porcelain;

fn test_metrics() -> ConsensusMetrics {
    ConsensusMetrics::register(&MetricsRegistry::new())
}

/// Spawn a bench actor with NoopWalWriter and a huge flush interval (timer
/// never fires — benchmarks use explicit `Flush` commands).
pub(crate) fn spawn_bench_actor(
    runtime: &tokio::runtime::Runtime,
) -> (mpsc::Sender<ActorCommand>, mz_ore::task::JoinHandle<()>) {
    runtime.block_on(async {
        let config = ActorConfig {
            flush_interval_ms: 86_400_000,
            snapshot_interval: u64::MAX,
            ..Default::default()
        };
        let (handle, task) = Actor::spawn(config, NoopWalWriter, test_metrics());
        (handle.sender().clone(), task)
    })
}

/// Spawn a bench actor with a [`LatencyWalWriter`] that simulates storage
/// latency. Uses a short flush interval so the timer drives flushes
/// realistically (like production).
pub(crate) fn spawn_latency_actor(
    runtime: &tokio::runtime::Runtime,
    profile: LatencyProfile,
    flush_interval_ms: u64,
) -> (mpsc::Sender<ActorCommand>, mz_ore::task::JoinHandle<()>) {
    runtime.block_on(async {
        let config = ActorConfig {
            flush_interval_ms,
            snapshot_interval: u64::MAX,
            ..Default::default()
        };
        let wal = LatencyWalWriter::new(profile);
        let (handle, task) = Actor::spawn(config, wal, test_metrics());
        (handle.sender().clone(), task)
    })
}

/// Send a CAS command, flush, and return whether the CAS was committed.
pub(crate) async fn cas_and_flush(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    expected: Option<u64>,
    seqno: u64,
    data: &[u8],
) -> bool {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::CompareAndSet {
        key: key.to_string(),
        expected,
        new: ProtoVersionedData {
            seqno,
            data: data.to_vec(),
        },
        reply: reply_tx,
    })
    .await
    .unwrap();
    send_flush(tx).await;
    reply_rx.await.unwrap().unwrap().committed
}

/// Send a CAS without flushing (for batching benchmarks).
pub(crate) async fn send_cas(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    expected: Option<u64>,
    seqno: u64,
    data: &[u8],
) -> oneshot::Receiver<
    Result<mz_persist::generated::consensus_service::ProtoCompareAndSetResponse, String>,
> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::CompareAndSet {
        key: key.to_string(),
        expected,
        new: ProtoVersionedData {
            seqno,
            data: data.to_vec(),
        },
        reply: reply_tx,
    })
    .await
    .unwrap();
    reply_rx
}

/// Trigger an explicit flush and wait for it to complete.
pub(crate) async fn send_flush(tx: &mpsc::Sender<ActorCommand>) {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Flush { reply: reply_tx })
        .await
        .unwrap();
    reply_rx.await.unwrap();
}

/// Send a Head command and return the response.
pub(crate) async fn send_head(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
) -> mz_persist::generated::consensus_service::ProtoHeadResponse {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Head {
        key: key.to_string(),
        reply: reply_tx,
    })
    .await
    .unwrap();
    reply_rx.await.unwrap().unwrap()
}

/// Send a Scan command and return the response.
pub(crate) async fn send_scan(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    from: u64,
    limit: u64,
) -> mz_persist::generated::consensus_service::ProtoScanResponse {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Scan {
        key: key.to_string(),
        from,
        limit,
        reply: reply_tx,
    })
    .await
    .unwrap();
    reply_rx.await.unwrap().unwrap()
}

/// Send a Truncate command, flush, and return the response.
pub(crate) async fn send_truncate_and_flush(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    seqno: u64,
) -> mz_persist::generated::consensus_service::ProtoTruncateResponse {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Truncate {
        key: key.to_string(),
        seqno,
        reply: reply_tx,
    })
    .await
    .unwrap();
    send_flush(tx).await;
    reply_rx.await.unwrap().unwrap()
}

pub fn bench_consensus_svc(c: &mut Criterion) {
    mz_ore::test::init_logging_default("warn");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    // --- Plumbing (direct actor channel) ---
    plumbing::bench_head(c, &runtime);
    plumbing::bench_cas_flush(c, &runtime);
    plumbing::bench_scan(c, &runtime);
    plumbing::bench_group_commit(c, &runtime);
    plumbing::bench_read_under_write_pressure(c, &runtime);
    plumbing::bench_write_under_read_pressure(c, &runtime);
    plumbing::bench_realistic_mix(c, &runtime);
    plumbing::bench_large_state(c, &runtime);
    plumbing::bench_latency_profiles(c, &runtime);

    // --- Porcelain (through gRPC) ---
    porcelain::bench_grpc_head(c, &runtime);
    porcelain::bench_grpc_cas(c, &runtime);
    porcelain::bench_grpc_realistic_mix(c, &runtime);
}

// The grouping here is an artifact of criterion's interaction with the
// plug-able rust benchmark harness. We use criterion's groups instead.
criterion_group! {
    benches,
    bench_consensus_svc,
}
criterion_main!(benches);
