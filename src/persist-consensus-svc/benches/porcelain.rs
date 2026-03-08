// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Porcelain benchmarks: operations through the gRPC layer.
//!
//! Comparison with plumbing numbers directly quantifies gRPC overhead
//! (proto ser/deser + loopback TCP + tonic).

use std::time::{Duration, Instant};

use criterion::Criterion;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tonic::transport::Server;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::consensus_service_client::ConsensusServiceClient;
use mz_persist::generated::consensus_service::consensus_service_server::ConsensusServiceServer;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoHeadRequest, ProtoScanRequest, ProtoVersionedData,
};
use mz_persist_consensus_svc::actor::{Actor, ActorCommand, ActorConfig};
use mz_persist_consensus_svc::metrics::ConsensusMetrics;
use mz_persist_consensus_svc::service::ConsensusGrpcService;
use mz_persist_consensus_svc::wal::NoopWalWriter;

use crate::{cas_and_flush, send_flush};

/// Start a gRPC server on a random port and return a connected client plus
/// the actor's command channel (for explicit flush triggering).
///
/// If `flush_interval` is provided, the actor uses timer-driven flushes.
/// Otherwise uses a huge interval (benchmarks flush explicitly).
async fn start_server_and_client_with_interval(
    flush_interval_ms: Option<u64>,
) -> (
    ConsensusServiceClient<tonic::transport::Channel>,
    mpsc::Sender<ActorCommand>,
) {
    let config = ActorConfig {
        flush_interval_ms: flush_interval_ms.unwrap_or(86_400_000),
        snapshot_interval: u64::MAX,
        ..Default::default()
    };
    let metrics = ConsensusMetrics::register(&MetricsRegistry::new());
    let (handle, _task) = Actor::spawn_on_current(config, NoopWalWriter, metrics);

    let service = ConsensusGrpcService { handle: handle.clone() };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    mz_ore::task::spawn(|| "bench-grpc-server", async move {
        Server::builder()
            .add_service(ConsensusServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Brief delay for the server to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = ConsensusServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    (client, handle.sender().clone())
}

/// gRPC head latency. Comparison with plumbing/head shows the gRPC tax on reads.
pub fn bench_grpc_head(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("porcelain/head");

    g.bench_function("grpc_loopback", |b| {
        let (mut client, tx) = runtime.block_on(start_server_and_client_with_interval(None));
        // Pre-populate via direct channel (faster than gRPC for setup).
        runtime.block_on(async {
            cas_and_flush(&tx, "shard-0", None, 1, &[0u8; 64]).await;
        });
        b.iter(|| {
            runtime.block_on(async {
                client
                    .head(ProtoHeadRequest {
                        key: "shard-0".to_string(),
                    })
                    .await
                    .unwrap();
            });
        });
    });
}

/// gRPC CAS latency including flush. Uses a 1ms timer-driven flush interval so
/// the gRPC CAS call blocks until the timer fires and flushes. This measures
/// the realistic end-to-end path: gRPC request → actor CAS → timer flush →
/// gRPC response.
pub fn bench_grpc_cas(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("porcelain/cas_flush");

    g.bench_function("grpc_loopback", |b| {
        // Use a 1ms flush interval so CAS replies come back promptly.
        let (mut client, _tx) = runtime.block_on(start_server_and_client_with_interval(Some(1)));
        let mut seqno = 0u64;
        b.iter(|| {
            seqno += 1;
            let expected = if seqno == 1 { None } else { Some(seqno - 1) };
            runtime.block_on(async {
                client
                    .compare_and_set(ProtoCompareAndSetRequest {
                        key: "s".to_string(),
                        expected,
                        new: Some(ProtoVersionedData {
                            seqno,
                            data: vec![0u8; 64],
                        }),
                    })
                    .await
                    .unwrap();
            });
        });
    });
}

/// Full-stack 90/8/1/1 mix through gRPC. CAS writes go through the direct
/// actor channel (since gRPC CAS blocks until flush), reads go through gRPC.
/// This measures gRPC overhead on the read path under realistic write load.
pub fn bench_grpc_realistic_mix(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("porcelain/realistic_mix");

    g.bench_function("90cas_8scan_1head_1trunc", |b| {
        b.iter_custom(|iters| {
            let (mut client, tx) = runtime.block_on(start_server_and_client_with_interval(None));
            let num_shards = 100;
            let ops_per_iter = 100;

            // Pre-populate via direct channel.
            runtime.block_on(async {
                for i in 0..num_shards {
                    cas_and_flush(&tx, &format!("s-{}", i), None, 1, &[0u8; 64]).await;
                }
            });
            let mut shard_seqno = vec![1u64; num_shards];

            let start = Instant::now();
            runtime.block_on(async {
                for _iter in 0..iters {
                    let mut pending_cas = Vec::new();
                    for op in 0..ops_per_iter {
                        let shard_idx = op % num_shards;
                        let key = format!("s-{}", shard_idx);

                        match op % 100 {
                            // 90% CAS via direct channel (gRPC CAS blocks on flush).
                            0..90 => {
                                let expected = shard_seqno[shard_idx];
                                shard_seqno[shard_idx] += 1;
                                pending_cas.push(
                                    crate::send_cas(
                                        &tx,
                                        &key,
                                        Some(expected),
                                        expected + 1,
                                        &[0u8; 64],
                                    )
                                    .await,
                                );
                                if pending_cas.len() >= 50 {
                                    send_flush(&tx).await;
                                    for rx in pending_cas.drain(..) {
                                        rx.await.unwrap().unwrap();
                                    }
                                }
                            }
                            // 8% scan via gRPC.
                            90..98 => {
                                client
                                    .scan(ProtoScanRequest {
                                        key,
                                        from: 0,
                                        limit: 10,
                                    })
                                    .await
                                    .unwrap();
                            }
                            // 1% head via gRPC.
                            98 => {
                                client.head(ProtoHeadRequest { key }).await.unwrap();
                            }
                            // 1% truncate via direct channel.
                            _ => {
                                crate::send_truncate_and_flush(&tx, &key, 1).await;
                            }
                        }
                    }
                    if !pending_cas.is_empty() {
                        send_flush(&tx).await;
                        for rx in pending_cas.drain(..) {
                            rx.await.unwrap().unwrap();
                        }
                    }
                }
            });
            start.elapsed()
        });
    });
}
