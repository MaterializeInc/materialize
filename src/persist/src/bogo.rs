// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`Consensus`] implementation backed by the `mz-bogo-consensus` gRPC
//! server.
//!
//! Use `bogo://host:port` as `--persist-consensus-url` to route persist's
//! Consensus traffic to an in-memory bogo-consensus server. This bypasses
//! Postgres/CRDB entirely; it exists solely to take that backend out of the
//! loop during performance measurements.

use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use mz_bogo_consensus::client::{BogoConsensusClient, CaSResult as ProtoCaSResult};
use mz_bogo_consensus::proto::VersionedData as ProtoVersionedData;
use mz_ore::url::SensitiveUrl;
use tonic::{Code, Status};

use crate::location::{
    CaSResult, Consensus, Determinate, ExternalError, Indeterminate, ResultStream, SCAN_ALL, SeqNo,
    VersionedData,
};

/// Configuration for opening a [`BogoConsensus`].
#[derive(Debug, Clone)]
pub struct BogoConsensusConfig {
    /// The `bogo://` URL of the server.
    pub url: SensitiveUrl,
}

impl BogoConsensusConfig {
    /// Builds a config from a `bogo://host:port` URL. Returns an error if the
    /// URL is missing the host/port or uses the wrong scheme.
    pub fn new(url: SensitiveUrl) -> Result<Self, ExternalError> {
        if url.scheme() != "bogo" {
            return Err(ExternalError::from(anyhow!(
                "bogo-consensus URL must use the `bogo://` scheme, got `{}://`",
                url.scheme()
            )));
        }
        if url.host_str().is_none() || url.port().is_none() {
            return Err(ExternalError::from(anyhow!(
                "bogo-consensus URL must include host and port, got `{}`",
                url
            )));
        }
        Ok(Self { url })
    }
}

/// A [`Consensus`] backed by a remote `mz-bogo-consensus` server.
#[derive(Debug, Clone)]
pub struct BogoConsensus {
    client: Arc<BogoConsensusClient>,
}

impl BogoConsensus {
    /// Connects to the bogo-consensus server identified by `cfg.url`.
    pub async fn open(cfg: BogoConsensusConfig) -> Result<Self, ExternalError> {
        let host = cfg
            .url
            .host_str()
            .ok_or_else(|| ExternalError::from(anyhow!("bogo URL missing host")))?;
        let port = cfg
            .url
            .port()
            .ok_or_else(|| ExternalError::from(anyhow!("bogo URL missing port")))?;
        // tonic wants an http:// (or https://) endpoint; the `bogo://` scheme
        // is just a marker we use for routing.
        let endpoint = format!("http://{host}:{port}");
        let client = BogoConsensusClient::connect(endpoint)
            .await
            .map_err(|e| ExternalError::from(anyhow!("connecting to bogo-consensus: {e:#}")))?;
        Ok(Self {
            client: Arc::new(client),
        })
    }
}

/// Map a `tonic::Status` to `ExternalError`, classifying codes as
/// determinate vs indeterminate.
///
/// Codes returned by the server's deterministic logic (e.g. invalid seqno,
/// truncate-too-high) are mapped to `Determinate`. Network/transport-class
/// codes are `Indeterminate` — the operation may or may not have committed.
fn status_to_external(rpc: &'static str, status: Status) -> ExternalError {
    let msg = anyhow!(
        "bogo-consensus {rpc}: {} ({})",
        status.message(),
        status.code()
    );
    match status.code() {
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            ExternalError::Determinate(Determinate::new(msg))
        }
        _ => ExternalError::Indeterminate(Indeterminate::new(msg)),
    }
}

fn from_proto(v: ProtoVersionedData) -> VersionedData {
    VersionedData {
        seqno: SeqNo(v.seqno),
        data: v.data,
    }
}

fn to_proto(v: VersionedData) -> ProtoVersionedData {
    ProtoVersionedData {
        seqno: v.seqno.0,
        data: v.data,
    }
}

#[async_trait]
impl Consensus for BogoConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let client = Arc::clone(&self.client);
        Box::pin(try_stream! {
            let mut stream = client
                .list_keys()
                .await
                .map_err(|s| status_to_external("list_keys", s))?;
            while let Some(item) = stream.next().await {
                let key = item.map_err(|s| status_to_external("list_keys", s))?;
                yield key;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let data = self
            .client
            .head(key)
            .await
            .map_err(|s| status_to_external("head", s))?;
        Ok(data.map(from_proto))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        if new.seqno.0 > i64::MAX.try_into().expect("i64::MAX fits in u64") {
            return Err(ExternalError::from(anyhow!(
                "sequence numbers must fit within [0, i64::MAX], received: {:?}",
                new.seqno
            )));
        }
        let result = self
            .client
            .compare_and_set(key, to_proto(new))
            .await
            .map_err(|s| status_to_external("compare_and_set", s))?;
        Ok(match result {
            ProtoCaSResult::Committed => CaSResult::Committed,
            ProtoCaSResult::ExpectationMismatch => CaSResult::ExpectationMismatch,
            ProtoCaSResult::Unspecified => {
                return Err(ExternalError::Indeterminate(Indeterminate::new(anyhow!(
                    "bogo-consensus returned unspecified CaSResult"
                ))));
            }
        })
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let scan_all_u64 = u64::try_from(SCAN_ALL).unwrap_or(u64::MAX);
        let limit_u64 = u64::try_from(limit).unwrap_or(scan_all_u64);
        let data = self
            .client
            .scan(key, from.0, limit_u64)
            .await
            .map_err(|s| status_to_external("scan", s))?;
        Ok(data.into_iter().map(from_proto).collect())
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let deleted = self
            .client
            .truncate(key, seqno.0)
            .await
            .map_err(|s| status_to_external("truncate", s))?;
        Ok(deleted.map(|d| usize::try_from(d).unwrap_or(usize::MAX)))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::Bytes;
    use futures_util::stream::{FuturesUnordered, StreamExt};
    use mz_bogo_consensus::metrics::BogoMetrics;
    use mz_bogo_consensus::proto::bogo_consensus_server::BogoConsensusServer as BogoGrpcServer;
    use mz_bogo_consensus::server::BogoConsensusServer;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::url::SensitiveUrl;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::location::tests::consensus_impl_test;
    use crate::mem::MemConsensus;

    async fn spawn_server() -> SocketAddr {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = TcpListenerStream::new(listener);
        let metrics = Arc::new(BogoMetrics::new(&MetricsRegistry::new()));
        let svc = BogoGrpcServer::new(BogoConsensusServer::new(metrics))
            .max_decoding_message_size(256 * 1024 * 1024)
            .max_encoding_message_size(256 * 1024 * 1024);
        tokio::spawn(async move {
            Server::builder()
                .tcp_nodelay(true)
                // Bump HTTP/2 flow control windows well above tonic's 65 KiB
                // default. With the default, after the connection window fills
                // up the sender stalls waiting for WINDOW_UPDATE round-trips,
                // dominating per-call latency on local benchmarks.
                .initial_stream_window_size(8 * 1024 * 1024)
                .initial_connection_window_size(16 * 1024 * 1024)
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
                .expect("server");
        });
        addr
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(coverage, ignore)]
    async fn bogo_consensus_conformance() -> Result<(), ExternalError> {
        let addr = spawn_server().await;
        consensus_impl_test(|| {
            let url = SensitiveUrl::from_str(&format!("bogo://{}", addr)).expect("valid url");
            async move {
                let cfg = BogoConsensusConfig::new(url)?;
                BogoConsensus::open(cfg).await
            }
        })
        .await
    }

    // Microbench: compare bogo (gRPC) vs MemConsensus (in-process) head-to-head.
    //
    // Run with:
    //   cargo test --release -p mz-persist bogo_consensus_microbench -- --ignored --nocapture
    //
    // The workload mirrors persist's per-shard access: many CAS appends, then
    // scans returning growing history. We run sequentially first to capture
    // per-op latency, then with concurrency to capture mutex/serialisation
    // behaviour.
    #[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    #[ignore]
    async fn bogo_consensus_microbench() -> Result<(), ExternalError> {
        let bogo_addr = spawn_server().await;
        let bogo_url = SensitiveUrl::from_str(&format!("bogo://{}", bogo_addr)).expect("valid url");
        let bogo: Arc<dyn Consensus> =
            Arc::new(BogoConsensus::open(BogoConsensusConfig::new(bogo_url)?).await?);
        let mem: Arc<dyn Consensus> = Arc::new(MemConsensus::default());

        for (name, c) in &[("mem", &mem), ("bogo", &bogo)] {
            println!("\n=== {} ===", name);
            // Fresh-server-state benches first to isolate per-op overhead from
            // accumulated-state effects. iters=2000 to amortise warmup.
            bench_serial_head(c, 2000).await;
            bench_serial_cas_size(c, 2000, 0).await;
            bench_serial_cas_size(c, 2000, 64).await;
            bench_serial_cas_size(c, 2000, 1024).await;
            bench_serial_cas_size(c, 2000, 16 * 1024).await;
            bench_serial_head(c, 2000).await; // post-CAS
            bench_serial_scan(c).await;
            bench_concurrent_cas(c, 16).await;
            bench_concurrent_cas(c, 64).await;
        }
        Ok(())
    }

    async fn bench_serial_cas_size(consensus: &Arc<dyn Consensus>, iters: u64, size: usize) {
        let key = format!("serial-cas-{}-{}", size, uuid::Uuid::new_v4());
        let data = Bytes::from(vec![0u8; size]);
        let start = Instant::now();
        for i in 0..iters {
            let r = consensus
                .compare_and_set(
                    &key,
                    VersionedData {
                        seqno: SeqNo(i),
                        data: data.clone(),
                    },
                )
                .await
                .expect("ok");
            assert_eq!(r, CaSResult::Committed);
        }
        let elapsed = start.elapsed();
        let per_op_us = elapsed.as_micros() as f64 / iters as f64;
        println!(
            "  serial_cas (size={:6}): {} ops in {:.2?} = {:.1} μs/op",
            size, iters, elapsed, per_op_us
        );
    }

    async fn bench_serial_head(consensus: &Arc<dyn Consensus>, iters: u64) {
        let key = format!("head-{}", uuid::Uuid::new_v4());
        let start = Instant::now();
        for _ in 0..iters {
            let _ = consensus.head(&key).await.expect("ok");
        }
        let elapsed = start.elapsed();
        let per_op_us = elapsed.as_micros() as f64 / iters as f64;
        println!(
            "  serial_head: {} ops in {:.2?} = {:.1} μs/op",
            iters, elapsed, per_op_us
        );
    }

    async fn bench_serial_scan(consensus: &Arc<dyn Consensus>) {
        // Pre-populate a key with 1000 versions, each 4 KiB. Then time scan-all.
        let key = format!("scan-{}", uuid::Uuid::new_v4());
        let data = Bytes::from(vec![0u8; 4 * 1024]);
        let history = 1000usize;
        for i in 0..history {
            consensus
                .compare_and_set(
                    &key,
                    VersionedData {
                        seqno: SeqNo(u64::try_from(i).unwrap()),
                        data: data.clone(),
                    },
                )
                .await
                .expect("ok");
        }
        let iters = 100;
        let start = Instant::now();
        for _ in 0..iters {
            let res = consensus
                .scan(&key, SeqNo::minimum(), SCAN_ALL)
                .await
                .expect("ok");
            assert_eq!(res.len(), history);
        }
        let elapsed = start.elapsed();
        let per_op_us = elapsed.as_micros() as f64 / iters as f64;
        println!(
            "  serial_scan ({} versions × 4 KiB): {} ops in {:.2?} = {:.1} μs/op",
            history, iters, elapsed, per_op_us
        );
    }

    async fn bench_concurrent_cas(consensus: &Arc<dyn Consensus>, concurrency: usize) {
        // Many shards, each one client doing serial CAS. Spawn `concurrency`
        // tasks that each hammer their own shard. Mimics persist's typical
        // "many tables, all writing concurrently" pattern.
        let data = Bytes::from(vec![0u8; 1024]);
        let iters_per_shard: u64 = 100;
        let mut handles = FuturesUnordered::new();
        let start = Instant::now();
        for shard_idx in 0..concurrency {
            let c = Arc::clone(consensus);
            let data = data.clone();
            let key = format!("concurrent-cas-{}-{}", uuid::Uuid::new_v4(), shard_idx);
            handles.push(mz_ore::task::spawn(|| "bench_concurrent_cas", async move {
                for i in 0..iters_per_shard {
                    let r = c
                        .compare_and_set(
                            &key,
                            VersionedData {
                                seqno: SeqNo(i),
                                data: data.clone(),
                            },
                        )
                        .await
                        .expect("ok");
                    assert_eq!(r, CaSResult::Committed);
                }
            }));
        }
        while let Some(_h) = handles.next().await {}
        let elapsed = start.elapsed();
        let total_ops = concurrency as u64 * iters_per_shard;
        let throughput = total_ops as f64 / elapsed.as_secs_f64();
        println!(
            "  concurrent_cas (concurrency={}, {} ops/shard): total {} ops in {:.2?} = {:.0} ops/s",
            concurrency, iters_per_shard, total_ops, elapsed, throughput,
        );
    }
}
