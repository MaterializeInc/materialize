// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wiring helper that bundles the moving parts of the committer into a single
//! call site so that `environmentd` does not need to know the internal
//! relationships between the cache, registry, refresh task, and tonic server.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use mz_persist::location::Consensus;
use tonic::transport::{Channel, Endpoint, Server};

use crate::cache::ShardCache;
use crate::heartbeat::spawn_heartbeat;
use crate::in_process::InProcessConsensus;
use crate::metrics::CommitterMetrics;
use crate::refresh::spawn_refresh;
use crate::server::PersistCommitter;
use crate::subscribe::SubscriberRegistry;

/// Handle returned by [`start_committer`]. Holding it keeps the gRPC server
/// and refresh task alive; dropping it aborts both.
#[derive(Debug)]
pub struct CommitterHandle {
    /// An in-process `Consensus` adapter for `environmentd`'s own persist
    /// traffic. Bypasses tonic to avoid runtime starvation when the same
    /// runtime drives both the gRPC server and its client.
    pub in_process_consensus: Arc<dyn Consensus + Send + Sync>,
    /// A lazy gRPC channel pointing at the committer's loopback listener.
    /// Kept for tests and tooling that want to exercise the wire path; do
    /// not use it for envd's own persist traffic.
    pub loopback_channel: Channel,
    _server_task: mz_ore::task::AbortOnDropHandle<()>,
    _refresh_task: mz_ore::task::AbortOnDropHandle<()>,
    _heartbeat_task: mz_ore::task::AbortOnDropHandle<()>,
}

/// Configuration for starting a committer.
#[derive(Debug, Clone)]
pub struct CommitterConfig {
    pub listen_addr: SocketAddr,
    pub max_cached_shards: usize,
    pub cache_refresh_interval: Duration,
    /// Periodic stats-log interval. `Duration::ZERO` disables the heartbeat
    /// entirely; the task is still spawned but exits immediately.
    pub heartbeat_interval: Duration,
}

/// Build a `PersistCommitter` from `consensus`, start its gRPC server on the
/// configured listen address, spawn the cache refresh task, and return a
/// loopback channel for use by `environmentd` itself.
///
/// `listen_addr` is the TCP address the committer's gRPC server binds. The
/// returned loopback channel points at `http://127.0.0.1:<port>` (lazy
/// connect), so the listen addr is required to be reachable from the local
/// process.
pub fn start_committer(
    consensus: Arc<dyn Consensus + Send + Sync>,
    metrics: CommitterMetrics,
    config: CommitterConfig,
) -> anyhow::Result<CommitterHandle> {
    let cache = Arc::new(ShardCache::new(config.max_cached_shards));
    let registry = Arc::new(SubscriberRegistry::new());
    let committer = Arc::new(PersistCommitter::new(
        Arc::clone(&consensus),
        Arc::clone(&cache),
        Arc::clone(&registry),
        metrics.clone(),
    ));

    let refresh_task = spawn_refresh(
        Arc::clone(&consensus),
        Arc::clone(&cache),
        registry,
        config.cache_refresh_interval,
    );
    let heartbeat_task = spawn_heartbeat(metrics, config.heartbeat_interval);

    let listen_addr = config.listen_addr;
    tracing::info!(
        listen_addr = %listen_addr,
        max_cached_shards = config.max_cached_shards,
        cache_refresh_interval = ?config.cache_refresh_interval,
        "persist committer starting",
    );
    let in_process_consensus: Arc<dyn Consensus + Send + Sync> =
        Arc::new(InProcessConsensus::new(Arc::clone(&committer)));
    let service = committer.into_service();
    let server_task = mz_ore::task::spawn(|| "persist_committer::grpc_server", async move {
        tracing::info!(listen_addr = %listen_addr, "persist committer gRPC server listening");
        let res = Server::builder()
            .add_service(service)
            .serve(listen_addr)
            .await;
        if let Err(e) = res {
            tracing::error!(error = %e, "persist committer gRPC server exited");
        }
    });

    let loopback_url = format!("http://{}", listen_addr);
    let loopback_channel = Endpoint::from_shared(loopback_url)
        .context("building loopback endpoint")?
        .connect_lazy();

    Ok(CommitterHandle {
        in_process_consensus,
        loopback_channel,
        _server_task: server_task.abort_on_drop(),
        _refresh_task: refresh_task.abort_on_drop(),
        _heartbeat_task: heartbeat_task.abort_on_drop(),
    })
}
