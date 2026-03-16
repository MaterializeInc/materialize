// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helper for constructing a [`PersistClient`] with in-process pubsub and
//! optional latency injection.
//!
//! In production persist, writers notify listeners about new batches via a
//! pubsub service. Without pubsub, the listener must poll, adding latency
//! proportional to the polling interval. This module creates an in-process
//! pubsub server and wires it into the `PersistClient`, so that writes from
//! the acceptor immediately notify the learner's `Listen`.
//!
//! The optional [`LatencyBlob`] wrapper can simulate storage round-trip times
//! (S3 Express, S3 Standard, etc.) for benchmarking.

use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::location::Blob;
use mz_persist::mem::{MemBlob, MemBlobConfig, MemConsensus};
use mz_dyncfg::ConfigUpdates;
use mz_persist_client::cache::StateCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_persist_client::rpc::PersistGrpcPubSubServer;
use mz_persist_client::stats::STATS_COLLECTION_ENABLED;
use mz_persist_client::PersistClient;

use crate::LatencyProfile;

use super::latency_blob::LatencyBlob;

/// Configuration for building a [`PersistClient`] with in-process pubsub.
pub struct PersistClientConfig {
    /// Latency profile for blob operations. `Zero` = no added latency.
    pub latency_profile: LatencyProfile,
}

impl Default for PersistClientConfig {
    fn default() -> Self {
        PersistClientConfig {
            latency_profile: LatencyProfile::Zero,
        }
    }
}

/// Build a `PersistClient` backed by in-memory storage with an in-process
/// pubsub server. The returned `PersistClient` can be cloned and shared
/// between an acceptor and a learner — they will share the same underlying
/// blob, consensus, and pubsub, so writes immediately notify listeners.
///
/// When `latency_profile` is not `Zero`, a [`LatencyBlob`] wrapper is
/// inserted to simulate storage round-trip times.
pub fn new_persist_client(config: PersistClientConfig) -> PersistClient {
    let registry = MetricsRegistry::new();
    let mut persist_cfg = PersistConfig::new_for_tests();
    // Honor PERSIST_ISOLATED_RUNTIME_THREADS if set, so benchmarks can
    // constrain total thread usage (e.g. to simulate 2-core deployments).
    if let Ok(val) = std::env::var("PERSIST_ISOLATED_RUNTIME_THREADS") {
        if let Ok(n) = val.parse::<usize>() {
            persist_cfg.isolated_runtime_worker_threads = n;
        }
    }
    // The shared log's learner reads every update in order — there's no
    // pushdown filtering, so stats are computed on writes and never consulted.
    // Disable stats collection to avoid the overhead.
    let mut updates = ConfigUpdates::default();
    updates.add(&STATS_COLLECTION_ENABLED, false);
    updates.apply(&persist_cfg);

    let metrics = Arc::new(Metrics::new(&persist_cfg, &registry));

    // In-process pubsub server — no network, no serde.
    let pubsub_server = PersistGrpcPubSubServer::new(&persist_cfg, &registry);
    let pubsub_conn = pubsub_server.new_same_process_connection();

    let state_cache = Arc::new(StateCache::new(
        &persist_cfg,
        Arc::clone(&metrics),
        Arc::clone(&pubsub_conn.sender),
    ));

    // Subscribe the state cache to pubsub diffs.
    let _pubsub_receiver_task =
        mz_persist_client::rpc::subscribe_state_cache_to_pubsub(
            Arc::clone(&state_cache),
            pubsub_conn.receiver,
        );
    // Leak the task handle so it runs for the lifetime of the process.
    // In a real deployment this would be managed by the runtime.
    std::mem::forget(_pubsub_receiver_task);

    // In-memory blob, optionally wrapped with latency injection.
    let blob: Arc<dyn Blob> = {
        let mem_blob = MemBlob::open(MemBlobConfig::new(false));
        match config.latency_profile {
            LatencyProfile::Zero => Arc::new(mem_blob),
            profile => Arc::new(LatencyBlob::new(Box::new(mem_blob), profile)),
        }
    };

    // In-memory consensus.
    let consensus = Arc::new(MemConsensus::default());

    let isolated_runtime = Arc::new(
        mz_persist_client::async_runtime::IsolatedRuntime::new(
            &MetricsRegistry::new(),
            Some(persist_cfg.isolated_runtime_worker_threads),
        ),
    );

    PersistClient::new(
        persist_cfg,
        blob,
        consensus,
        metrics,
        isolated_runtime,
        state_cache,
        pubsub_conn.sender,
    )
    .expect("in-mem persist client creation should not fail")
}
