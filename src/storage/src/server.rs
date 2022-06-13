// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use mz_persist_client::cache::PersistClientCache;
use timely::communication::initialize::WorkerGuards;
use tokio::sync::mpsc;

use mz_dataflow_types::client::{LocalClient, LocalStorageClient};
use mz_dataflow_types::connections::ConnectionContext;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;

use crate::source::metrics::SourceBaseMetrics;
use crate::storage_state::{StorageState, Worker};
use crate::DecodeMetrics;

/// Configures a dataflow server.
pub struct Config {
    /// The number of worker threads to spawn.
    pub workers: usize,
    /// The Timely configuration
    pub timely_config: timely::Config,
    /// Function to get wall time now.
    pub now: NowFn,
    /// Metrics registry through which dataflow metrics will be reported.
    pub metrics_registry: MetricsRegistry,
    /// Configuration for source and sink connection.
    pub connection_context: ConnectionContext,
}

/// A handle to a running dataflow server.
///
/// Dropping this object will block until the dataflow computation ceases.
pub struct Server {
    _worker_guards: WorkerGuards<()>,
}

/// Initiates a timely dataflow computation, processing materialized commands.
pub fn serve(config: Config) -> Result<(Server, LocalStorageClient), anyhow::Error> {
    assert!(config.workers > 0);

    // Various metrics related things.
    let source_metrics = SourceBaseMetrics::register_with(&config.metrics_registry);
    let decode_metrics = DecodeMetrics::register_with(&config.metrics_registry);
    // Bundle metrics to conceal complexity.
    let metrics_bundle = (source_metrics, decode_metrics);

    // Construct endpoints for each thread that will receive the coordinator's
    // sequenced command stream and send the responses to the coordinator.
    //
    // TODO(benesch): package up this idiom of handing out ownership of N items
    // to the N timely threads that will be spawned. The Mutex<Vec<Option<T>>>
    // is hard to read through.
    let (command_txs, command_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| crossbeam_channel::unbounded())
        .unzip();
    let (response_tx, response_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    // Mutexes around a vector of optional (take-able) pairs of (tx, rx) for worker/client communication.
    let command_channels: Mutex<Vec<_>> = Mutex::new(command_rxs.into_iter().map(Some).collect());
    let storage_response_channels: Mutex<Vec<_>> =
        Mutex::new(response_tx.into_iter().map(Some).collect());

    let tokio_executor = tokio::runtime::Handle::current();
    let now = config.now;
    let persist_clients = PersistClientCache::new(&config.metrics_registry);
    let persist_clients = Arc::new(tokio::sync::Mutex::new(persist_clients));

    let worker_guards = timely::execute::execute(config.timely_config, move |timely_worker| {
        let timely_worker_index = timely_worker.index();
        let timely_worker_peers = timely_worker.peers();

        // ensure tokio primitives are available on timely workers
        let _tokio_guard = tokio_executor.enter();

        let command_rx = command_channels.lock().unwrap()[timely_worker_index % config.workers]
            .take()
            .unwrap();
        let response_tx = storage_response_channels.lock().unwrap()
            [timely_worker_index % config.workers]
            .take()
            .unwrap();
        let (source_metrics, decode_metrics) = metrics_bundle.clone();
        let persist_clients = Arc::clone(&persist_clients);
        Worker {
            timely_worker,
            command_rx,
            storage_state: StorageState {
                source_uppers: HashMap::new(),
                source_tokens: HashMap::new(),
                decode_metrics,
                reported_frontiers: HashMap::new(),
                now: now.clone(),
                source_metrics,
                timely_worker_index,
                timely_worker_peers,
                connection_context: config.connection_context.clone(),
                persist_clients,
            },
            response_tx,
        }
        .run()
    })
    .map_err(|e| anyhow!("{}", e))?;
    let worker_threads = worker_guards
        .guards()
        .iter()
        .map(|g| g.thread().clone())
        .collect::<Vec<_>>();
    let storage_client = LocalClient::new(response_rxs, command_txs, worker_threads);
    let server = Server {
        _worker_guards: worker_guards,
    };
    Ok((server, storage_client))
}
