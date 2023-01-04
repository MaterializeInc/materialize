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
use timely::communication::initialize::WorkerGuards;
use tokio::sync::mpsc;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_service::local::LocalClient;
use mz_storage_client::client::StorageClient;
use mz_storage_client::types::connections::ConnectionContext;

use crate::sink::SinkBaseMetrics;
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
    /// `persist` client cache.
    pub persist_clients: Arc<tokio::sync::Mutex<PersistClientCache>>,
}

/// A handle to a running dataflow server.
///
/// Dropping this object will block until the dataflow computation ceases.
pub struct Server {
    _worker_guards: WorkerGuards<()>,
}

/// Initiates a timely dataflow computation, processing storage commands.
pub fn serve(
    config: Config,
) -> Result<(Server, impl Fn() -> Box<dyn StorageClient>), anyhow::Error> {
    assert!(config.workers > 0);

    // Various metrics related things.
    let source_metrics = SourceBaseMetrics::register_with(&config.metrics_registry);
    let sink_metrics = SinkBaseMetrics::register_with(&config.metrics_registry);
    let decode_metrics = DecodeMetrics::register_with(&config.metrics_registry);
    // Bundle metrics to conceal complexity.
    let metrics_bundle = (source_metrics, sink_metrics, decode_metrics);

    let (client_txs, client_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| crossbeam_channel::unbounded())
        .unzip();
    let client_rxs: Mutex<Vec<_>> = Mutex::new(client_rxs.into_iter().map(Some).collect());

    let tokio_executor = tokio::runtime::Handle::current();
    let now = config.now;

    let worker_guards = timely::execute::execute(config.timely_config, move |timely_worker| {
        let timely_worker_index = timely_worker.index();
        let timely_worker_peers = timely_worker.peers();

        // ensure tokio primitives are available on timely workers
        let _tokio_guard = tokio_executor.enter();

        let client_rx = client_rxs.lock().unwrap()[timely_worker_index % config.workers]
            .take()
            .unwrap();
        let (source_metrics, sink_metrics, decode_metrics) = metrics_bundle.clone();
        let persist_clients = Arc::clone(&config.persist_clients);
        Worker {
            timely_worker,
            client_rx,
            storage_state: StorageState {
                source_uppers: HashMap::new(),
                source_tokens: HashMap::new(),
                decode_metrics,
                reported_frontiers: HashMap::new(),
                ingestions: HashMap::new(),
                exports: HashMap::new(),
                now: now.clone(),
                source_metrics,
                sink_metrics,
                timely_worker_index,
                timely_worker_peers,
                connection_context: config.connection_context.clone(),
                persist_clients,
                sink_tokens: HashMap::new(),
                sink_write_frontiers: HashMap::new(),
                sink_handles: HashMap::new(),
                dropped_ids: Vec::new(),
                source_statistics: HashMap::new(),
            },
        }
        .run()
    })
    .map_err(|e| anyhow!("{}", e))?;
    let worker_threads = worker_guards
        .guards()
        .iter()
        .map(|g| g.thread().clone())
        .collect::<Vec<_>>();
    let client_builder = move || {
        let (command_txs, command_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
            .map(|_| crossbeam_channel::unbounded())
            .unzip();
        let (response_txs, response_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
            .map(|_| mpsc::unbounded_channel())
            .unzip();
        for (client_tx, channels) in client_txs
            .iter()
            .zip(command_rxs.into_iter().zip(response_txs))
        {
            client_tx
                .send(channels)
                .expect("worker should not drop first");
        }
        let client =
            LocalClient::new_partitioned(response_rxs, command_txs, worker_threads.clone());
        let client: Box<dyn StorageClient> = Box::new(client);
        client
    };
    let server = Server {
        _worker_guards: worker_guards,
    };
    Ok((server, client_builder))
}
