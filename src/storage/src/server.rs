// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::sync::{Arc, Mutex};

use mz_cluster::client::{ClusterClient, ClusterSpec};
use mz_cluster_client::client::TimelyConfig;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage_client::client::{StorageClient, StorageCommand, StorageResponse};
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use timely::communication::Allocate;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::metrics::StorageMetrics;
use crate::storage_state::{StorageInstanceContext, Worker};

/// Configures a dataflow server.
#[derive(Clone)]
struct Config {
    /// `persist` client cache.
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    pub tracing_handle: Arc<TracingHandle>,
    /// Function to get wall time now.
    pub now: NowFn,
    /// Configuration for source and sink connection.
    pub connection_context: ConnectionContext,
    /// Other configuration for storage instances.
    pub instance_context: StorageInstanceContext,

    /// Metrics for storage
    pub metrics: StorageMetrics,
    /// Shared rocksdb write buffer manager
    pub shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
}

/// Initiates a timely dataflow computation, processing storage commands.
pub async fn serve(
    timely_config: TimelyConfig,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    now: NowFn,
    connection_context: ConnectionContext,
    instance_context: StorageInstanceContext,
) -> Result<impl Fn() -> Box<dyn StorageClient> + use<>, anyhow::Error> {
    let config = Config {
        persist_clients,
        txns_ctx,
        tracing_handle,
        now,
        connection_context,
        instance_context,
        metrics: StorageMetrics::register_with(metrics_registry),
        // The shared RocksDB `WriteBufferManager` is shared between the workers.
        // It protects (behind a shared mutex) a `Weak` that will be upgraded and shared when the
        // first worker attempts to initialize it.
        shared_rocksdb_write_buffer_manager: Default::default(),
    };
    let tokio_executor = tokio::runtime::Handle::current();

    let timely_container = config.build_cluster(timely_config, tokio_executor).await?;
    let timely_container = Arc::new(Mutex::new(timely_container));

    let client_builder = move || {
        let client = ClusterClient::new(Arc::clone(&timely_container));
        let client: Box<dyn StorageClient> = Box::new(client);
        client
    };

    Ok(client_builder)
}

impl ClusterSpec for Config {
    type Command = StorageCommand;
    type Response = StorageResponse;

    fn run_worker<A: Allocate + 'static>(
        &self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            Uuid,
            crossbeam_channel::Receiver<StorageCommand>,
            mpsc::UnboundedSender<StorageResponse>,
        )>,
    ) {
        Worker::new(
            timely_worker,
            client_rx,
            self.metrics.clone(),
            self.now.clone(),
            self.connection_context.clone(),
            self.instance_context.clone(),
            Arc::clone(&self.persist_clients),
            self.txns_ctx.clone(),
            Arc::clone(&self.tracing_handle),
            self.shared_rocksdb_write_buffer_manager.clone(),
        )
        .run();
    }
}
