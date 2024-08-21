// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::sync::Arc;
use std::thread::Thread;

use mz_cluster::server::TimelyContainerRef;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage_client::client::{StorageClient, StorageCommand, StorageResponse};
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use timely::communication::initialize::WorkerGuards;
use timely::worker::Worker as TimelyWorker;

use crate::metrics::StorageMetrics;
use crate::storage_state::{StorageInstanceContext, Worker};

/// Configures a dataflow server.
#[derive(Clone)]
pub struct Config {
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

/// A handle to a running dataflow server.
///
/// Dropping this object will block until the dataflow computation ceases.
pub struct Server {
    _worker_guards: WorkerGuards<()>,
}

/// Initiates a timely dataflow computation, processing storage commands.
pub fn serve(
    generic_config: mz_cluster::server::ClusterConfig,
    now: NowFn,
    connection_context: ConnectionContext,
    instance_context: StorageInstanceContext,
) -> Result<
    (
        TimelyContainerRef<StorageCommand, StorageResponse, Thread>,
        impl Fn() -> Box<dyn StorageClient>,
    ),
    anyhow::Error,
> {
    // Various metrics related things.
    let metrics = StorageMetrics::register_with(&generic_config.metrics_registry);

    let shared_rocksdb_write_buffer_manager = Default::default();

    let config = Config {
        now,
        connection_context,
        instance_context,
        metrics,
        // The shared RocksDB `WriteBufferManager` is shared between the workers.
        // It protects (behind a shared mutex) a `Weak` that will be upgraded and shared when the first worker attempts to initialize it.
        shared_rocksdb_write_buffer_manager,
    };

    let (timely_container, client_builder) = mz_cluster::server::serve::<
        Config,
        StorageCommand,
        StorageResponse,
    >(generic_config, config)?;
    let client_builder = {
        move || {
            let client: Box<dyn StorageClient> = client_builder();
            client
        }
    };

    Ok((timely_container, client_builder))
}

impl mz_cluster::types::AsRunnableWorker<StorageCommand, StorageResponse> for Config {
    type Activatable = std::thread::Thread;
    fn build_and_run<A: timely::communication::Allocate>(
        config: Self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            crossbeam_channel::Receiver<StorageCommand>,
            tokio::sync::mpsc::UnboundedSender<StorageResponse>,
            tokio::sync::mpsc::UnboundedSender<std::thread::Thread>,
        )>,
        persist_clients: Arc<PersistClientCache>,
        txns_ctx: TxnsContext,
        tracing_handle: Arc<TracingHandle>,
    ) {
        Worker::new(
            timely_worker,
            client_rx,
            config.metrics,
            config.now.clone(),
            config.connection_context,
            config.instance_context,
            persist_clients,
            txns_ctx,
            tracing_handle,
            config.shared_rocksdb_write_buffer_manager,
        )
        .run();
    }
}
