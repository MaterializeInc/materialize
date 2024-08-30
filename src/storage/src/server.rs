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

use mz_cluster::server::{ClusterClient, ClusterConfig};
use mz_cluster::types::AsRunnableWorker;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage_client::client::{StorageClient, StorageCommand, StorageResponse};
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use timely::communication::initialize::WorkerGuards;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, oneshot};

use crate::metrics::StorageMetrics;
use crate::storage_state::{StorageInstanceContext, Worker};

/// Configures a dataflow server.
#[derive(Clone, derivative::Derivative)]
#[derivative(Debug)]
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
    #[derivative(Debug = "ignore")]
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
    generic_config: ClusterConfig,
    now: NowFn,
    connection_context: ConnectionContext,
    instance_context: StorageInstanceContext,
) -> Box<dyn StorageClient> {
    // Various metrics related things.
    let metrics = StorageMetrics::register_with(&generic_config.metrics_registry);

    let shared_rocksdb_write_buffer_manager = Default::default();

    let config = Config {
        now,
        connection_context,
        instance_context,
        metrics,
        // The shared RocksDB `WriteBufferManager` is shared between the workers.
        // It protects (behind a shared mutex) a `Weak` that will be upgraded and shared when the
        // first worker attempts to initialize it.
        shared_rocksdb_write_buffer_manager,
    };

    let client = ClusterClient::new(generic_config, config);
    Box::new(client)
}

impl AsRunnableWorker<StorageCommand, StorageResponse> for Config {
    type Activatable = Thread;

    fn build_and_run<A: timely::communication::Allocate>(
        config: Self,
        timely_worker: &mut TimelyWorker<A>,
        (command_rx, response_tx, activator_tx): (
            crossbeam_channel::Receiver<StorageCommand>,
            mpsc::UnboundedSender<StorageResponse>,
            oneshot::Sender<Thread>,
        ),
        persist_clients: Arc<PersistClientCache>,
        txns_ctx: TxnsContext,
        tracing_handle: Arc<TracingHandle>,
    ) {
        Worker::new(
            timely_worker,
            config.metrics,
            config.now.clone(),
            config.connection_context,
            config.instance_context,
            persist_clients,
            txns_ctx,
            tracing_handle,
            config.shared_rocksdb_write_buffer_manager,
        )
        .run(command_rx, response_tx, activator_tx);
    }
}
