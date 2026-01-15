// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unified cluster configuration for compute and storage.
//!
//! This module provides a unified [`ClusterSpec`] implementation that allows running
//! both compute and storage dataflows within a single Timely runtime.

use std::fmt;
use std::sync::{Arc, Mutex};

use anyhow::Error;
use async_trait::async_trait;
use mz_cluster::client::{ClusterClient, ClusterSpec, TimelyContainer};
use mz_cluster_client::client::{TimelyConfig, TryIntoProtocolNonce};
use mz_compute::metrics::ComputeMetrics;
use mz_compute::server::ComputeInstanceContext;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_storage::metrics::StorageMetrics;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_client::client::{StorageClient, StorageCommand, StorageResponse};
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use uuid::Uuid;

/// A command that can be either a compute or storage command.
///
/// This enum multiplexes commands from both the compute and storage controllers
/// into a single command stream for the unified worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterCommand<T = mz_repr::Timestamp> {
    /// A compute command from the compute controller.
    Compute(ComputeCommand<T>),
    /// A storage command from the storage controller.
    Storage(StorageCommand<T>),
}

impl TryIntoProtocolNonce for ClusterCommand {
    fn try_into_protocol_nonce(self) -> Result<Uuid, Self> {
        match self {
            ClusterCommand::Compute(cmd) => cmd
                .try_into_protocol_nonce()
                .map_err(ClusterCommand::Compute),
            ClusterCommand::Storage(cmd) => cmd
                .try_into_protocol_nonce()
                .map_err(ClusterCommand::Storage),
        }
    }
}

impl<T: fmt::Debug> fmt::Display for ClusterCommand<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterCommand::Compute(cmd) => write!(f, "Compute({:?})", cmd),
            ClusterCommand::Storage(cmd) => write!(f, "Storage({:?})", cmd),
        }
    }
}

/// A response that can be either a compute or storage response.
///
/// This enum multiplexes responses from both the compute and storage domains
/// into a single response stream for the controllers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterResponse<T = mz_repr::Timestamp> {
    /// A compute response for the compute controller.
    Compute(ComputeResponse<T>),
    /// A storage response for the storage controller.
    Storage(StorageResponse<T>),
}

/// Simple partitioned state for unified commands/responses.
///
/// This is a minimal implementation that passes through commands and responses
/// without any sophisticated merging. For the unified runtime, response merging
/// is not needed because the unified worker handles partitioning internally.
#[derive(Debug)]
pub struct PartitionedUnifiedState {
    /// Number of partitions.
    parts: usize,
}

impl Partitionable<ClusterCommand, ClusterResponse> for (ClusterCommand, ClusterResponse) {
    type PartitionedState = PartitionedUnifiedState;

    fn new(parts: usize) -> PartitionedUnifiedState {
        PartitionedUnifiedState { parts }
    }
}

impl PartitionedState<ClusterCommand, ClusterResponse> for PartitionedUnifiedState {
    fn split_command(&mut self, command: ClusterCommand) -> Vec<Option<ClusterCommand>> {
        // Forward all commands to all workers.
        // The unified worker handles command routing internally.
        vec![Some(command); self.parts]
    }

    fn absorb_response(
        &mut self,
        _shard_id: usize,
        response: ClusterResponse,
    ) -> Option<Result<ClusterResponse, anyhow::Error>> {
        // Pass through all responses.
        // Response merging is not needed for the unified runtime since the
        // unified worker handles response routing internally.
        Some(Ok(response))
    }
}

/// Configuration for the unified compute and storage cluster.
///
/// This struct holds all the configuration and shared resources needed to
/// initialize both compute and storage workers within a single Timely runtime.
#[derive(Clone)]
pub struct UnifiedConfig {
    // Shared resources
    /// `persist` client cache, shared between compute and storage.
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    pub tracing_handle: Arc<TracingHandle>,

    // Compute-specific configuration
    /// Metrics exposed by compute replicas.
    pub compute_metrics: ComputeMetrics,
    /// Compute instance context (scratch directory, core affinity, connection context).
    pub compute_instance_context: ComputeInstanceContext,

    // Storage-specific configuration
    /// Metrics for storage objects.
    pub storage_metrics: StorageMetrics,
    /// Function to get wall time now.
    pub now: NowFn,
    /// Configuration for source and sink connections.
    pub connection_context: ConnectionContext,
    /// Storage instance context (scratch directory, memory limits).
    pub storage_instance_context: StorageInstanceContext,
    /// Shared RocksDB write buffer manager.
    pub shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
}

/// Client builders returned by the `serve` function.
///
/// This struct contains functions to create both compute and storage clients
/// that share the same underlying unified cluster.
pub struct UnifiedClientBuilders {
    /// Builds compute clients connected to the unified cluster.
    pub compute: Box<dyn Fn() -> Box<dyn ComputeClient> + Send>,
    /// Builds storage clients connected to the unified cluster.
    pub storage: Box<dyn Fn() -> Box<dyn StorageClient> + Send>,
}

/// Starts a unified Timely cluster serving both compute and storage.
///
/// Returns client builders for both compute and storage that connect to the
/// same underlying unified cluster.
pub async fn serve(
    timely_config: TimelyConfig,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    now: NowFn,
    connection_context: ConnectionContext,
    compute_instance_context: ComputeInstanceContext,
    storage_instance_context: StorageInstanceContext,
) -> Result<UnifiedClientBuilders, Error> {
    let config = UnifiedConfig {
        persist_clients,
        txns_ctx,
        tracing_handle,
        compute_metrics: ComputeMetrics::register_with(metrics_registry),
        compute_instance_context,
        storage_metrics: StorageMetrics::register_with(metrics_registry),
        now,
        connection_context,
        storage_instance_context,
        shared_rocksdb_write_buffer_manager: Default::default(),
    };

    let tokio_executor = Handle::current();
    let timely_container = config.build_cluster(timely_config, tokio_executor).await?;
    let timely_container = Arc::new(Mutex::new(timely_container));

    // Create compute client builder
    let compute_timely_container = Arc::clone(&timely_container);
    let compute_builder: Box<dyn Fn() -> Box<dyn ComputeClient> + Send> = Box::new(move || {
        let client: Box<dyn ComputeClient> =
            Box::new(ComputeAdapter::new(Arc::clone(&compute_timely_container)));
        client
    });

    // Create storage client builder
    let storage_timely_container = Arc::clone(&timely_container);
    let storage_builder: Box<dyn Fn() -> Box<dyn StorageClient> + Send> = Box::new(move || {
        let client: Box<dyn StorageClient> =
            Box::new(StorageAdapter::new(Arc::clone(&storage_timely_container)));
        client
    });

    Ok(UnifiedClientBuilders {
        compute: compute_builder,
        storage: storage_builder,
    })
}

/// Adapter that translates compute protocol to unified protocol.
///
/// This adapter wraps a `ClusterClient<UnifiedConfig>` and presents it as a
/// `ComputeClient`, translating commands and responses between the two protocols.
struct ComputeAdapter {
    inner: ClusterClient<UnifiedConfig>,
}

impl ComputeAdapter {
    fn new(timely_container: Arc<Mutex<TimelyContainer<UnifiedConfig>>>) -> Self {
        Self {
            inner: ClusterClient::new(timely_container),
        }
    }
}

impl std::fmt::Debug for ComputeAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeAdapter")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl GenericClient<ComputeCommand, ComputeResponse> for ComputeAdapter {
    async fn send(&mut self, cmd: ComputeCommand) -> Result<(), Error> {
        self.inner.send(ClusterCommand::Compute(cmd)).await
    }

    async fn recv(&mut self) -> Result<Option<ComputeResponse>, Error> {
        loop {
            match self.inner.recv().await? {
                Some(ClusterResponse::Compute(resp)) => return Ok(Some(resp)),
                Some(ClusterResponse::Storage(_)) => {
                    // Skip storage responses - they go to the storage adapter
                    continue;
                }
                None => return Ok(None),
            }
        }
    }
}

/// Adapter that translates storage protocol to unified protocol.
///
/// This adapter wraps a `ClusterClient<UnifiedConfig>` and presents it as a
/// `StorageClient`, translating commands and responses between the two protocols.
struct StorageAdapter {
    inner: ClusterClient<UnifiedConfig>,
}

impl StorageAdapter {
    fn new(timely_container: Arc<Mutex<TimelyContainer<UnifiedConfig>>>) -> Self {
        Self {
            inner: ClusterClient::new(timely_container),
        }
    }
}

impl std::fmt::Debug for StorageAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageAdapter")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl GenericClient<StorageCommand, StorageResponse> for StorageAdapter {
    async fn send(&mut self, cmd: StorageCommand) -> Result<(), Error> {
        self.inner.send(ClusterCommand::Storage(cmd)).await
    }

    async fn recv(&mut self) -> Result<Option<StorageResponse>, Error> {
        loop {
            match self.inner.recv().await? {
                Some(ClusterResponse::Storage(resp)) => return Ok(Some(resp)),
                Some(ClusterResponse::Compute(_)) => {
                    // Skip compute responses - they go to the compute adapter
                    continue;
                }
                None => return Ok(None),
            }
        }
    }
}
