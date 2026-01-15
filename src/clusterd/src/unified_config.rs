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
use std::sync::Arc;

use mz_cluster_client::client::TryIntoProtocolNonce;
use mz_compute::metrics::ComputeMetrics;
use mz_compute::server::ComputeInstanceContext;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage::metrics::StorageMetrics;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_client::client::{StorageCommand, StorageResponse};
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use serde::{Deserialize, Serialize};
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
