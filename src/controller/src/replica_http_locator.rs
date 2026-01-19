// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tracks HTTP addresses for cluster replica processes.
//!
//! This module provides the [`ReplicaHttpLocator`] which maintains an in-memory
//! mapping of cluster replica HTTP addresses. This is used by environmentd to
//! proxy HTTP requests to clusterd internal endpoints (profiling, metrics,
//! tracing) without requiring direct network access to the clusterd pods.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::Service;

/// Tracks HTTP addresses for cluster replica processes.
///
/// Each replica can have multiple processes (based on `scale`), and each
/// process has its own HTTP endpoint. This struct maintains an in-memory
/// mapping that is updated by the controller when replicas are provisioned
/// or dropped.
///
/// For managed replicas, we store a reference to the orchestrator's Service
/// object and query `tcp_addresses()` lazily. This is necessary because the
/// process orchestrator allocates TCP proxy ports asynchronously, so the
/// addresses may not be available immediately after `ensure_service()` returns.
#[derive(Default)]
pub struct ReplicaHttpLocator {
    /// Maps (cluster_id, replica_id) -> Service reference.
    /// For managed replicas, we store the Service and call tcp_addresses() lazily.
    /// For unmanaged replicas, we store None (they don't have HTTP addresses).
    services: RwLock<BTreeMap<(ClusterId, ReplicaId), Option<Arc<dyn Service>>>>,
}

impl std::fmt::Debug for ReplicaHttpLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaHttpLocator")
            .field("services", &"<services>")
            .finish()
    }
}

impl ReplicaHttpLocator {
    /// Creates a new empty `ReplicaHttpLocator`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the HTTP address for a specific process of a replica.
    ///
    /// Returns `None` if the replica is not found, the process index is out of
    /// bounds, or the addresses are not yet available.
    pub fn get_http_addr(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process: usize,
    ) -> Option<String> {
        let guard = self.services.read().expect("lock poisoned");
        let service = guard.get(&(cluster_id, replica_id))?.as_ref()?;
        let addrs = service.tcp_addresses("internal-http");
        addrs.get(process).cloned()
    }

    /// Returns the number of processes for a replica, or `None` if not found.
    pub fn process_count(&self, cluster_id: ClusterId, replica_id: ReplicaId) -> Option<usize> {
        let guard = self.services.read().expect("lock poisoned");
        let service = guard.get(&(cluster_id, replica_id))?.as_ref()?;
        Some(service.tcp_addresses("internal-http").len())
    }

    /// Registers a service for a managed replica.
    ///
    /// Called by the controller when a managed replica is provisioned.
    /// The TCP addresses will be queried lazily via `tcp_addresses()`.
    pub fn register_service(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        service: Arc<dyn Service>,
    ) {
        let mut guard = self.services.write().expect("lock poisoned");
        guard.insert((cluster_id, replica_id), Some(service));
    }

    /// Registers an unmanaged replica (which has no HTTP addresses).
    ///
    /// Called by the controller when an unmanaged replica is created.
    pub fn register_unmanaged(&self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let mut guard = self.services.write().expect("lock poisoned");
        guard.insert((cluster_id, replica_id), None);
    }

    /// Removes a replica from the locator.
    ///
    /// Called by the controller when a replica is dropped.
    pub fn remove_replica(&self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let mut guard = self.services.write().expect("lock poisoned");
        guard.remove(&(cluster_id, replica_id));
    }

    /// Lists all registered replica IDs.
    ///
    /// Returns a list of (cluster_id, replica_id, process_count) tuples.
    /// Use the catalog to look up human-readable names.
    pub fn list_replicas(&self) -> Vec<(ClusterId, ReplicaId, usize)> {
        let guard = self.services.read().expect("lock poisoned");
        guard
            .iter()
            .map(|((cluster_id, replica_id), service)| {
                let process_count = service
                    .as_ref()
                    .map(|s| s.tcp_addresses("internal-http").len())
                    .unwrap_or(0);
                (*cluster_id, *replica_id, process_count)
            })
            .collect()
    }
}
