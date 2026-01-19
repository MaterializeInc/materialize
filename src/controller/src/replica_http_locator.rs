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
use std::sync::RwLock;

use mz_controller_types::{ClusterId, ReplicaId};

/// Tracks HTTP addresses for cluster replica processes.
///
/// Each replica can have multiple processes (based on `scale`), and each
/// process has its own HTTP endpoint. This struct maintains an in-memory
/// mapping that is updated by the controller when replicas are provisioned
/// or dropped.
#[derive(Debug, Default)]
pub struct ReplicaHttpLocator {
    /// Maps (cluster_id, replica_id) -> list of HTTP addresses (one per process)
    addresses: RwLock<BTreeMap<(ClusterId, ReplicaId), Vec<String>>>,
}

impl ReplicaHttpLocator {
    /// Creates a new empty `ReplicaHttpLocator`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the HTTP address for a specific process of a replica.
    ///
    /// Returns `None` if the replica is not found or the process index is out of bounds.
    pub fn get_http_addr(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process: usize,
    ) -> Option<String> {
        let guard = self.addresses.read().expect("lock poisoned");
        guard
            .get(&(cluster_id, replica_id))
            .and_then(|addrs| addrs.get(process).cloned())
    }

    /// Returns the number of processes for a replica, or `None` if not found.
    pub fn process_count(&self, cluster_id: ClusterId, replica_id: ReplicaId) -> Option<usize> {
        let guard = self.addresses.read().expect("lock poisoned");
        guard.get(&(cluster_id, replica_id)).map(|addrs| addrs.len())
    }

    /// Updates the HTTP addresses for a replica.
    ///
    /// Called by the controller when a replica is provisioned.
    pub fn update_addresses(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        addrs: Vec<String>,
    ) {
        let mut guard = self.addresses.write().expect("lock poisoned");
        guard.insert((cluster_id, replica_id), addrs);
    }

    /// Removes address information for a replica.
    ///
    /// Called by the controller when a replica is dropped.
    pub fn remove_replica(&self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let mut guard = self.addresses.write().expect("lock poisoned");
        guard.remove(&(cluster_id, replica_id));
    }
}
