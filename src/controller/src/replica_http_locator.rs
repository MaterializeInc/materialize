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
//! proxy HTTP requests to clusterd internal endpoints without requiring
//! direct network access to the clusterd pods.

use std::collections::BTreeMap;
use std::sync::RwLock;

use mz_controller_types::{ClusterId, ReplicaId};

/// Tracks HTTP addresses for cluster replica processes.
#[derive(Debug, Default)]
pub struct ReplicaHttpLocator {
    /// Maps (cluster_id, replica_id) to a list of process HTTP addresses.
    replica_addresses: RwLock<BTreeMap<(ClusterId, ReplicaId), Vec<String>>>,
}

impl ReplicaHttpLocator {
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
        let guard = self.replica_addresses.read().expect("lock poisoned");
        let addrs = guard.get(&(cluster_id, replica_id))?;
        addrs.get(process).cloned()
    }

    /// Registers a service for a replica.
    ///
    /// Called by the controller when a managed replica is provisioned.
    pub fn register_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        addresses: Vec<String>,
    ) {
        let mut guard = self.replica_addresses.write().expect("lock poisoned");
        guard.insert((cluster_id, replica_id), addresses);
    }

    /// Removes a replica from the locator.
    ///
    /// Called by the controller when a replica is dropped.
    pub(crate) fn remove_replica(&self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let mut guard = self.replica_addresses.write().expect("lock poisoned");
        guard.remove(&(cluster_id, replica_id));
    }
}
