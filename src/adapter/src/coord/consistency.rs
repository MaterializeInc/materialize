// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Internal consistency checks that validate invariants of [`Coordinator`].

use super::Coordinator;
use crate::catalog::consistency::CatalogInconsistencies;
use mz_adapter_types::connection::ConnectionIdType;
use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, Source};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::instrument;
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogCluster, CatalogClusterReplica};
use serde::Serialize;

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct CoordinatorInconsistencies {
    /// Inconsistencies found in the catalog.
    catalog_inconsistencies: Box<CatalogInconsistencies>,
    /// Inconsistencies found in read capabilities.
    read_capabilities: Vec<ReadCapabilitiesInconsistency>,
    /// Inconsistencies found with our map of active webhooks.
    active_webhooks: Vec<ActiveWebhookInconsistency>,
    /// Inconsistencies found with our map of cluster statuses.
    cluster_statuses: Vec<ClusterStatusInconsistency>,
}

impl CoordinatorInconsistencies {
    pub fn is_empty(&self) -> bool {
        self.catalog_inconsistencies.is_empty()
            && self.read_capabilities.is_empty()
            && self.active_webhooks.is_empty()
            && self.cluster_statuses.is_empty()
    }
}

impl Coordinator {
    /// Checks the [`Coordinator`] to make sure we're internally consistent.
    #[instrument(name = "coord::check_consistency")]
    pub fn check_consistency(&self) -> Result<(), CoordinatorInconsistencies> {
        let mut inconsistencies = CoordinatorInconsistencies::default();

        if let Err(catalog_inconsistencies) = self.catalog().state().check_consistency() {
            inconsistencies.catalog_inconsistencies = catalog_inconsistencies;
        }

        if let Err(read_capabilities) = self.check_read_capabilities() {
            inconsistencies.read_capabilities = read_capabilities;
        }

        if let Err(active_webhooks) = self.check_active_webhooks() {
            inconsistencies.active_webhooks = active_webhooks;
        }

        if let Err(cluster_statuses) = self.check_cluster_statuses() {
            inconsistencies.cluster_statuses = cluster_statuses;
        }

        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(inconsistencies)
        }
    }

    /// # Invariants:
    ///
    /// * Read capabilities should reference known objects.
    ///
    fn check_read_capabilities(&self) -> Result<(), Vec<ReadCapabilitiesInconsistency>> {
        let mut read_capabilities_inconsistencies = Vec::new();
        for (gid, _) in &self.storage_read_capabilities {
            if self.catalog().try_get_entry(gid).is_none() {
                read_capabilities_inconsistencies
                    .push(ReadCapabilitiesInconsistency::Storage(gid.clone()));
            }
        }
        for (gid, _) in &self.compute_read_capabilities {
            if !gid.is_transient() && self.catalog().try_get_entry(gid).is_none() {
                read_capabilities_inconsistencies
                    .push(ReadCapabilitiesInconsistency::Compute(gid.clone()));
            }
        }
        for (conn_id, _) in &self.txn_read_holds {
            if !self.active_conns.contains_key(conn_id) {
                read_capabilities_inconsistencies.push(ReadCapabilitiesInconsistency::Transaction(
                    conn_id.unhandled(),
                ));
            }
        }

        if read_capabilities_inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(read_capabilities_inconsistencies)
        }
    }

    /// # Invariants
    ///
    /// * All [`GlobalId`]s in the `active_webhooks` map should reference known webhook sources.
    ///
    fn check_active_webhooks(&self) -> Result<(), Vec<ActiveWebhookInconsistency>> {
        let mut inconsistencies = vec![];
        for (id, _) in &self.active_webhooks {
            let is_webhook = self
                .catalog()
                .try_get_entry(id)
                .map(|entry| entry.item())
                .and_then(|item| {
                    let CatalogItem::Source(Source { data_source, .. }) = &item else {
                        return None;
                    };
                    Some(matches!(data_source, DataSourceDesc::Webhook { .. }))
                })
                .unwrap_or(false);
            if !is_webhook {
                inconsistencies.push(ActiveWebhookInconsistency::NonExistentWebhook(*id));
            }
        }

        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(inconsistencies)
        }
    }

    /// # Invariants
    ///
    /// * All [`ClusterId`]s in the `cluster_replica_statuses` map should reference known clusters.
    /// * All [`ReplicaId`]s in the `cluster_replica_statuses` map should reference known cluster
    /// replicas.
    fn check_cluster_statuses(&self) -> Result<(), Vec<ClusterStatusInconsistency>> {
        let mut inconsistencies = vec![];
        for (cluster_id, replica_status) in &self.cluster_replica_statuses.0 {
            if self.catalog().try_get_cluster(*cluster_id).is_none() {
                inconsistencies.push(ClusterStatusInconsistency::NonExistentCluster(*cluster_id));
            }
            for replica_id in replica_status.keys() {
                if self
                    .catalog()
                    .try_get_cluster_replica(*cluster_id, *replica_id)
                    .is_none()
                {
                    inconsistencies.push(ClusterStatusInconsistency::NonExistentReplica(
                        *cluster_id,
                        *replica_id,
                    ));
                }
            }
        }
        for cluster in self.catalog().clusters() {
            if let Some(cluster_statuses) = self.cluster_replica_statuses.0.get(&cluster.id()) {
                for replica in cluster.replicas() {
                    if !cluster_statuses.contains_key(&replica.replica_id()) {
                        inconsistencies.push(ClusterStatusInconsistency::NonExistentReplicaStatus(
                            cluster.name.clone(),
                            replica.name.clone(),
                            cluster.id(),
                            replica.replica_id(),
                        ));
                    }
                }
            } else {
                inconsistencies.push(ClusterStatusInconsistency::NonExistentClusterStatus(
                    cluster.name.clone(),
                    cluster.id(),
                ));
            }
        }
        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(inconsistencies)
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
enum ReadCapabilitiesInconsistency {
    Storage(GlobalId),
    Compute(GlobalId),
    Transaction(ConnectionIdType),
}

#[derive(Debug, Serialize, PartialEq, Eq)]
enum ActiveWebhookInconsistency {
    NonExistentWebhook(GlobalId),
}

#[derive(Debug, Serialize, PartialEq, Eq)]
enum ClusterStatusInconsistency {
    NonExistentCluster(ClusterId),
    NonExistentReplica(ClusterId, ReplicaId),
    NonExistentClusterStatus(String, ClusterId),
    NonExistentReplicaStatus(String, String, ClusterId, ReplicaId),
}
