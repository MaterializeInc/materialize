// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster management.

pub mod provisioning;

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::anyhow;
pub use mz_controller_types::clusters::{
    ClusterRole, ClusterStatus, ManagedReplicaLocation, ReplicaAllocation, ReplicaConfig,
    ReplicaLocation, ReplicaLogging, UnmanagedReplicaLocation,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::NamespacedOrchestrator;
use mz_orchestrator::ServicePort;
use mz_ore::task::AbortOnDropHandle;
use regex::Regex;
use tracing::warn;

use crate::Controller;

/// Identifier of a process within a replica.
pub type ProcessId = u64;

impl Controller {
    /// Registers a committed cluster in local inventory and metrics.
    pub fn create_cluster(&mut self, id: ClusterId, workload_class: Option<String>) {
        self.storage.create_instance(id, workload_class.clone());
        self.metrics
            .set_workload_class(id.to_string(), workload_class);
    }

    /// Updates the workload class for a cluster.
    ///
    /// # Panics
    ///
    /// Panics if the instance does not exist in the local storage inventory.
    pub fn update_cluster_workload_class(&mut self, id: ClusterId, workload_class: Option<String>) {
        self.storage
            .update_instance_workload_class(id, workload_class.clone());
        self.metrics
            .set_workload_class(id.to_string(), workload_class);
    }

    /// Drops the specified cluster.
    ///
    /// # Panics
    ///
    /// Panics if the cluster still has replicas.
    pub fn drop_cluster(&mut self, id: ClusterId) {
        self.storage.drop_instance(id);
        self.metrics.set_workload_class(id.to_string(), None);
    }

    /// Registers committed replica membership and discovers its HTTP addresses.
    /// Controllerd provisions the process independently.
    pub fn create_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        config: ReplicaConfig,
    ) -> Result<(), anyhow::Error> {
        if let ReplicaLocation::Managed(m) = config.location {
            let service = ReplicaServiceName {
                cluster_id,
                replica_id,
                generation: self.deploy_generation,
            }
            .to_string();
            let addresses = |name: &str, port_hint| {
                self.orchestrator.service_addresses(
                    &service,
                    m.allocation.scale,
                    &ServicePort {
                        name: name.into(),
                        port_hint,
                        tcp_listen_ip: None,
                    },
                )
            };
            self.replica_http_locator.register_replica(
                cluster_id,
                replica_id,
                addresses("internal-http", 6878)?,
            );
        }

        self.storage.register_replica(cluster_id, replica_id);

        Ok(())
    }

    /// Drops the specified replica of the specified cluster.
    pub fn drop_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        publish_status_history: bool,
    ) -> Result<(), anyhow::Error> {
        // Remove HTTP addresses from the locator.
        self.replica_http_locator
            .remove_replica(cluster_id, replica_id);

        self.storage
            .drop_replica(cluster_id, replica_id, publish_status_history);
        Ok(())
    }
}

/// Represents the name of a cluster replica service in the orchestrator.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplicaServiceName {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub generation: u64,
}

impl fmt::Display for ReplicaServiceName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ReplicaServiceName {
            cluster_id,
            replica_id,
            generation,
        } = self;
        write!(f, "{cluster_id}-replica-{replica_id}-gen-{generation}")
    }
}

impl FromStr for ReplicaServiceName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        static SERVICE_NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"(?-u)^([us]\d+)-replica-([us]\d+)(?:-gen-(\d+))?$").unwrap()
        });

        let caps = SERVICE_NAME_RE
            .captures(s)
            .ok_or_else(|| anyhow!("invalid service name: {s}"))?;

        Ok(ReplicaServiceName {
            cluster_id: caps.get(1).unwrap().as_str().parse().unwrap(),
            replica_id: caps.get(2).unwrap().as_str().parse().unwrap(),
            // Old versions of Materialize did not include generations in
            // replica service names. Synthesize generation 0 if absent.
            // TODO: remove this in the next version of Materialize.
            generation: caps.get(3).map_or("0", |m| m.as_str()).parse().unwrap(),
        })
    }
}

/// Samples a provisioned service every minute until the returned handle is dropped.
/// Backend errors skip a sample rather than terminating observation.
pub fn spawn_service_metrics_task(
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    service_name: String,
    on_metrics: impl Fn(Vec<mz_orchestrator::ServiceProcessMetrics>) + Send + 'static,
) -> AbortOnDropHandle<()> {
    let task_name = format!("service-metrics-{service_name}");
    mz_ore::task::spawn(|| task_name, async move {
        // metrics-server does not supply resourceVersion for a reliable watch.
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            match orchestrator.fetch_service_metrics(&service_name).await {
                Ok(metrics) => on_metrics(metrics),
                Err(error) => warn!(%service_name, %error, "failed to fetch service metrics"),
            }
        }
    })
    .abort_on_drop()
}
