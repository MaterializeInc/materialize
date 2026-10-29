// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deployment-scoped process management, separate from SQL coordination.

mod collections;
mod http;
mod metrics;
mod observations;
pub mod provisioning;
pub mod pubsub;
mod runtime;
mod startup;
mod status;

pub use runtime::run;
pub use startup::open_catalog;

/// Completes fresh deployment initialization before provisioning its children.
///
/// Requires explicit unfinished controller provenance and writable initializer
/// admission. Completed and unmarked environments cannot enter this path. It
/// starts neither a SQL coordinator nor shared maintenance.
pub async fn initialize_fresh(
    mut config: mz_catalog::config::Config<'_>,
    oracle: &(dyn mz_timestamp_oracle::TimestampOracle<mz_repr::Timestamp> + Send + Sync),
) -> Result<mz_catalog::catalog::Catalog, mz_catalog::catalog::CatalogError> {
    if config.storage.is_read_only() || config.storage.is_savepoint() || config.storage.is_join() {
        return Err(anyhow::anyhow!(
            "controller initialization requires writable initializer admission"
        )
        .into());
    }
    if !config.storage.controller_initialization_pending().await? {
        return Err(anyhow::anyhow!(
            "controller initialization requires explicit unfinished provenance"
        )
        .into());
    }
    let persist = config.state.persist_client.clone();
    let metrics = config.metrics_registry.clone();
    mz_catalog::catalog::Catalog::open(config)
        .await?
        .complete_fresh_deployment_initialization(persist, &metrics, oracle)
        .await
}

fn serialize_id<T: std::fmt::Display, S: serde::Serializer>(
    id: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(id)
}

/// Desired adapter resources and observations, without credential-bearing launch inputs.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AdapterInventoryEntry {
    #[serde(serialize_with = "serialize_id")]
    pub id: mz_repr::AdapterId,
    pub name: String,
    pub size: String,
    pub generation: u64,
    pub service: String,
    pub resources: Option<provisioning::AdapterAllocation>,
    pub endpoints: std::collections::BTreeMap<String, Vec<String>>,
    pub observed_processes: Vec<mz_orchestrator::ServiceEvent>,
    pub readiness: Option<observations::Readiness>,
    pub provisioning_error: Option<String>,
}

/// Calculates desired adapter inventory without contacting or changing the backend.
pub fn desired_adapters(
    catalog: &mz_catalog::catalog::Catalog,
    launch: &provisioning::AdapterProvisioningConfig,
    sizes: &std::collections::BTreeMap<String, provisioning::AdapterAllocation>,
) -> Vec<AdapterInventoryEntry> {
    catalog
        .state()
        .adapter_declarations()
        .values()
        .map(|declaration| {
            let resources = sizes.get(&declaration.size).cloned();
            AdapterInventoryEntry {
                id: declaration.id,
                name: declaration.name.clone(),
                size: declaration.size.clone(),
                generation: launch.generation,
                service: provisioning::service_name(declaration.id, launch.generation),
                provisioning_error: resources.is_none().then(|| "unknown adapter size".into()),
                resources,
                endpoints: Default::default(),
                observed_processes: Vec::new(),
                readiness: None,
            }
        })
        .collect()
}

/// Enacts committed adapter inventory in this deployment's private namespace.
/// Empty inventory is valid. Assigned endpoints do not establish readiness.
pub async fn reconcile_adapters(
    catalog: &mut mz_catalog::catalog::Catalog,
    orchestrator: &dyn mz_orchestrator::NamespacedOrchestrator,
    launch: &provisioning::AdapterProvisioningConfig,
    sizes: &std::collections::BTreeMap<String, provisioning::AdapterAllocation>,
) -> anyhow::Result<Vec<AdapterInventoryEntry>> {
    // List before refreshing membership so a concurrent CREATE cannot look orphaned.
    let observed = orchestrator.list_services().await?;
    catalog.sync_to_current_updates().await?;
    anyhow::ensure!(
        catalog.storage().await.get_deployment_generation().await? == launch.generation,
        "adapter launch configuration belongs to a different deployment"
    );
    let mut inventory = desired_adapters(catalog, launch, sizes);
    let desired: std::collections::BTreeSet<_> = inventory
        .iter()
        .map(|entry| entry.service.clone())
        .collect();
    for service in observed {
        if service_in_generation(&service, launch.generation) && !desired.contains(&service) {
            orchestrator.drop_service(&service)?;
        }
    }
    for entry in &mut inventory {
        let Some(allocation) = &entry.resources else {
            continue;
        };
        match launch.service_config(entry.id, &entry.name, &entry.size, allocation) {
            Ok(config) => {
                let ports = config.ports.clone();
                match orchestrator.ensure_service(&entry.service, config) {
                    Ok(service) => {
                        for port in ports {
                            entry
                                .endpoints
                                .insert(port.name.clone(), service.addresses(&port.name));
                        }
                    }
                    // Backend errors may contain credential-bearing launch inputs.
                    Err(_) => {
                        entry.provisioning_error =
                            Some("backend rejected adapter provisioning".into())
                    }
                }
            }
            Err(_) => {
                entry.provisioning_error = Some("invalid adapter launch configuration".into())
            }
        }
    }
    orchestrator.flush().await?;
    Ok(inventory)
}

fn service_in_generation(service: &str, generation: u64) -> bool {
    service
        .strip_prefix("adapter-")
        .and_then(|name| name.strip_suffix(&format!("-gen-{generation}")))
        .is_some_and(|id| id.parse::<mz_repr::AdapterId>().is_ok())
}

#[cfg(test)]
mod tests {
    #[mz_ore::test]
    fn adapter_cleanup_is_deployment_scoped() {
        for service in ["adapter-a1-gen-4", "adapter-a92-gen-4"] {
            assert!(super::service_in_generation(service, 4));
        }
        for service in [
            "adapter-a1-gen-3",
            "adapter-a1-gen-40",
            "cluster-u1-replica-u1-gen-4",
            "adapter-u1-gen-4",
            "adapter-a1-gen-4-extra",
        ] {
            assert!(!super::service_in_generation(service, 4));
        }
    }
}

/// Desired replica resources and backend observations. Launch arguments stay private.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplicaInventoryEntry {
    #[serde(serialize_with = "serialize_id")]
    pub cluster_id: mz_controller_types::ClusterId,
    #[serde(serialize_with = "serialize_id")]
    pub replica_id: mz_controller_types::ReplicaId,
    pub cluster_name: String,
    pub replica_name: String,
    pub generation: u64,
    pub service: String,
    pub resources: mz_controller_types::clusters::ManagedReplicaLocation,
    #[serde(skip)]
    role: mz_controller_types::clusters::ClusterRole,
    pub endpoints: std::collections::BTreeMap<String, Vec<String>>,
    pub observed_processes: Vec<mz_orchestrator::ServiceEvent>,
    pub provisioning_error: Option<String>,
}

/// Calculates the process inventory committed by adapter-owned replica policy.
pub fn desired_replicas(
    catalog: &mz_catalog::catalog::Catalog,
    generation: u64,
) -> Vec<ReplicaInventoryEntry> {
    use mz_controller::clusters::{ReplicaLocation, ReplicaServiceName};
    let mut desired = Vec::new();
    for cluster in catalog.clusters() {
        for replica in cluster.replicas() {
            if let ReplicaLocation::Managed(location) = &replica.config.location {
                let service = ReplicaServiceName {
                    cluster_id: cluster.id,
                    replica_id: replica.replica_id,
                    generation,
                }
                .to_string();
                desired.push(ReplicaInventoryEntry {
                    cluster_id: cluster.id,
                    replica_id: replica.replica_id,
                    cluster_name: cluster.name.clone(),
                    replica_name: replica.name.clone(),
                    generation,
                    service,
                    resources: location.clone(),
                    role: cluster.role(),
                    endpoints: Default::default(),
                    observed_processes: Vec::new(),
                    provisioning_error: None,
                });
            }
        }
    }
    desired
}

/// Enacts committed replica realizations without SQL coordination or replica-set policy.
pub async fn reconcile_replicas(
    catalog: &mut mz_catalog::catalog::Catalog,
    orchestrator: &dyn mz_orchestrator::NamespacedOrchestrator,
    launch: &mz_controller::clusters::provisioning::ReplicaProvisioningConfig<'_>,
) -> anyhow::Result<Vec<ReplicaInventoryEntry>> {
    use mz_controller::clusters::ReplicaServiceName;
    let observed = orchestrator.list_services().await?;
    catalog.sync_to_current_updates().await?;
    anyhow::ensure!(
        catalog.storage().await.get_deployment_generation().await? == launch.deploy_generation,
        "replica launch configuration belongs to a different deployment"
    );
    let mut desired = desired_replicas(catalog, launch.deploy_generation);
    let live: std::collections::BTreeSet<_> =
        desired.iter().map(|entry| entry.service.clone()).collect();
    for service in observed {
        let name: ReplicaServiceName = service.parse()?;
        if name.generation == launch.deploy_generation && !live.contains(&service) {
            orchestrator.drop_service(&service)?;
        }
    }
    let overrides = mz_catalog::compute_config::replica_dyncfg_overrides(catalog);
    let affinity = catalog.system_config().enable_worker_core_affinity();
    for entry in &mut desired {
        let launch = mz_controller::clusters::provisioning::ReplicaProvisioningConfig {
            replica_dyncfg_overrides: overrides
                .get(&entry.cluster_id)
                .and_then(|replicas| replicas.get(&entry.replica_id)),
            ..*launch
        };
        let spec = launch.service_config(
            entry.cluster_id,
            entry.replica_id,
            entry.cluster_name.clone(),
            entry.replica_name.clone(),
            entry.role.clone(),
            entry.resources.clone(),
            affinity,
        )?;
        let ports = spec.ports.clone();
        match orchestrator.ensure_service(&entry.service, spec) {
            Ok(service) => {
                for port in ports {
                    entry
                        .endpoints
                        .insert(port.name.clone(), service.addresses(&port.name));
                }
            }
            Err(_) => {
                entry.provisioning_error = Some("backend rejected replica provisioning".into())
            }
        }
    }
    orchestrator.flush().await?;
    Ok(desired)
}
