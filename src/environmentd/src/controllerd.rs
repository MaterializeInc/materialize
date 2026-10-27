// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deployment-scoped process management, separate from SQL coordination.

pub mod provisioning;
mod runtime;
mod startup;

pub use runtime::run;
pub use startup::open_catalog;

/// Completes fresh deployment initialization before provisioning its children.
///
/// The launcher supplies the writable initializer admission. Existing environments
/// are rejected here rather than replaying positive snapshots on restart. This
/// path starts neither a SQL coordinator nor shared maintenance.
pub async fn initialize_fresh(
    config: mz_catalog::config::Config<'_>,
    oracle: &(dyn mz_timestamp_oracle::TimestampOracle<mz_repr::Timestamp> + Send + Sync),
) -> Result<mz_catalog::catalog::Catalog, mz_catalog::catalog::CatalogError> {
    if !config.storage.initialized_catalog() {
        return Err(
            anyhow::anyhow!("fresh controller initialization requires a new catalog").into(),
        );
    }
    let persist = config.state.persist_client.clone();
    let metrics = config.metrics_registry.clone();
    mz_catalog::catalog::Catalog::open(config)
        .await?
        .complete_fresh_deployment_initialization(persist, &metrics, oracle)
        .await
}

/// Declared adapter intent and assigned addresses, not a readiness assertion.
/// This representation deliberately excludes credential-bearing launch arguments.
#[derive(Debug, serde::Serialize)]
pub struct AdapterInventoryEntry {
    pub id: mz_repr::AdapterId,
    pub name: String,
    pub size: String,
    pub generation: u64,
    pub service: String,
    pub endpoints: std::collections::BTreeMap<String, Vec<String>>,
    pub provisioning_error: Option<String>,
}

/// Reconciles the deployment's adapter namespace against committed declarations.
///
/// The caller has completed deployment initialization and owns this deployment's
/// process lifecycle. Pass a namespace used only for adapter services. An empty
/// declaration set is valid and never causes a default declaration to be created.
/// Addresses report assignment only. Process and SQL readiness are observed
/// independently. Errors for one desired service do not hide the remaining intent.
pub async fn reconcile_adapters(
    catalog: &mut mz_catalog::catalog::Catalog,
    orchestrator: &dyn mz_orchestrator::NamespacedOrchestrator,
    launch: &provisioning::AdapterProvisioningConfig,
    sizes: &std::collections::BTreeMap<String, provisioning::AdapterAllocation>,
) -> anyhow::Result<Vec<AdapterInventoryEntry>> {
    // List before refreshing committed membership, so a concurrent CREATE that
    // was not visible to this list cannot be mistaken for an orphan.
    let observed = orchestrator.list_services().await?;
    catalog.sync_to_current_updates().await?;
    anyhow::ensure!(
        catalog.storage().await.get_deployment_generation().await? == launch.generation,
        "adapter launch configuration belongs to a different deployment"
    );
    let declarations = catalog.state().adapter_declarations();
    let desired: std::collections::BTreeSet<_> = declarations
        .keys()
        .map(|id| provisioning::service_name(*id, launch.generation))
        .collect();
    for service in observed {
        if service_in_generation(&service, launch.generation) && !desired.contains(&service) {
            orchestrator.drop_service(&service)?;
        }
    }
    let mut inventory = Vec::new();
    for declaration in declarations.values() {
        let service = provisioning::service_name(declaration.id, launch.generation);
        let mut entry = AdapterInventoryEntry {
            id: declaration.id,
            name: declaration.name.clone(),
            size: declaration.size.clone(),
            generation: launch.generation,
            service,
            endpoints: Default::default(),
            provisioning_error: None,
        };
        let Some(allocation) = sizes.get(&declaration.size) else {
            entry.provisioning_error = Some("unknown adapter size".into());
            inventory.push(entry);
            continue;
        };
        match launch.service_config(
            declaration.id,
            &declaration.name,
            &declaration.size,
            allocation,
        ) {
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
                    // Backend errors may include credential-bearing launch inputs.
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
        inventory.push(entry);
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

/// Enacts committed replica realizations without constructing compute or storage
/// controllers. Replica-set policy must commit its decisions before calling this.
/// The orchestrator must be restricted to the cluster namespace.
pub async fn reconcile_replicas(
    catalog: &mut mz_catalog::catalog::Catalog,
    orchestrator: &dyn mz_orchestrator::NamespacedOrchestrator,
    launch: &mz_controller::clusters::provisioning::ReplicaProvisioningConfig<'_>,
) -> anyhow::Result<()> {
    use mz_controller::clusters::{ReplicaLocation, ReplicaServiceName};

    let observed = orchestrator.list_services().await?;
    catalog.sync_to_current_updates().await?;
    anyhow::ensure!(
        catalog.storage().await.get_deployment_generation().await? == launch.deploy_generation,
        "replica launch configuration belongs to a different deployment"
    );
    let mut desired = Vec::new();
    for cluster in catalog.clusters() {
        for replica in cluster.replicas() {
            if let ReplicaLocation::Managed(location) = &replica.config.location {
                desired.push((
                    ReplicaServiceName {
                        cluster_id: cluster.id,
                        replica_id: replica.replica_id,
                        generation: launch.deploy_generation,
                    },
                    cluster.name.clone(),
                    replica.name.clone(),
                    cluster.role(),
                    location.clone(),
                ));
            }
        }
    }
    let live: std::collections::BTreeSet<_> =
        desired.iter().map(|(name, ..)| name.to_string()).collect();
    for service in observed {
        let name: ReplicaServiceName = service.parse()?;
        if name.generation == launch.deploy_generation && !live.contains(&service) {
            orchestrator.drop_service(&service)?;
        }
    }
    let overrides = mz_catalog::compute_config::replica_dyncfg_overrides(catalog);
    let affinity = catalog.system_config().enable_worker_core_affinity();
    for (name, cluster_name, replica_name, role, location) in desired {
        let launch = mz_controller::clusters::provisioning::ReplicaProvisioningConfig {
            replica_dyncfg_overrides: overrides
                .get(&name.cluster_id)
                .and_then(|replicas| replicas.get(&name.replica_id)),
            ..*launch
        };
        let spec = launch.service_config(
            name.cluster_id,
            name.replica_id,
            cluster_name,
            replica_name,
            role,
            location,
            affinity,
        )?;
        orchestrator.ensure_service(&name.to_string(), spec)?;
    }
    orchestrator.flush().await?;
    Ok(())
}
