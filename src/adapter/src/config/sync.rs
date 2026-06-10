// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use mz_cluster_client::ReplicaId;
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::ClusterId;
use mz_dyncfg::{ConfigUpdates, ParameterScope};
use tokio::time;

use crate::Client;
use crate::catalog::Catalog;
use crate::config::{
    ClusterScopeContext, ReplicaEvalContext, ReplicaScopeContext, SynchronizedParameters,
    SystemParameterBackend, SystemParameterFrontend, SystemParameterSyncConfig,
};

/// Run a loop that periodically pulls system parameters defined in the
/// LaunchDarkly-backed [SystemParameterFrontend] and pushes modified values to the
/// `ALTER SYSTEM`-backed [SystemParameterBackend].
pub async fn system_parameter_sync(
    sync_config: SystemParameterSyncConfig,
    adapter_client: Client,
    tick_interval: Option<Duration>,
) -> Result<(), anyhow::Error> {
    let Some(tick_interval) = tick_interval else {
        tracing::info!("skipping system parameter sync as tick_interval = None");
        return Ok(());
    };

    // Keep a client handle for catalog snapshots and the per-replica scoped
    // config push, since the backend consumes its own clone.
    let scoped_client = adapter_client.clone();

    // Ensure the frontend client is initialized.
    let mut frontend = Option::<SystemParameterFrontend>::None; // lazy initialize the frontend below
    let mut backend = SystemParameterBackend::new(adapter_client).await?;

    // Tick every `tick_duration` ms, skipping missed ticks.
    let mut interval = time::interval(tick_interval);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    // Run the synchronization loop.
    tracing::info!(
        "synchronizing system parameter values every {} seconds",
        tick_interval.as_secs()
    );

    let mut params = SynchronizedParameters::default();
    loop {
        // Wait for the next sync period
        interval.tick().await;

        // Fetch current parameter values from the backend
        backend.pull(&mut params).await;

        if !params.enable_launchdarkly() && sync_config.backend_config.is_launch_darkly() {
            if frontend.is_some() {
                tracing::info!("stopping system parameter frontend");
                frontend = None;
            } else {
                tracing::info!("system parameter sync is disabled; not syncing")
            }

            // Don't do anything until the next loop.
            continue;
        }

        if frontend.is_none() {
            tracing::info!("initializing system parameter frontend");
            frontend = Some(SystemParameterFrontend::from(&sync_config).await?);
        }

        // Pull latest state from frontend and push changes to backend.
        let frontend = frontend.as_ref().expect("frontend exists");
        if frontend.pull(&mut params) {
            backend.push(&mut params).await;
        }

        // Reconcile the replica-local scoped parameters. We do this every tick
        // (independent of whether the environment-wide values changed) so the
        // per-replica overrides track the current set of live replicas.
        sync_replica_scoped_params(&scoped_client, frontend, &params).await;
    }
}

/// Evaluate the replica-local scoped parameters for the currently live replicas
/// and push the resulting overrides into the controller's per-replica dyncfg
/// layer.
async fn sync_replica_scoped_params(
    client: &Client,
    frontend: &SystemParameterFrontend,
    params: &SynchronizedParameters,
) {
    let catalog = client.catalog_snapshot().await;

    // The set of synced parameters declared as replica-local.
    let param_names: Vec<&'static str> = catalog
        .system_config()
        .iter_synced()
        .filter(|var| var.scope() == ParameterScope::Replica)
        .map(|var| var.name())
        .collect();

    // No replica-local flags means there is nothing to evaluate and we pay
    // nothing for replica evaluation.
    if param_names.is_empty() {
        return;
    }

    let replicas = build_replica_eval_contexts(&catalog);
    let overrides = frontend.pull_replica_overrides(params, &param_names, &replicas);
    let overrides = parse_replica_overrides(&catalog, overrides);
    client.update_replica_scoped_config(overrides).await;
}

/// Build a [`ReplicaEvalContext`] for every live managed replica in the catalog.
fn build_replica_eval_contexts(catalog: &Catalog) -> Vec<ReplicaEvalContext> {
    let mut contexts = Vec::new();
    for cluster in catalog.clusters() {
        let is_builtin = cluster.id.is_system();
        let cluster_ctx = ClusterScopeContext {
            id: cluster.id.to_string(),
            name: cluster.name.clone(),
            is_builtin,
        };
        for replica in cluster.replicas() {
            // Only managed replicas have a size (and therefore a size family).
            let ReplicaLocation::Managed(location) = &replica.config.location else {
                continue;
            };
            let replica_ctx = ReplicaScopeContext {
                id: replica.replica_id.to_string(),
                name: replica.name.clone(),
                is_builtin,
                size: location.size.clone(),
                size_family: location.allocation.family().to_string(),
                cluster_id: cluster.id.to_string(),
                cluster_name: cluster.name.clone(),
            };
            contexts.push(ReplicaEvalContext {
                cluster_id: cluster.id,
                replica_id: replica.replica_id,
                cluster: cluster_ctx.clone(),
                replica: replica_ctx,
            });
        }
    }
    contexts
}

/// Parse the per-replica string overrides into typed [`ConfigUpdates`], using
/// the catalog's dyncfg definitions to determine each parameter's type.
fn parse_replica_overrides(
    catalog: &Catalog,
    overrides: BTreeMap<ClusterId, BTreeMap<ReplicaId, BTreeMap<String, String>>>,
) -> BTreeMap<ClusterId, BTreeMap<ReplicaId, ConfigUpdates>> {
    let dyncfgs = catalog.system_config().dyncfgs();
    let mut out: BTreeMap<ClusterId, BTreeMap<ReplicaId, ConfigUpdates>> = BTreeMap::new();
    for (cluster_id, replicas) in overrides {
        let mut replica_updates = BTreeMap::new();
        for (replica_id, values) in replicas {
            let mut updates = ConfigUpdates::default();
            for (name, value) in values {
                let Some(entry) = dyncfgs.entry(&name) else {
                    // A replica-local scoped parameter that is not a dyncfg has
                    // no per-replica realization; skip it.
                    continue;
                };
                match entry.parse_val(&value) {
                    Ok(val) => updates.add_dynamic(&name, val),
                    Err(e) => {
                        tracing::warn!(%name, %value, "cannot parse scoped override: {e}");
                    }
                }
            }
            if !updates.updates.is_empty() {
                replica_updates.insert(replica_id, updates);
            }
        }
        if !replica_updates.is_empty() {
            out.insert(cluster_id, replica_updates);
        }
    }
    out
}
