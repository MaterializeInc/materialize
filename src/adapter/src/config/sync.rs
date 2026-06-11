// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_controller::clusters::ReplicaLocation;
use mz_dyncfg::ParameterScope;
use tokio::time;

use crate::Client;
use crate::catalog::Catalog;
use crate::config::{
    ClusterEvalContext, ClusterScopeContext, ReplicaEvalContext, ReplicaScopeContext,
    ScopedParameters, SynchronizedParameters, SystemParameterBackend, SystemParameterFrontend,
    SystemParameterSyncConfig,
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

        // Reconcile the scoped (per-cluster and per-replica) parameters. We do
        // this every tick (independent of whether the environment-wide values
        // changed) so the overrides track the current set of live objects.
        sync_scoped_params(&scoped_client, frontend, &params).await;
    }
}

/// Evaluate the scoped parameters (cluster-coherent and replica-local) for the
/// currently live clusters and replicas and push the resulting overrides to the
/// coordinator's working copy.
async fn sync_scoped_params(
    client: &Client,
    frontend: &SystemParameterFrontend,
    params: &SynchronizedParameters,
) {
    let catalog = client.catalog_snapshot().await;
    let system_config = catalog.system_config();

    // The synced parameters, partitioned by scope class. The scope declaration
    // bounds per-tick evaluation to exactly the flags in use: an environment
    // with no scoped flags evaluates neither pass.
    let replica_param_names: Vec<&'static str> = system_config
        .iter_synced()
        .filter(|var| var.scope() == ParameterScope::Replica)
        .map(|var| var.name())
        .collect();
    let cluster_param_names: Vec<&'static str> = system_config
        .iter_synced()
        .filter(|var| var.scope() == ParameterScope::Cluster)
        .map(|var| var.name())
        .collect();

    if replica_param_names.is_empty() && cluster_param_names.is_empty() {
        return;
    }

    let replica = if replica_param_names.is_empty() {
        Default::default()
    } else {
        let replicas = build_replica_eval_contexts(&catalog);
        frontend.pull_replica_overrides(params, &replica_param_names, &replicas)
    };
    let cluster = if cluster_param_names.is_empty() {
        Default::default()
    } else {
        let clusters = build_cluster_eval_contexts(&catalog);
        frontend.pull_cluster_overrides(params, &cluster_param_names, &clusters)
    };

    // Push the complete desired state to the coordinator, which holds the
    // working copy and resolves each layer at its boundary: the controller's
    // per-replica dyncfg push for `replica`, plan-time
    // `OptimizerFeatureOverrides` for `cluster`.
    let scoped = ScopedParameters { cluster, replica };
    client.update_scoped_system_parameters(scoped).await;
}

/// Build a [`ClusterEvalContext`] for every live cluster in the catalog.
fn build_cluster_eval_contexts(catalog: &Catalog) -> Vec<ClusterEvalContext> {
    catalog
        .clusters()
        .map(|cluster| ClusterEvalContext {
            cluster_id: cluster.id,
            cluster: ClusterScopeContext {
                id: cluster.id.to_string(),
                name: cluster.name.clone(),
                is_builtin: cluster.id.is_system(),
            },
        })
        .collect()
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
