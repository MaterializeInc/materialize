// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_dyncfg::ParameterScope;
use tokio::time;

use crate::Client;
use crate::catalog::Catalog;
use crate::config::{
    ClusterEvalContext, ClusterScopeContext, ReplicaEvalContext, ReplicaScopeContext,
    ScopedParameters, ScopedParametersScope, SynchronizedParameters, SystemParameterBackend,
    SystemParameterFrontend, SystemParameterSyncConfig,
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

    // Ensure the frontend client is initialized. Wrapped in `Arc` so a clone can
    // be shared with the coordinator for synchronous create-time scoped
    // resolution.
    let mut frontend = Option::<Arc<SystemParameterFrontend>>::None; // lazy initialize the frontend below
    let mut backend = SystemParameterBackend::new(adapter_client).await?;

    // Tick every `tick_duration` ms, skipping missed ticks.
    let mut interval = time::interval(tick_interval);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    // Run the synchronization loop.
    tracing::info!(
        "synchronizing system parameter values every {} seconds",
        tick_interval.as_secs()
    );

    // Whether the scoped overrides may carry state that needs clearing once the
    // feature is disabled. True at startup, since a prior run (this process or
    // an earlier one) may have left overrides behind. While the feature stays
    // disabled this lets the scoped reconcile do no per-tick work after the
    // single clearing push.
    let mut scoped_overrides_maybe_dirty = true;

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
            let new_frontend = Arc::new(SystemParameterFrontend::from(&sync_config).await?);
            // Share the frontend with the coordinator so the create-cluster /
            // create-replica paths can resolve a new object's scoped overrides
            // synchronously, instead of waiting for the next tick.
            scoped_client.install_scoped_system_parameter_frontend(Arc::clone(&new_frontend));
            frontend = Some(new_frontend);
        }

        // Pull latest state from frontend and push changes to backend.
        let frontend = frontend.as_ref().expect("frontend exists");
        if frontend.pull(&mut params) {
            backend.push(&mut params).await;
        }

        // Reconcile the scoped (per-cluster and per-replica) parameters. We do
        // this every tick (independent of whether the environment-wide values
        // changed) so the overrides track the current set of live objects.
        sync_scoped_params(
            &scoped_client,
            frontend,
            &params,
            &mut scoped_overrides_maybe_dirty,
        )
        .await;
    }
}

/// Evaluate the scoped parameters (cluster-coherent and replica-local) for the
/// currently live clusters and replicas and push the resulting overrides to the
/// coordinator's working copy.
async fn sync_scoped_params(
    client: &Client,
    frontend: &SystemParameterFrontend,
    params: &SynchronizedParameters,
    maybe_dirty: &mut bool,
) {
    // Read the feature gate from the working copy rather than a catalog
    // snapshot. The gate is an environment-wide synced parameter, so `params`
    // already carries its current value. This keeps a disabled environment, the
    // default, from paying a coordinator round-trip every tick.
    //
    // Scoped (per-cluster and per-replica) overrides are off by default,
    // leaving the environment-wide behavior unchanged. While disabled we
    // evaluate no scoped contexts. If a prior run left overrides behind we clear
    // them once so resolution falls back to the environment-wide value
    // everywhere, then do no per-tick work until the feature is enabled again.
    if !params.enable_scoped_system_parameters() {
        if *maybe_dirty {
            // Full replace with an empty desired state clears every override.
            // No create-time write races here: it is gated off too.
            client
                .update_scoped_system_parameters(ScopedParameters::default(), None)
                .await;
            *maybe_dirty = false;
        }
        return;
    }
    *maybe_dirty = true;

    let catalog = client.catalog_snapshot().await;

    // Push the desired state to the coordinator, which holds the working copy
    // and resolves each layer at its boundary: the controller's per-replica
    // dyncfg push for `replica`, plan-time `OptimizerFeatureOverrides` for
    // `cluster`. Scope removals to the live objects in this snapshot, so an
    // object created between this snapshot and the apply (folding its override
    // into its own create transaction) is not wiped by this reconcile.
    let prune_scope = ScopedParametersScope {
        clusters: catalog.clusters().map(|cluster| cluster.id).collect(),
        replicas: catalog
            .clusters()
            .flat_map(|cluster| cluster.replicas().map(|replica| replica.replica_id))
            .collect(),
    };
    let scoped = evaluate_scoped_parameters(frontend, params, &catalog, None, None);
    client
        .update_scoped_system_parameters(scoped, Some(prune_scope))
        .await;
}

/// Evaluate the scoped parameters (cluster-coherent and replica-local) for the
/// live objects, optionally restricted to a subset of cluster or replica ids.
///
/// The full pass (both filters `None`) is the sync loop's per-tick reconcile.
/// The create path passes `Some(..)` to resolve just the newly-created objects
/// synchronously, so they observe their overrides in their first controller
/// configuration or first plan rather than after the next tick. Returns the
/// sparse desired overrides for the evaluated objects.
pub(crate) fn evaluate_scoped_parameters(
    frontend: &SystemParameterFrontend,
    params: &SynchronizedParameters,
    catalog: &Catalog,
    cluster_filter: Option<&BTreeSet<ClusterId>>,
    replica_filter: Option<&BTreeSet<ReplicaId>>,
) -> ScopedParameters {
    let system_config = catalog.system_config();

    // The synced parameters, partitioned by scope class. The scope declaration
    // bounds evaluation to exactly the flags in use: an environment with no
    // scoped flags evaluates neither pass.
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

    let replica = if replica_param_names.is_empty() {
        Default::default()
    } else {
        let replicas = build_replica_eval_contexts(catalog, replica_filter);
        frontend.pull_replica_overrides(params, &replica_param_names, &replicas)
    };
    let cluster = if cluster_param_names.is_empty() {
        Default::default()
    } else {
        let clusters = build_cluster_eval_contexts(catalog, cluster_filter);
        frontend.pull_cluster_overrides(params, &cluster_param_names, &clusters)
    };

    ScopedParameters { cluster, replica }
}

/// Build a [`ClusterEvalContext`] for each live cluster in the catalog, skipping
/// clusters absent from `filter` when one is given.
fn build_cluster_eval_contexts(
    catalog: &Catalog,
    filter: Option<&BTreeSet<ClusterId>>,
) -> Vec<ClusterEvalContext> {
    catalog
        .clusters()
        .filter(|cluster| filter.is_none_or(|f| f.contains(&cluster.id)))
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

/// Build a [`ReplicaEvalContext`] for each live managed replica in the catalog,
/// skipping replicas absent from `filter` when one is given.
fn build_replica_eval_contexts(
    catalog: &Catalog,
    filter: Option<&BTreeSet<ReplicaId>>,
) -> Vec<ReplicaEvalContext> {
    // An empty filter cannot match any replica, so skip the per-cluster scan.
    // The create-cluster path passes an empty set to resolve only the cluster
    // scope.
    if filter.is_some_and(|f| f.is_empty()) {
        return Vec::new();
    }

    let mut contexts = Vec::new();
    for cluster in catalog.clusters() {
        let is_builtin = cluster.id.is_system();
        let cluster_ctx = ClusterScopeContext {
            id: cluster.id.to_string(),
            name: cluster.name.clone(),
            is_builtin,
        };
        for replica in cluster.replicas() {
            if filter.is_some_and(|f| !f.contains(&replica.replica_id)) {
                continue;
            }
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
