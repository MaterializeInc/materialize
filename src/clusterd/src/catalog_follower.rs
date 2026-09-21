// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replica-local following and enactment through the shared committed catalog.
//!
//! Runtime ownership is supplied at startup. Without a native endpoint this
//! follower only observes selections and cannot install maintained work.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use mz_catalog::catalog::{Catalog, Op};
use mz_catalog::config::ReplicaCatalogConfig;
use mz_catalog::durable::{Metrics, persist_backed_catalog_state};
use mz_catalog::expr_cache::{ExpressionCacheHandle, GlobalExpressions, expression_build_version};
use mz_catalog::memory::implications::{CatalogImplications, ParsedStateUpdate};
use mz_catalog::memory::objects::CatalogItem;
use mz_compute::server::ReplicaCompute;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::{PersistLocation, cache::PersistClientCache};
use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
use mz_sql::catalog::EnvironmentId;
use mz_storage_types::connections::ConnectionContext;
use uuid::Uuid;

mod compaction;
mod compute;
mod storage_metadata;
mod time_dependence;

#[cfg(test)]
mod tests;

pub(crate) struct Config {
    pub environment_id: EnvironmentId,
    pub reconstruction: ReplicaCatalogConfig,
    pub connection_context: ConnectionContext,
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub deploy_generation: u64,
    pub persist_location: PersistLocation,
    pub build_info: &'static mz_build_info::BuildInfo,
}

/// The replica's pending effects, not a second projection of catalog membership.
#[derive(Default)]
struct ReplicaEffects {
    pending: BTreeSet<CatalogItemId>,
    selected: BTreeMap<CatalogItemId, (GlobalId, Uuid, GlobalExpressions)>,
    configuration_changed: bool,
}

impl ReplicaEffects {
    fn absorb(&mut self, catalog: &Catalog, cluster: ClusterId, effects: CatalogImplications) {
        self.configuration_changed |= effects.system_config_changed
            || effects.replica_scoped_config_changed
            || effects.clusters.contains_key(&cluster);
        self.pending.extend(effects.items.into_keys());
        self.pending.extend(
            effects
                .written_plans
                .into_iter()
                .filter_map(|id| catalog.try_resolve_item_id(&id)),
        );
        if effects.clusters.contains_key(&cluster) {
            if catalog.try_get_cluster(cluster).is_some() {
                // Cluster bound_objects contains user objects only. Bootstrap
                // also needs the build-defined objects in the native catalog.
                self.pending.extend(
                    catalog
                        .entries()
                        .filter(|entry| entry.item().cluster_id() == Some(cluster))
                        .map(|entry| entry.id()),
                );
            } else {
                self.selected.clear();
            }
        }
        // A changed storage lifetime or permission may unblock pending effects.
        // Reading it is not permission to compact without local dependency accounting.
    }

    async fn observe_plans(
        &mut self,
        catalog: &Catalog,
        cluster: ClusterId,
        replica: ReplicaId,
        store: &ExpressionCacheHandle,
        build: &str,
    ) -> anyhow::Result<()> {
        let mut revisions = Vec::new();
        let mut candidates = BTreeMap::new();
        // These arrangements are created by instance initialization, not written
        // dataflows. Other indexes, including user indexes on logs, need selections.
        let log_indexes: BTreeSet<_> = catalog
            .try_get_cluster(cluster)
            .into_iter()
            .flat_map(|cluster| cluster.log_indexes.values().copied())
            .collect();
        self.pending.retain(|item_id| {
            // This cache describes the current selection, not installed work.
            // A replacement that is not available must not expose stale bytes.
            self.selected.remove(item_id);
            let candidate = catalog.try_get_entry(item_id).and_then(|entry| {
                let item = entry.item();
                if item.cluster_id() != Some(cluster) {
                    return None;
                }
                match item {
                    CatalogItem::Index(index) if !log_indexes.contains(&index.global_id()) => {
                        Some((index.global_id(), RelationVersion::root()))
                    }
                    CatalogItem::MaterializedView(mv)
                        if mv.target_replica.is_none_or(|id| id == replica) =>
                    {
                        Some((
                            mv.global_id_writes(),
                            *mv.collections.last_key_value().expect("MV has a version").0,
                        ))
                    }
                    CatalogItem::MetricSink(sink) => {
                        Some((sink.global_id, RelationVersion::root()))
                    }
                    _ => None,
                }
            });
            let Some((id, version)) = candidate else {
                return false;
            };
            if let Some(revision) = catalog.state().written_plan(id, build) {
                revisions.push((id, revision));
                candidates.insert(*item_id, (id, revision, version));
            }
            true
        });
        let mut plans = store.read_plans(revisions).await?;
        for (item, (id, revision, version)) in candidates {
            if let Some(plan) = plans.remove(&(id, revision))
                && plan.item_version == version
                && plan.physical_plan.export_ids().any(|export| export == id)
            {
                self.selected.insert(item, (id, revision, plan));
                self.pending.remove(&item);
            }
        }
        Ok(())
    }
}

fn absorb_updates(
    effects: &mut ReplicaEffects,
    catalog: &Catalog,
    cluster: ClusterId,
    build: &str,
    updates: Vec<ParsedStateUpdate>,
) {
    // Absorption's contract is one consolidated timestamp at a time. One sync
    // can contain multiple committed transactions for the same object.
    let mut timestamps: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for update in updates {
        timestamps.entry(update.ts).or_default().push(update);
    }
    for updates in timestamps.into_values() {
        effects.absorb(
            catalog,
            cluster,
            CatalogImplications::from_updates(updates, build),
        );
    }
}

pub(crate) async fn run(
    config: Config,
    persist_clients: Arc<PersistClientCache>,
    registry: MetricsRegistry,
    endpoint: Option<ReplicaCompute>,
) -> anyhow::Result<()> {
    let failure_counts: mz_ore::metrics::IntCounterVec = registry.register(mz_ore::metric! {
        name: "mz_catalog_follower_failures_total",
        help: "Catalog follower attempts that require retry, by phase.",
        var_labels: ["phase"],
    });
    let failures: BTreeMap<_, _> = ["observation", "installation", "publication", "compaction"]
        .into_iter()
        .map(|phase| {
            (
                phase,
                failure_counts.get_delete_on_drop_metric(vec![phase.to_string()]),
            )
        })
        .collect();
    let pending_installs: mz_ore::metrics::UIntGauge = registry.register(mz_ore::metric! {
        name: "mz_catalog_follower_pending_compute_installations",
        help: "Compute selections awaiting replica installation.",
    });
    let persist = persist_clients
        .open(config.persist_location.clone())
        .await?;
    let storage = persist_backed_catalog_state(
        persist.clone(),
        config.environment_id.organization_id(),
        config.build_info.semver_version(),
        Some(config.deploy_generation),
        Arc::new(Metrics::new(&registry)),
    )
    .await?
    .join()
    .await?;
    let state_config = config.reconstruction.into_state(
        config.build_info,
        config.environment_id,
        config.connection_context,
        persist.clone(),
    );
    let opened = Catalog::open_committed(state_config, storage).await?;
    let mut catalog = opened.catalog;
    let initial = opened.initial_updates;
    let txns_shard = opened.txn_wal_shard;
    let build = expression_build_version(config.build_info);
    let shard = opened
        .expression_cache_shard
        .context("catalog has no expression shard")?;
    let store = ExpressionCacheHandle::open_plan_store(build.clone(), &persist, shard).await;
    let build = build.to_string();
    let mut effects = ReplicaEffects::default();
    absorb_updates(&mut effects, &catalog, config.cluster_id, &build, initial);
    let mut compute = if let Some(endpoint) = endpoint {
        let (incarnation, publication_started) = loop {
            let (_, updates) = catalog.sync_to_current_updates().await?;
            absorb_updates(&mut effects, &catalog, config.cluster_id, &build, updates);
            let started = std::time::Instant::now();
            let ts = catalog.current_upper().await;
            match catalog
                .transact(None, ts, None, vec![Op::CreateClientIncarnation])
                .await
            {
                Ok(result) => {
                    absorb_updates(
                        &mut effects,
                        &catalog,
                        config.cluster_id,
                        &build,
                        result.catalog_updates,
                    );
                    break (
                        *result
                            .created_client_incarnations
                            .first()
                            .context("created replica incarnation")?,
                        started,
                    );
                }
                Err(mz_catalog::catalog::CatalogError::Catalog(error))
                    if matches!(
                        error.kind,
                        mz_catalog::memory::error::ErrorKind::Durable(
                            mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
                        )
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        };
        anyhow::ensure!(
            catalog
                .try_get_cluster_replica(config.cluster_id, config.replica_id)
                .is_some(),
            "replica was removed before initialization"
        );
        let instance = mz_catalog::compute_config::replica_instance_config(
            &catalog,
            config.cluster_id,
            config.replica_id,
            config.persist_location.clone(),
        );
        Some(compute::ComputeEnactment::new(
            endpoint,
            instance,
            incarnation,
            publication_started,
            config.cluster_id,
            config.replica_id,
            &registry,
        ))
    } else {
        None
    };
    let mut last_report = tokio::time::Instant::now();
    let mut last_error = None;
    let mut delay = Duration::from_secs(1);
    let mut pending_metadata = true;
    let mut compaction = compaction::Compaction::default();
    loop {
        // Native application owns parsing, ordering and in-memory catalog state.
        // It halts on unapplicable committed changes and returns fencing errors.
        let (_, updates) = wait(&mut compute, catalog.sync_to_current_updates()).await?;
        let changed = !updates.is_empty();
        absorb_updates(&mut effects, &catalog, config.cluster_id, &build, updates);
        if let Some(compute) = &mut compute {
            compute.ensure_live(&catalog)?;
            if compute.renewal_due() {
                if let Err(error) = compute
                    .publish(
                        &mut catalog,
                        &mut effects,
                        config.cluster_id,
                        &build,
                        true,
                        None,
                    )
                    .await
                {
                    failures["publication"].inc();
                    tracing::warn!(%error, "replica heartbeat pending");
                }
            }
            compute.apply_progress(&catalog);
            // Advancing permission waits for current selections and import holds.
            // Retired definitions cannot be imported by current own-build selections:
            // their removing transaction also repairs those written plans.
            compute.apply_catalog(&catalog, &storage_metadata::Resolution::default(), false);
            if effects.configuration_changed {
                anyhow::ensure!(
                    catalog
                        .try_get_cluster_replica(config.cluster_id, config.replica_id)
                        .is_some(),
                    "replica was removed"
                );
                compute.configure(mz_catalog::compute_config::replica_compute_config(
                    &catalog,
                    config.cluster_id,
                    config.replica_id,
                ));
                effects.configuration_changed = false;
            }
        }
        if compute.is_none() && !changed && effects.pending.is_empty() && !pending_metadata {
            tokio::time::sleep(delay).await;
            continue;
        }
        let result: anyhow::Result<_> = async {
            wait(
                &mut compute,
                effects.observe_plans(
                    &catalog,
                    config.cluster_id,
                    config.replica_id,
                    &store,
                    &build,
                ),
            )
            .await?;
            let wanted = effects
                .selected
                .values()
                .flat_map(|(_, _, plan)| {
                    plan.physical_plan
                        .imported_source_ids()
                        .chain(plan.physical_plan.persist_sink_ids())
                })
                .collect();
            wait(
                &mut compute,
                storage_metadata::resolve(
                    &catalog,
                    &wanted,
                    &store,
                    &build,
                    &persist,
                    &config.persist_location,
                    txns_shard,
                ),
            )
            .await
        }
        .await;
        match result {
            Ok(metadata) => {
                pending_metadata = !metadata.pending.is_empty();
                let mut pending = !effects.pending.is_empty() || pending_metadata;
                if let Some(compute) = &mut compute {
                    if compute.renewal_due() {
                        if let Err(error) = compute
                            .publish(
                                &mut catalog,
                                &mut effects,
                                config.cluster_id,
                                &build,
                                true,
                                None,
                            )
                            .await
                        {
                            failures["publication"].inc();
                            tracing::warn!(%error, "replica heartbeat pending");
                        }
                    }
                    let result = compute
                        .install(
                            &mut catalog,
                            &mut effects,
                            config.cluster_id,
                            &build,
                            &store,
                            &persist,
                            &metadata,
                        )
                        .await;
                    if let Err(error) = result {
                        failures["installation"].inc();
                        pending = true;
                        tracing::warn!(cluster = %config.cluster_id, replica = %config.replica_id,
                            %error, "compute installation pending");
                    }
                    compute.apply_progress(&catalog);
                    compute.apply_catalog(&catalog, &metadata, effects.pending.is_empty());
                    if let Err(error) = compute
                        .publish(
                            &mut catalog,
                            &mut effects,
                            config.cluster_id,
                            &build,
                            pending,
                            Some(&metadata),
                        )
                        .await
                    {
                        failures["publication"].inc();
                        tracing::warn!(%error, "replica protection publication pending");
                    }
                    let wanted = metadata.metadata.keys().copied().collect();
                    if let Err(error) = compute
                        .io
                        .wait(compaction.reconcile(
                            &persist,
                            catalog.state().storage_metadata(),
                            &wanted,
                        ))
                        .await
                    {
                        failures["compaction"].inc();
                        tracing::warn!(%error, "committed storage compaction pending");
                    }
                    pending_installs.set(
                        compute
                            .pending_installations(&effects)
                            .try_into()
                            .expect("fits u64"),
                    );
                }
                if changed
                    || last_error.is_some()
                    || (pending && last_report.elapsed() >= Duration::from_secs(60))
                {
                    tracing::info!(
                        cluster = %config.cluster_id, replica = %config.replica_id,
                        plans = effects.selected.len(), pending_plans = ?effects.pending,
                        pending_metadata = ?metadata.pending,
                        storage_inputs = metadata.metadata.len(),
                        observed_uppers = metadata.uppers.len(), replica_owned = compute.is_some(),
                        "catalog follower effects processed"
                    );
                    last_report = tokio::time::Instant::now();
                }
                last_error = None;
                delay = if !pending || changed {
                    Duration::from_secs(1)
                } else {
                    (delay * 2).min(Duration::from_secs(10))
                };
            }
            Err(error) => {
                failures["observation"].inc();
                pending_metadata = true;
                if let Some(compute) = &mut compute {
                    // Missing metadata cannot prevent renewal of existing protection.
                    if let Err(error) = compute
                        .publish(
                            &mut catalog,
                            &mut effects,
                            config.cluster_id,
                            &build,
                            true,
                            None,
                        )
                        .await
                    {
                        failures["publication"].inc();
                        tracing::warn!(%error, "replica heartbeat pending");
                    }
                }
                let error = format!("{error:#}");
                if last_error.as_ref() != Some(&error)
                    || last_report.elapsed() >= Duration::from_secs(60)
                {
                    tracing::warn!(cluster = %config.cluster_id, replica = %config.replica_id,
                        %error, replica_owned = compute.is_some(), "catalog follower stalled");
                    last_report = tokio::time::Instant::now();
                }
                last_error = Some(error);
                delay = (delay * 2).min(Duration::from_secs(10));
            }
        }
        wait(&mut compute, tokio::time::sleep(delay)).await;
    }
}

async fn wait<F: std::future::Future>(
    compute: &mut Option<compute::ComputeEnactment>,
    future: F,
) -> F::Output {
    match compute {
        Some(compute) => compute.io.wait(future).await,
        None => future.await,
    }
}
