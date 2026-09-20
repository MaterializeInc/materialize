// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replica-local following through the shared committed catalog implementation.
//!
//! The controller is the sole installer until replica execution protection and
//! worker sequencing are established. Observing a selection is not installation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use mz_catalog::catalog::Catalog;
use mz_catalog::config::ReplicaCatalogConfig;
use mz_catalog::durable::{Metrics, persist_backed_catalog_state};
use mz_catalog::expr_cache::{ExpressionCacheHandle, GlobalExpressions, expression_build_version};
use mz_catalog::memory::implications::{CatalogImplications, ParsedStateUpdate};
use mz_catalog::memory::objects::CatalogItem;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::{PersistLocation, cache::PersistClientCache};
use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
use mz_sql::catalog::EnvironmentId;
use mz_storage_types::connections::ConnectionContext;
use uuid::Uuid;

mod storage_metadata;

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
}

impl ReplicaEffects {
    fn absorb(&mut self, catalog: &Catalog, cluster: ClusterId, effects: CatalogImplications) {
        self.pending.extend(effects.items.into_keys());
        self.pending.extend(
            effects
                .written_plans
                .into_iter()
                .filter_map(|id| catalog.try_resolve_item_id(&id)),
        );
        if effects.clusters.contains_key(&cluster) {
            if let Some(cluster) = catalog.try_get_cluster(cluster) {
                self.pending.extend(cluster.bound_objects.iter().copied());
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
        self.pending.retain(|item_id| {
            // This cache describes the current selection, not installed work.
            // A replacement that is not available must not expose stale bytes.
            self.selected.remove(item_id);
            let candidate = catalog.try_get_entry(item_id).and_then(|entry| {
                let item = entry.item();
                if item.is_compute_object_on_cluster() != Some(cluster) {
                    return None;
                }
                match item {
                    CatalogItem::Index(index) => Some((index.global_id(), RelationVersion::root())),
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
) -> anyhow::Result<()> {
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
    let mut last_report = tokio::time::Instant::now();
    let mut last_error = None;
    let mut delay = Duration::from_secs(1);
    let mut pending_metadata = true;
    loop {
        // Native application owns parsing, ordering and in-memory catalog state.
        // It halts on unapplicable committed changes and returns fencing errors.
        let (_, updates) = catalog.sync_to_current_updates().await?;
        let changed = !updates.is_empty();
        absorb_updates(&mut effects, &catalog, config.cluster_id, &build, updates);
        if !changed && effects.pending.is_empty() && !pending_metadata {
            tokio::time::sleep(delay).await;
            continue;
        }
        let result: anyhow::Result<_> = async {
            effects
                .observe_plans(
                    &catalog,
                    config.cluster_id,
                    config.replica_id,
                    &store,
                    &build,
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
            storage_metadata::resolve(
                &catalog,
                &wanted,
                &store,
                &build,
                &persist,
                &config.persist_location,
                txns_shard,
            )
            .await
        }
        .await;
        match result {
            Ok(metadata) => {
                pending_metadata = !metadata.pending.is_empty();
                let pending = !effects.pending.is_empty() || pending_metadata;
                if changed
                    || last_error.is_some()
                    || (pending && last_report.elapsed() >= Duration::from_secs(60))
                {
                    tracing::info!(cluster = %config.cluster_id, replica = %config.replica_id,
                        plans = effects.selected.len(), pending_plans = ?effects.pending,
                        pending_metadata = ?metadata.pending, storage_inputs = metadata.metadata.len(),
                        observed_uppers = metadata.uppers.len(), "catalog follower effects observed (not enacted)");
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
                pending_metadata = true;
                let error = format!("{error:#}");
                if last_error.as_ref() != Some(&error)
                    || last_report.elapsed() >= Duration::from_secs(60)
                {
                    tracing::warn!(cluster = %config.cluster_id, replica = %config.replica_id,
                        %error, "catalog follower stalled (not enacted)");
                    last_report = tokio::time::Instant::now();
                }
                last_error = Some(error);
                delay = (delay * 2).min(Duration::from_secs(10));
            }
        }
        tokio::time::sleep(delay).await;
    }
}
