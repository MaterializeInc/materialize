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
use mz_catalog::expr_cache::{ExpressionCacheHandle, GlobalExpressions};
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
mod execution;
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
            || effects.replicas.keys().any(|(id, _)| *id == cluster)
            || effects.clusters.contains_key(&cluster);
        self.pending.extend(effects.items.into_keys());
        self.pending
            .extend(effects.written_plans.into_iter().filter_map(|id| {
                catalog.try_resolve_item_id(&id).or(match id {
                    GlobalId::Transient(id) => Some(CatalogItemId::Transient(id)),
                    _ => None,
                })
            }));
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
            if let CatalogItemId::Transient(value) = item_id {
                let id = GlobalId::Transient(*value);
                if let Some(owner) = catalog.state().written_plan_replica_owner(id, build) {
                    if owner.replica_id != replica {
                        return false;
                    }
                    if let Some(revision) = catalog.state().written_plan(id, build) {
                        revisions.push((id, revision));
                        candidates.insert(*item_id, (id, revision, RelationVersion::root()));
                    }
                    return true;
                }
            }
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
                if let Some(owner) = catalog.state().written_plan_replica_owner(id, build) {
                    let matching_sink =
                        plan.physical_plan
                            .sink_exports
                            .get(&id)
                            .is_some_and(|sink| match &sink.connection {
                                mz_compute_types::sinks::ComputeSinkConnection::MetricSink(
                                    connection,
                                ) => connection.label == owner.name,
                                _ => false,
                            });
                    anyhow::ensure!(
                        plan.physical_plan.source_imports.is_empty()
                            && plan.physical_plan.index_exports.is_empty()
                            && plan.physical_plan.sink_exports.len() == 1
                            && matching_sink,
                        "invalid replica-owned metric plan {id}"
                    );
                }
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
    storage_endpoint: Option<mz_storage::server::ReplicaStorage>,
) -> anyhow::Result<()> {
    let build = config
        .reconstruction
        .plan_build_version(config.build_info)?;
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
    let shard = opened
        .expression_cache_shard
        .context("catalog has no expression shard")?;
    let store = ExpressionCacheHandle::open_plan_store(build.clone(), &persist, shard).await;
    let build = build.to_string();
    let mut effects = ReplicaEffects::default();
    absorb_updates(&mut effects, &catalog, config.cluster_id, &build, initial);
    let mut execution = if let Some(endpoint) = endpoint {
        let (incarnation, publication_started) = loop {
            let (_, updates) = catalog.sync_to_current_updates().await?;
            absorb_updates(&mut effects, &catalog, config.cluster_id, &build, updates);
            let started = std::time::Instant::now();
            let ts = catalog.current_upper().await;
            match catalog
                .transact(
                    None,
                    ts,
                    None,
                    vec![Op::CreateClientIncarnation {
                        replica_id: Some(config.replica_id),
                    }],
                )
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
        Some(execution::ReplicaEnactment::new(
            endpoint,
            instance,
            incarnation,
            publication_started,
            config.cluster_id,
            config.replica_id,
            &registry,
            storage_endpoint,
        ))
    } else {
        None
    };
    let mut last_report = tokio::time::Instant::now();
    let mut last_error = None;
    let mut delay = Duration::from_secs(1);
    let mut pending_metadata = true;
    let mut compaction = compaction::Compaction::default();
    let mut publication_interval = catalog
        .system_config()
        .catalog_read_protection_publish_interval();
    let mut publication_after = tokio::time::Instant::now() + publication_interval;
    loop {
        // Native application owns parsing, ordering and in-memory catalog state.
        // It halts on unapplicable committed changes and returns fencing errors.
        let (_, updates) = wait(&mut execution, catalog.sync_to_current_updates()).await?;
        let changed = !updates.is_empty();
        absorb_updates(&mut effects, &catalog, config.cluster_id, &build, updates);
        if let Some(execution) = &mut execution {
            execution.ensure_live(&catalog)?;
            if execution.renewal_due() {
                if let Err(error) = execution
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
            execution.apply_progress(&catalog);
            execution.apply_storage_progress();
            if let Err(error) = execution
                .admit_kafka_preopens(&mut catalog, &mut effects, config.cluster_id, &build)
                .await
            {
                failures["installation"].inc();
                tracing::warn!(%error, "Kafka pre-open admission denied");
            }
            execution.ensure_live(&catalog)?;
            // Advancing permission waits for current selections and import holds.
            // Retired definitions cannot be imported by current own-build selections:
            // their removing transaction also repairs those written plans.
            execution.apply_catalog(
                &catalog,
                &build,
                &storage_metadata::Resolution::default(),
                false,
            );
            if effects.configuration_changed {
                anyhow::ensure!(
                    catalog
                        .try_get_cluster_replica(config.cluster_id, config.replica_id)
                        .is_some(),
                    "replica was removed"
                );
                execution.configure_storage(&catalog, config.cluster_id, config.replica_id);
                execution.configure(mz_catalog::compute_config::replica_compute_config(
                    &catalog,
                    config.cluster_id,
                    config.replica_id,
                ));
                effects.configuration_changed = false;
            }
        }
        if execution.is_none() && !changed && effects.pending.is_empty() && !pending_metadata {
            tokio::time::sleep(delay).await;
            continue;
        }
        let result: anyhow::Result<_> = async {
            wait(
                &mut execution,
                effects.observe_plans(
                    &catalog,
                    config.cluster_id,
                    config.replica_id,
                    &store,
                    &build,
                ),
            )
            .await?;
            let mut wanted: BTreeSet<_> = effects
                .selected
                .values()
                .flat_map(|(_, _, plan)| {
                    plan.physical_plan
                        .imported_source_ids()
                        .chain(plan.physical_plan.persist_sink_ids())
                })
                .collect();
            if let Some(execution) = &execution {
                wanted.extend(execution.storage_wanted(
                    &catalog,
                    config.cluster_id,
                    config.replica_id,
                ));
            }
            let metadata = wait(
                &mut execution,
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
            .await?;
            Ok((catalog.transient_revision(), metadata))
        }
        .await;
        match result {
            Ok((metadata_revision, metadata)) => {
                pending_metadata = !metadata.pending.is_empty();
                let mut pending = !effects.pending.is_empty() || pending_metadata;
                if let Some(execution) = &mut execution {
                    if execution.renewal_due() {
                        if let Err(error) = execution
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
                    // Renewal can consume peer DDL. Resolve schemas and selected
                    // producers again rather than combine metadata from two prefixes.
                    if catalog.transient_revision() != metadata_revision {
                        continue;
                    }
                    match execution
                        .install_sources(
                            &mut catalog,
                            &mut effects,
                            config.cluster_id,
                            config.replica_id,
                            &build,
                            &metadata,
                        )
                        .await
                    {
                        Ok(source_pending) => pending |= source_pending,
                        Err(error) => {
                            failures["installation"].inc();
                            pending = true;
                            tracing::warn!(%error, "source installation pending");
                        }
                    }
                    match execution
                        .install_sinks(
                            &mut catalog,
                            &mut effects,
                            config.cluster_id,
                            &build,
                            &metadata,
                        )
                        .await
                    {
                        Ok(sink_pending) => pending |= sink_pending,
                        Err(error) => {
                            failures["installation"].inc();
                            pending = true;
                            tracing::warn!(%error, "sink installation pending");
                        }
                    }
                    let result = execution
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
                            %error, "execution installation pending");
                    }
                    pending |= execution.pending_installations(&effects) > 0;
                    execution.apply_progress(&catalog);
                    execution.apply_catalog(
                        &catalog,
                        &build,
                        &metadata,
                        effects.pending.is_empty(),
                    );
                    let interval = catalog
                        .system_config()
                        .catalog_read_protection_publish_interval();
                    if interval != publication_interval {
                        publication_interval = interval;
                        publication_after = tokio::time::Instant::now() + interval;
                    }
                    let publish_bounds = tokio::time::Instant::now() >= publication_after;
                    if let Err(error) = execution
                        .publish(
                            &mut catalog,
                            &mut effects,
                            config.cluster_id,
                            &build,
                            pending,
                            publish_bounds.then_some(&metadata),
                        )
                        .await
                    {
                        failures["publication"].inc();
                        tracing::warn!(%error, "replica protection publication pending");
                    } else if publish_bounds && !pending {
                        publication_after = tokio::time::Instant::now() + publication_interval;
                    }
                    let wanted = metadata.metadata.keys().copied().collect();
                    if let Err(error) = execution
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
                        execution
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
                        observed_uppers = metadata.uppers.len(),
                        replica_owned = execution.is_some(),
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
                if let Some(execution) = &mut execution {
                    // Missing metadata cannot prevent renewal of existing protection.
                    if let Err(error) = execution
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
                        %error, replica_owned = execution.is_some(), "catalog follower stalled");
                    last_report = tokio::time::Instant::now();
                }
                last_error = Some(error);
                delay = (delay * 2).min(Duration::from_secs(10));
            }
        }
        wait(&mut execution, tokio::time::sleep(delay)).await;
    }
}

async fn wait<F: std::future::Future>(
    execution: &mut Option<execution::ReplicaEnactment>,
    future: F,
) -> F::Output {
    match execution {
        Some(execution) => execution.io.wait(future).await,
        None => future.await,
    }
}
