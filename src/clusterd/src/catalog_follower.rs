// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replica-local observation of committed desired state. The controller remains
//! the sole installer. Nothing here acquires read protection or enacts bounds.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use mz_catalog::builtin::{BUILTINS, Builtin};
use mz_catalog::durable::objects::{self, DurableType};
use mz_catalog::durable::{Metrics, Snapshot, persist_backed_catalog_state};
use mz_catalog::expr_cache::{ExpressionCacheHandle, GlobalExpressions, expression_build_version};
use mz_controller_types::ClusterId;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use mz_proto::RustType;
use mz_repr::{GlobalId, RelationVersion};
use mz_sql::catalog::CatalogItemType;
use mz_sql_parser::ast::{RawClusterName, Statement};
use uuid::Uuid;

mod storage_metadata;

pub(crate) struct Config {
    pub organization_id: Uuid,
    pub cluster_id: ClusterId,
    pub replica_id: mz_cluster_client::ReplicaId,
    pub deploy_generation: u64,
    pub persist_location: PersistLocation,
    pub build_info: &'static mz_build_info::BuildInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Member {
    WrittenPlan(RelationVersion),
    Introspection,
    Storage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Pending {
    Selection,
    Bytes(Uuid),
    ItemVersion,
    Imports(BTreeSet<GlobalId>),
    Dependencies(BTreeSet<GlobalId>),
    StorageMetadata(BTreeMap<GlobalId, storage_metadata::Pending>),
}

/// A complete committed prefix, including full definitions and desired bounds.
/// Plans remain intact even when dependencies are missing. Presence here is not
/// an installation acknowledgement or proof that any input is readable.
struct DesiredState {
    snapshot: Snapshot,
    members: BTreeMap<GlobalId, Member>,
    selections: BTreeMap<GlobalId, Uuid>,
    plans: BTreeMap<(GlobalId, Uuid), GlobalExpressions>,
    pending: BTreeMap<GlobalId, Pending>,
    storage_metadata: storage_metadata::Resolution,
}

/// Observe with a retained generation-bound join, without allocating an epoch or
/// incarnation. Retry delays are capped, while persistent stalls remain visible.
pub(crate) async fn run(
    config: Config,
    persist_clients: Arc<PersistClientCache>,
    metrics_registry: MetricsRegistry,
) -> anyhow::Result<()> {
    let persist = persist_clients
        .open(config.persist_location.clone())
        .await?;
    let mut catalog = persist_backed_catalog_state(
        persist.clone(),
        config.organization_id,
        config.build_info.semver_version(),
        Some(config.deploy_generation),
        Arc::new(Metrics::new(&metrics_registry)),
    )
    .await?
    .join()
    .await?;
    let build = expression_build_version(config.build_info);
    let mut store = None;
    let mut desired: Option<DesiredState> = None;
    let mut delay = Duration::from_secs(1);
    let mut last_report = tokio::time::Instant::now();
    let mut last_error = None;
    loop {
        let result: anyhow::Result<DesiredState> = async {
            // These updates only drain the handle's delivery queue. snapshot()
            // synchronizes independently and is the sole source of derived state.
            // Applying either queue to the snapshot would double-apply commits.
            catalog.sync_to_current_updates().await?;
            let snapshot = catalog.snapshot().await?;
            let shard = snapshot
                .settings
                .iter()
                .find(|(key, _)| key.name == mz_catalog::durable::EXPRESSION_CACHE_SHARD_KEY)
                .context("catalog has no expression shard")?
                .1
                .value
                .parse()
                .map_err(anyhow::Error::msg)?;
            if store.as_ref().map(|(id, _)| *id) != Some(shard) {
                store = Some((
                    shard,
                    ExpressionCacheHandle::open_plan_store(build.clone(), &persist, shard).await,
                ));
            }
            let mut next = derive(snapshot, config.cluster_id, &build.to_string())?;
            let revisions = next
                .selections
                .iter()
                .map(|(id, rev)| (*id, *rev))
                .collect();
            next.plans = store
                .as_ref()
                .expect("opened above")
                .1
                .read_plans(revisions)
                .await?;
            next.check_plans()?;
            let wanted = next
                .plans
                .iter()
                .filter(|((id, _), _)| !next.pending.contains_key(id))
                .flat_map(|(_, plan)| {
                    plan.physical_plan
                        .imported_source_ids()
                        .chain(plan.physical_plan.persist_sink_ids())
                })
                .collect();
            next.storage_metadata = storage_metadata::resolve(
                &next.snapshot,
                &wanted,
                &store.as_ref().expect("opened above").1,
                &build.to_string(),
                &persist,
                &config.persist_location,
            )
            .await?;
            next.check_storage_metadata();
            Ok(next)
        }
        .await;
        match result {
            Ok(next) => {
                let changed = desired.as_ref().is_none_or(|old| {
                    old.members != next.members
                        || old.selections != next.selections
                        || old.pending != next.pending
                        || old.snapshot.collection_compaction_bounds
                            != next.snapshot.collection_compaction_bounds
                });
                if changed || last_error.is_some() {
                    tracing::info!(cluster = %config.cluster_id, replica = %config.replica_id,
                        members = next.members.len(), plans = next.plans.len(),
                        storage_inputs = next.storage_metadata.metadata.len(),
                        observed_uppers = next.storage_metadata.uppers.len(),
                        pending = ?next.pending, "catalog follower desired state changed (not enacted)");
                    last_report = tokio::time::Instant::now();
                } else if !next.pending.is_empty()
                    && last_report.elapsed() >= Duration::from_secs(60)
                {
                    tracing::warn!(cluster = %config.cluster_id, pending = ?next.pending,
                        "catalog follower waiting for written plans or dependencies");
                    last_report = tokio::time::Instant::now();
                }
                delay = if next.pending.is_empty() || changed {
                    Duration::from_secs(1)
                } else {
                    (delay * 2).min(Duration::from_secs(10))
                };
                last_error = None;
                desired = Some(next);
            }
            Err(error) => {
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

/// Extract placement syntactically. Ordinary durable SQL must already contain
/// resolved IDs. Only builtin SQL may resolve names against durable clusters.
fn placement(
    sql: &str,
    clusters: Option<&BTreeMap<String, ClusterId>>,
) -> anyhow::Result<Option<(ClusterId, bool)>> {
    let mut statements = mz_sql_parser::parser::parse_statements(sql)?;
    anyhow::ensure!(
        statements.len() == 1,
        "expected one canonical CREATE statement"
    );
    let (cluster, compute) = match statements.remove(0).ast {
        Statement::CreateIndex(s) => (s.in_cluster, true),
        Statement::CreateMaterializedView(s) => (s.in_cluster, true),
        Statement::CreateMetricSink(s) => (s.in_cluster, true),
        Statement::CreateSource(s) => (s.in_cluster, false),
        Statement::CreateWebhookSource(s) if !s.is_table => (s.in_cluster, false),
        Statement::CreateSink(s) => (s.in_cluster, false),
        _ => return Ok(None),
    };
    let cluster = match cluster {
        Some(RawClusterName::Resolved(id)) => id.parse()?,
        Some(RawClusterName::Unresolved(name)) => *clusters
            .and_then(|clusters| clusters.get(name.as_str()))
            .with_context(|| format!("unresolved maintained cluster {name}"))?,
        None => bail!("maintained object has no explicit cluster"),
    };
    Ok(Some((cluster, compute)))
}

fn derive(snapshot: Snapshot, cluster_id: ClusterId, build: &str) -> anyhow::Result<DesiredState> {
    let mut members = BTreeMap::new();
    for (key, value) in &snapshot.items {
        let item = objects::Item::from_key_value(
            RustType::from_proto(key.clone())?,
            RustType::from_proto(value.clone())?,
        );
        if item.ephemeral_owner_session.is_some() {
            continue;
        }
        if let Some((placement, compute)) =
            placement(&item.create_sql, None).with_context(|| format!("item {}", item.id))?
            && placement == cluster_id
        {
            let (version, writer_id) = item.extra_versions.last_key_value().map_or_else(
                || (RelationVersion::root(), item.global_id),
                |(v, id)| (*v, *id),
            );
            for id in std::iter::once(item.global_id).chain(item.extra_versions.values().copied()) {
                // Replaced MV outputs remain readable aliases. Only the current
                // output owns a writer and requires a selected execution plan.
                let retired_alias =
                    item.item_type() == CatalogItemType::MaterializedView && id != writer_id;
                members.insert(
                    id,
                    if compute && !retired_alias {
                        Member::WrittenPlan(version)
                    } else {
                        Member::Storage
                    },
                );
            }
        }
    }
    let clusters = snapshot
        .clusters
        .iter()
        .map(|(key, value)| {
            let cluster = objects::Cluster::from_key_value(
                RustType::from_proto(key.clone())?,
                RustType::from_proto(value.clone())?,
            );
            Ok((cluster.name, cluster.id))
        })
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
    let builtins: BTreeMap<_, _> = BUILTINS::iter()
        .map(|builtin| {
            (
                objects::SystemObjectDescription {
                    schema_name: builtin.schema().into(),
                    object_type: builtin.catalog_item_type(),
                    object_name: builtin.name().into(),
                },
                builtin,
            )
        })
        .collect();
    for (key, value) in &snapshot.system_object_mappings {
        let mapping = objects::SystemObjectMapping::from_key_value(
            RustType::from_proto(key.clone())?,
            RustType::from_proto(value.clone())?,
        );
        let description = &mapping.description;
        // Unknown build definitions are a blocker, not evidence that the object
        // does not belong to this cluster.
        let builtin = builtins
            .get(description)
            .with_context(|| format!("unknown builtin {description:?}"))?;
        let sql = match builtin {
            Builtin::Index(index) => index.create_sql(),
            Builtin::MaterializedView(view) => view.create_sql(),
            _ => continue,
        };
        if let Some((placement, _)) = placement(&sql, Some(&clusters))?
            && placement == cluster_id
        {
            members.insert(
                mapping.unique_identifier.global_id,
                Member::WrittenPlan(RelationVersion::root()),
            );
        }
    }
    for (key, value) in &snapshot.introspection_sources {
        let index = objects::IntrospectionSourceIndex::from_key_value(
            RustType::from_proto(key.clone())?,
            RustType::from_proto(value.clone())?,
        );
        if index.cluster_id == cluster_id {
            members.insert(index.index_id, Member::Introspection);
        }
    }
    let mut selections = BTreeMap::new();
    for (key, value) in &snapshot.written_plans {
        let selection = objects::WrittenPlan::from_key_value(
            RustType::from_proto(key.clone())?,
            RustType::from_proto(value.clone())?,
        );
        if selection.build_version == build
            && matches!(members.get(&selection.id), Some(Member::WrittenPlan(_)))
        {
            selections.insert(selection.id, selection.revision);
        }
    }
    Ok(DesiredState {
        snapshot,
        members,
        selections,
        plans: BTreeMap::new(),
        pending: BTreeMap::new(),
        storage_metadata: Default::default(),
    })
}

impl DesiredState {
    fn check_plans(&mut self) -> anyhow::Result<()> {
        let mut live = BTreeSet::new();
        for (key, value) in &self.snapshot.items {
            let item = objects::Item::from_key_value(
                RustType::from_proto(key.clone())?,
                RustType::from_proto(value.clone())?,
            );
            if item.ephemeral_owner_session.is_none()
                && (item.item_type() != CatalogItemType::Index
                    || self.members.contains_key(&item.global_id))
            {
                live.insert(item.global_id);
                live.extend(item.extra_versions.values().copied());
            }
        }
        for (key, value) in &self.snapshot.system_object_mappings {
            let mapping = objects::SystemObjectMapping::from_key_value(
                RustType::from_proto(key.clone())?,
                RustType::from_proto(value.clone())?,
            );
            let id = mapping.unique_identifier.global_id;
            if mapping.description.object_type != CatalogItemType::Index
                || self.members.contains_key(&id)
            {
                live.insert(id);
            }
        }
        for (key, value) in &self.snapshot.introspection_sources {
            let index = objects::IntrospectionSourceIndex::from_key_value(
                RustType::from_proto(key.clone())?,
                RustType::from_proto(value.clone())?,
            );
            if self.members.contains_key(&index.index_id) {
                live.insert(index.index_id);
            }
        }
        self.pending.clear();
        for (id, member) in &self.members {
            let Member::WrittenPlan(version) = member else {
                continue;
            };
            let Some(revision) = self.selections.get(id) else {
                self.pending.insert(*id, Pending::Selection);
                continue;
            };
            let Some(plan) = self.plans.get(&(*id, *revision)) else {
                self.pending.insert(*id, Pending::Bytes(*revision));
                continue;
            };
            if plan.item_version != *version {
                self.pending.insert(*id, Pending::ItemVersion);
                continue;
            }
            let missing: BTreeSet<_> = plan
                .collection_imports()
                .filter(|id| !live.contains(id))
                .copied()
                .collect();
            if !missing.is_empty() {
                self.pending.insert(*id, Pending::Imports(missing));
            }
        }
        self.propagate_pending();
        Ok(())
    }

    fn check_storage_metadata(&mut self) {
        for ((id, _), plan) in &self.plans {
            if self.pending.contains_key(id) {
                continue;
            }
            let missing: BTreeMap<_, _> = plan
                .physical_plan
                .imported_source_ids()
                .chain(plan.physical_plan.persist_sink_ids())
                .filter_map(|id| {
                    self.storage_metadata
                        .pending
                        .get(&id)
                        .map(|reason| (id, reason.clone()))
                })
                .collect();
            if !missing.is_empty() {
                self.pending.insert(*id, Pending::StorageMetadata(missing));
            }
        }
        self.propagate_pending();
    }

    fn propagate_pending(&mut self) {
        // Propagate pending dependencies through the complete prefix, independent
        // of ID ordering. This is desired-state readiness, not worker readiness.
        loop {
            let mut additions = BTreeMap::new();
            for (id, revision) in &self.selections {
                if self.pending.contains_key(id) {
                    continue;
                }
                let plan = &self.plans[&(*id, *revision)];
                let dependencies: BTreeSet<_> = plan
                    .collection_imports()
                    .filter(|id| self.pending.contains_key(id))
                    .copied()
                    .collect();
                if !dependencies.is_empty() {
                    additions.insert(*id, Pending::Dependencies(dependencies));
                }
            }
            if additions.is_empty() {
                break;
            }
            self.pending.extend(additions);
        }
    }
}

#[cfg(test)]
mod tests;
