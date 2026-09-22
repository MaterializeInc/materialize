// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replica-owned execution, read protection, and progress publication.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, bail, ensure};
use differential_dataflow::lattice::Lattice;
use mz_catalog::catalog::{Catalog, Op};
use mz_catalog::memory::objects::CatalogItem;
use mz_catalog::read_protection::publication::publication_candidates;
use mz_catalog::read_protection::{
    CLIENT_PROTECTION_HEARTBEAT_INTERVAL, CLIENT_PROTECTION_UNCHANGED_GRACE,
    ClientProtectionReclaimer, ClientReadProtection,
};
use mz_compute::server::ReplicaCompute;
use mz_compute_client::as_of_selection::{self, CapturedLiveInput};
use mz_compute_client::protocol::command::{ComputeCommand, InstanceConfig};
use mz_compute_client::protocol::response::{ComputeResponse, FrontiersResponse};
use mz_compute_client::sequential_hydration::SequentialHydration;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::LirRelationExpr;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::metrics::{MetricVecExt, MetricsRegistry};
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::storage_collections::CollectionFrontiers;
use mz_storage_types::read_holds::{ChangeTx, ReadHold};
use mz_storage_types::time_dependence::TimeDependence;
use timely::PartialOrder;
use timely::progress::Antichain;
use timely::progress::frontier::MutableAntichain;

use super::{ReplicaEffects, absorb_updates, storage_metadata, time_dependence};

type Plan = DataflowDescription<LirRelationExpr, ()>;

mod storage;

#[cfg(test)]
mod lag_tests;
#[cfg(test)]
mod liveness_tests;
#[cfg(test)]
mod metric_tests;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod tests;

/// Drains runtime progress even while catalog/Persist operations are outstanding.
/// Frontiers coalesce by field, so slow metadata I/O does not accumulate a queue
/// proportional to elapsed time. Unobserved fields remain unknown.
pub(super) struct ReplicaIo {
    endpoint: ReplicaCompute,
    storage: Option<storage::StorageIo>,
    frontiers: BTreeMap<GlobalId, FrontiersResponse>,
    hydration: SequentialHydration,
    config: mz_dyncfg::ConfigSet,
}

impl ReplicaIo {
    pub async fn wait<F: Future>(&mut self, future: F) -> F::Output {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => return result,
                response = async {
                    match &mut self.storage {
                        Some(storage) => storage.endpoint.recv().await,
                        None => std::future::pending().await,
                    }
                } => match response {
                    Ok(Some(response)) => self.storage.as_mut().expect("storage endpoint").absorb(response),
                    result => mz_ore::halt!("replica storage progress lost: {result:?}"),
                },
                response = self.endpoint.recv() => match response {
                    Ok(Some(response)) => {
                        for command in self.hydration.observe_response(&response, &self.config) {
                            self.endpoint.send(command);
                        }
                        let ComputeResponse::Frontiers(id, update) = response else { continue; };
                        let current = self.frontiers.entry(id).or_default();
                        if update.read_frontier.is_some() {
                            current.read_frontier = update.read_frontier;
                        }
                        if update.write_frontier.is_some() {
                            current.write_frontier = update.write_frontier;
                        }
                        if update.input_frontier.is_some() {
                            current.input_frontier = update.input_frontier;
                        }
                        if update.output_frontier.is_some() {
                            current.output_frontier = update.output_frontier;
                        }
                        if update.hydrated.is_some() {
                            current.hydrated = update.hydrated;
                        }
                    }
                    result => mz_ore::halt!("replica compute progress lost: {result:?}"),
                }
            }
        }
    }

    fn send(&mut self, command: ComputeCommand) {
        match &command {
            ComputeCommand::CreateInstance(config) => config.initial_config.apply(&self.config),
            ComputeCommand::UpdateConfiguration(config) => {
                config.dyncfg_updates.apply(&self.config)
            }
            _ => (),
        }
        for command in self.hydration.absorb_command(command, &self.config) {
            self.endpoint.send(command);
        }
    }
}

#[derive(Default)]
struct LocalReadProtection {
    active: Arc<Mutex<BTreeMap<GlobalId, MutableAntichain<Timestamp>>>>,
}

impl LocalReadProtection {
    /// The sole command owner captures its enforced ceiling before issuing the
    /// token. The token then constrains every subsequent compaction/drop command.
    fn acquire(&self, id: GlobalId, since: Antichain<Timestamp>) -> ReadHold {
        self.active
            .lock()
            .expect("local holds mutex")
            .entry(id)
            .or_default()
            .update_iter(since.iter().map(|time| (*time, 1)));
        let active = Arc::clone(&self.active);
        let change: ChangeTx = Arc::new(move |id, mut changes| {
            let mut active = active.lock().expect("local holds mutex");
            let frontier = active.entry(id).or_default();
            frontier.update_iter(changes.drain());
            if frontier.frontier().is_empty() {
                active.remove(&id);
            }
            Ok(())
        });
        ReadHold::new(id, since, change)
    }

    fn frontier(&self, id: GlobalId) -> Antichain<Timestamp> {
        self.active
            .lock()
            .expect("local holds mutex")
            .get(&id)
            .map(|active| active.frontier().to_owned())
            .unwrap_or_default()
    }
}

struct Installed {
    as_of: Antichain<Timestamp>,
    /// Last ordered compaction ceiling, not a best-effort worker observation.
    allowed: Antichain<Timestamp>,
    progress: FrontiersResponse,
    /// One clone per export. DROP cannot release these ahead of input completion.
    execution: BTreeMap<GlobalId, ReadHold>,
    /// Index plus persisted logical leaves, independent of physical execution.
    window: Option<ReadHold>,
    index: bool,
    retired: bool,
    scheduled: bool,
    writes_allowed: bool,
    storage_inputs: BTreeSet<GlobalId>,
    compute_inputs: BTreeSet<GlobalId>,
    time_dependence: Option<TimeDependence>,
}

pub(super) struct ReplicaEnactment {
    pub io: ReplicaIo,
    cluster: ClusterId,
    replica: ReplicaId,
    protection: ClientReadProtection,
    local: LocalReadProtection,
    installed: BTreeMap<GlobalId, Installed>,
    pending_grants: BTreeMap<GlobalId, ReadHold>,
    pending_imports: BTreeMap<(GlobalId, uuid::Uuid), BTreeMap<GlobalId, ReadHold>>,
    storage_state: storage::StorageState,
    reclaimer: ClientProtectionReclaimer,
    published_at: Instant,
}

impl ReplicaEnactment {
    pub fn new(
        endpoint: ReplicaCompute,
        config: InstanceConfig,
        incarnation: u64,
        publication_started: Instant,
        cluster: ClusterId,
        replica: ReplicaId,
        registry: &MetricsRegistry,
        storage_endpoint: Option<mz_storage::server::ReplicaStorage>,
    ) -> Self {
        let logs = config
            .logging
            .index_logs
            .values()
            .copied()
            .collect::<Vec<_>>();
        let gauge = SequentialHydration::register_queue_metric(registry);
        let hydration = SequentialHydration::new(
            gauge.get_delete_on_drop_metric(vec![cluster.to_string(), replica.to_string()]),
        );
        let mut io = ReplicaIo {
            endpoint,
            storage: storage_endpoint.map(storage::StorageIo::new),
            frontiers: BTreeMap::new(),
            hydration,
            config: mz_dyncfgs::all_dyncfgs(),
        };
        io.send(ComputeCommand::CreateInstance(Box::new(config)));
        let mut result = Self {
            io,
            cluster,
            replica,
            protection: ClientReadProtection::new(incarnation),
            local: LocalReadProtection::default(),
            installed: BTreeMap::new(),
            pending_grants: BTreeMap::new(),
            pending_imports: BTreeMap::new(),
            storage_state: storage::StorageState::default(),
            reclaimer: ClientProtectionReclaimer::default(),
            published_at: publication_started,
        };
        for id in logs {
            result.installed.insert(
                id,
                Installed {
                    as_of: Antichain::from_elem(Timestamp::MIN),
                    allowed: Antichain::from_elem(Timestamp::MIN),
                    progress: FrontiersResponse::default(),
                    execution: BTreeMap::new(),
                    window: None,
                    index: true,
                    retired: false,
                    scheduled: true,
                    writes_allowed: true,
                    storage_inputs: BTreeSet::new(),
                    compute_inputs: BTreeSet::new(),
                    time_dependence: Some(TimeDependence::default()),
                },
            );
        }
        result
    }

    async fn transact(
        &mut self,
        catalog: &mut Catalog,
        effects: &mut ReplicaEffects,
        cluster: ClusterId,
        build: &str,
        ops: Vec<Op>,
    ) -> anyhow::Result<()> {
        let ts = self.io.wait(catalog.current_upper()).await;
        let result = self.io.wait(catalog.transact(None, ts, None, ops)).await?;
        absorb_updates(effects, catalog, cluster, build, result.catalog_updates);
        Ok(())
    }

    async fn acquire(
        &mut self,
        catalog: &mut Catalog,
        effects: &mut ReplicaEffects,
        cluster: ClusterId,
        build: &str,
        storage: &BTreeSet<GlobalId>,
        compute: &BTreeSet<GlobalId>,
        requested: &BTreeMap<GlobalId, Timestamp>,
    ) -> anyhow::Result<BTreeMap<GlobalId, ReadHold>> {
        self.ensure_live(catalog)?;
        self.ensure_recent_protection()?;
        let dependencies = compute
            .iter()
            .map(|id| {
                let entry = catalog.get_entry_by_global_id(id);
                let CatalogItem::Index(index) = entry.item() else {
                    unreachable!("compute grants target indexes");
                };
                let leaves = catalog
                    .state()
                    .logical_collection_inputs([index.on])
                    .into_iter()
                    .filter(|input| {
                        !matches!(
                            catalog.get_entry_by_global_id(input).item(),
                            CatalogItem::Log(_)
                        )
                    })
                    .collect();
                (*id, leaves)
            })
            .collect();
        if let Some(holds) =
            self.protection
                .try_acquire(storage, compute, requested, &dependencies)?
        {
            return Ok(holds);
        }
        let incarnation = self.protection.incarnation();
        let extra = catalog
            .state()
            .expand_client_read_requirements(incarnation, requested.clone())?;
        let requirements = self.protection.prepare_publication(extra);
        self.commit_grants(catalog, effects, cluster, build, requirements)
            .await?;
        self.ensure_recent_protection()?;
        self.protection
            .try_acquire(storage, compute, requested, &dependencies)?
            .context("acknowledged replica protection was not acquired")
    }

    /// Installs the ready portion of the pending DAG. Source and live-window
    /// grants precede selection. Missing prerequisites block their consumers,
    /// not independent execution. Publication still waits for every installation.
    pub async fn install(
        &mut self,
        catalog: &mut Catalog,
        effects: &mut ReplicaEffects,
        cluster: ClusterId,
        build: &str,
        store: &mz_catalog::expr_cache::ExpressionCacheHandle,
        persist: &mz_persist_client::PersistClient,
        metadata: &storage_metadata::Resolution,
    ) -> anyhow::Result<()> {
        self.capture_pending_imports(effects);
        self.ensure_live(catalog)?;
        let windows: BTreeSet<_> = self
            .installed
            .iter()
            .filter_map(|(id, c)| (c.index && !c.retired && c.window.is_none()).then_some(*id))
            .collect();
        if !windows.is_empty() {
            let requested = windows
                .iter()
                .map(|id| {
                    let mut since = self.installed[id].as_of.clone();
                    if let Some(bound) = catalog.state().collection_compaction_bounds().get(id) {
                        since.join_assign(bound);
                    }
                    Ok((
                        *id,
                        since
                            .into_option()
                            .context("logging index is not readable")?,
                    ))
                })
                .collect::<anyhow::Result<_>>()?;
            let holds = self
                .acquire(
                    catalog,
                    effects,
                    cluster,
                    build,
                    &BTreeSet::new(),
                    &windows,
                    &requested,
                )
                .await?;
            for (id, hold) in holds {
                self.installed.get_mut(&id).expect("logging index").window = Some(hold);
            }
        }
        let mut candidates: Vec<Plan> = effects
            .selected
            .values()
            .filter(|(id, revision, _)| {
                // Renewal can refresh the catalog after the plan-byte fetch.
                // Only a still-selected revision is installation authority.
                !self.installed.contains_key(id)
                    && catalog.state().written_plan(*id, build) == Some(*revision)
            })
            .map(|(id, _, plan)| {
                let mut plan = plan.physical_plan.clone();
                if let Some(entry) = catalog.try_get_entry_by_global_id(id)
                    && let CatalogItem::MaterializedView(mv) = entry.item()
                {
                    mv.apply_execution_bounds(&mut plan);
                }
                plan
            })
            .collect();
        let mut ready: BTreeSet<_> = self
            .installed
            .iter()
            .filter_map(|(id, collection)| {
                (!collection.retired && collection.progress.write_frontier.is_some()).then_some(*id)
            })
            .collect();
        let mut plans = Vec::new();
        while let Some(position) = candidates.iter().position(|plan| {
            plan.index_imports.keys().all(|id| ready.contains(id))
                && plan
                    .imported_source_ids()
                    .chain(plan.persist_sink_ids())
                    .all(|id| metadata.metadata.contains_key(&id))
        }) {
            let plan = candidates.remove(position);
            ready.extend(plan.export_ids());
            plans.push(plan);
        }
        if plans.is_empty() {
            self.pending_grants.clear();
            return Ok(());
        }
        let dependencies = self
            .installed
            .iter()
            .map(|(id, c)| (*id, c.time_dependence.clone()))
            .collect();
        self.io
            .wait(time_dependence::resolve(
                catalog,
                store,
                build,
                &mut plans,
                &dependencies,
            ))
            .await?;
        let storage: BTreeSet<_> = plans
            .iter()
            .flat_map(|p| p.source_imports.keys().copied())
            .collect();
        let indexes: BTreeSet<_> = plans
            .iter()
            .flat_map(|p| p.index_exports.keys().copied())
            .collect();
        self.pending_grants
            .retain(|id, _| storage.contains(id) || indexes.contains(id));
        let mut requested = BTreeMap::new();
        for id in &storage {
            let floor = self
                .protection
                .granted_frontier(*id)
                .or_else(|| {
                    catalog
                        .state()
                        .collection_compaction_bounds()
                        .get(id)
                        .and_then(|f| f.as_option().copied())
                })
                .with_context(|| format!("source {id} has no readable permission"))?;
            requested.insert(*id, floor);
        }
        for id in &indexes {
            let entry = catalog.get_entry_by_global_id(id);
            let CatalogItem::Index(index) = entry.item() else {
                bail!("index export {id} has no index definition");
            };
            let mut floor = catalog
                .state()
                .collection_compaction_bounds()
                .get(id)
                .cloned()
                .unwrap_or_else(|| Antichain::from_elem(Timestamp::MIN));
            for input in catalog.state().logical_collection_inputs([index.on]) {
                if matches!(
                    catalog.get_entry_by_global_id(&input).item(),
                    CatalogItem::Log(_)
                ) {
                    continue;
                }
                floor.join_assign(
                    catalog
                        .state()
                        .collection_compaction_bounds()
                        .get(&input)
                        .with_context(|| format!("logical input {input} has no permission"))?,
                );
            }
            requested.insert(
                *id,
                floor.into_option().context("index history is exhausted")?,
            );
        }
        let grants = self
            .acquire(
                catalog, effects, cluster, build, &storage, &indexes, &requested,
            )
            .await?;
        self.pending_grants = grants.clone();
        let uppers = self
            .io
            .wait(storage_metadata::uppers(persist, &metadata.metadata))
            .await?;
        // All remaining selection and command emission is synchronous. A slow
        // observation must not resume installation using possibly reclaimed grants.
        // Worker-side Persist checks still cover the asynchronous reader-open gap.
        self.ensure_recent_protection()?;
        let protected = storage
            .iter()
            .map(|id| (*id, grants[id].since().clone()))
            .collect();
        let frontiers = uppers
            .iter()
            .map(|(id, upper)| {
                let since = grants
                    .get(id)
                    .map(|h| h.since().clone())
                    .or_else(|| {
                        catalog
                            .state()
                            .collection_compaction_bounds()
                            .get(id)
                            .cloned()
                    })
                    .unwrap_or_else(|| Antichain::from_elem(Timestamp::MIN));
                (
                    *id,
                    CollectionFrontiers {
                        id: *id,
                        write_frontier: upper.clone(),
                        implied_capability: since.clone(),
                        read_capabilities: since,
                    },
                )
            })
            .collect();
        let mut live = BTreeMap::new();
        for id in plans
            .iter()
            .flat_map(|p| p.index_imports.keys())
            .filter(|id| !indexes.contains(id))
        {
            let input = self
                .installed
                .get(id)
                .with_context(|| format!("compute input {id} is pending"))?;
            ensure!(!input.retired, "compute input {id} was retired");
            let upper = input
                .progress
                .write_frontier
                .clone()
                .with_context(|| format!("compute input {id} has no progress"))?;
            let hold = self
                .pending_imports
                .values()
                .find_map(|holds| holds.get(id))
                .expect("live import protected before installation work");
            live.insert(
                *id,
                CapturedLiveInput {
                    protected_since: hold.since().clone(),
                    write_upper: upper,
                },
            );
        }
        let policies = indexes
            .iter()
            .filter_map(|id| catalog.state().index_read_policy(*id).map(|p| (*id, p)))
            .collect();
        let bounds = catalog
            .state()
            .collection_compaction_bounds()
            .iter()
            .map(|(id, f)| (*id, f.clone()))
            .collect();
        let replacements = plans
            .iter()
            .flat_map(|p| p.export_ids())
            .filter_map(|id| {
                let entry = catalog.try_get_entry_by_global_id(&id)?;
                let CatalogItem::MaterializedView(mv) = entry.item() else {
                    return None;
                };
                mv.replacement_target.map(|_| {
                    (
                        id,
                        mv.initial_as_of.clone().expect("replacement visibility"),
                    )
                })
            })
            .collect();
        as_of_selection::select(
            &mut plans,
            &policies,
            &bounds,
            &replacements,
            &protected,
            &frontiers,
            &live,
            mz_ore::now::SYSTEM_TIME().into(),
            false,
            true,
        )?;

        // Written plans may refer to newer access paths. Catalog ID order is not
        // a topological order. Render producers before importing their traces.
        while !plans.is_empty() {
            let position = plans
                .iter()
                .position(|p| {
                    p.index_imports
                        .keys()
                        .all(|id| self.installed.contains_key(id))
                })
                .context("pending compute plans do not form an installable DAG")?;
            let plan = plans.remove(position);
            let as_of = plan.as_of.clone().expect("selected as_of");
            let source_holds: BTreeMap<_, _> = if as_of.is_empty() {
                BTreeMap::new()
            } else {
                plan.source_imports
                    .keys()
                    .map(|id| {
                        let mut hold = grants[id].clone();
                        hold.try_downgrade(as_of.clone())
                            .expect("selected above protected source");
                        (*id, hold)
                    })
                    .collect()
            };
            for id in plan.export_ids() {
                let window = grants.get(&id).map(|hold| {
                    let mut hold = hold.clone();
                    let frontier = hold.since().join(&as_of);
                    hold.try_downgrade(frontier).expect("joined window");
                    hold
                });
                self.installed.insert(
                    id,
                    Installed {
                        as_of: as_of.clone(),
                        allowed: as_of.clone(),
                        progress: if as_of.is_empty() {
                            // No worker dataflow is created for a sealed plan.
                            FrontiersResponse {
                                read_frontier: Some(Antichain::new()),
                                write_frontier: Some(Antichain::new()),
                                input_frontier: Some(Antichain::new()),
                                output_frontier: Some(Antichain::new()),
                                hydrated: Some(true),
                            }
                        } else {
                            FrontiersResponse::default()
                        },
                        execution: source_holds.clone(),
                        window,
                        index: plan.index_exports.contains_key(&id),
                        retired: false,
                        scheduled: as_of.is_empty(),
                        writes_allowed: as_of.is_empty()
                            || !plan.persist_sink_ids().any(|sink| sink == id),
                        storage_inputs: plan.source_imports.keys().copied().collect(),
                        compute_inputs: plan.index_imports.keys().copied().collect(),
                        time_dependence: plan.time_dependence.clone(),
                    },
                );
            }
            if !as_of.is_empty() {
                let plan = plan.into_render_plan::<_, anyhow::Error>(
                    |id| Ok((metadata.metadata[&id].clone(), uppers[&id].clone())),
                    |id| Ok(metadata.metadata[&id].clone()),
                )?;
                self.io.send(ComputeCommand::CreateDataflow(Box::new(plan)));
            }
        }
        // Every creation is ordered before any policy advancement or export DROP.
        // Runtime TraceAgents now own each import's execution protection.
        self.pending_imports.clear();
        self.pending_grants.clear();
        Ok(())
    }

    fn capture_pending_imports(&mut self, effects: &ReplicaEffects) {
        let wanted: BTreeSet<_> = effects
            .selected
            .values()
            .filter(|(id, _, _)| !self.installed.contains_key(id))
            .map(|(id, revision, _)| (*id, *revision))
            .collect();
        self.pending_imports.retain(|key, _| wanted.contains(key));
        for (id, revision, plan) in effects.selected.values() {
            if !wanted.contains(&(*id, *revision)) {
                continue;
            }
            let holds = self.pending_imports.entry((*id, *revision)).or_default();
            for input in plan.physical_plan.index_imports.keys() {
                if holds.contains_key(input) {
                    continue;
                }
                if let Some(collection) = self.installed.get(input)
                    && !collection.retired
                {
                    holds.insert(
                        *input,
                        self.local.acquire(*input, collection.allowed.clone()),
                    );
                }
            }
        }
    }

    pub fn ensure_live(&self, catalog: &Catalog) -> anyhow::Result<()> {
        let participant = catalog
            .state()
            .client_incarnations()
            .get(&self.protection.incarnation())
            .context("replica incarnation was reclaimed")?;
        ensure!(
            participant.replica_id == Some(self.replica),
            "replica incarnation identity mismatch"
        );
        ensure!(
            catalog
                .try_get_cluster_replica(self.cluster, self.replica)
                .is_some(),
            "replica was removed"
        );
        Ok(())
    }

    fn ensure_recent_protection(&self) -> anyhow::Result<()> {
        ensure!(
            self.published_at.elapsed()
                < CLIENT_PROTECTION_UNCHANGED_GRACE - CLIENT_PROTECTION_HEARTBEAT_INTERVAL,
            "replica protection requires renewal before installation"
        );
        Ok(())
    }

    pub fn renewal_due(&self) -> bool {
        self.published_at.elapsed() >= CLIENT_PROTECTION_HEARTBEAT_INTERVAL
    }

    async fn commit_grants(
        &mut self,
        catalog: &mut Catalog,
        effects: &mut ReplicaEffects,
        cluster: ClusterId,
        build: &str,
        requirements: BTreeMap<GlobalId, Timestamp>,
    ) -> anyhow::Result<()> {
        // A peer can observe the heartbeat before our acknowledgement arrives.
        // Its reclamation window therefore starts no earlier than this instant,
        // not the instant at which this await returns.
        let started = Instant::now();
        let incarnation = self.protection.incarnation();
        let result = self
            .transact(
                catalog,
                effects,
                cluster,
                build,
                vec![Op::PublishClientReadRequirements {
                    incarnation,
                    requirements,
                }],
            )
            .await;
        self.protection.finish_publication(result.is_ok());
        result?;
        self.published_at = started;
        Ok(())
    }

    pub fn pending_installations(&self, effects: &ReplicaEffects) -> usize {
        effects.pending.len()
            + effects
                .selected
                .values()
                .filter(|(id, _, _)| !self.installed.contains_key(id))
                .count()
    }

    pub fn configure(
        &mut self,
        parameters: mz_compute_client::protocol::command::ComputeParameters,
    ) {
        self.io
            .send(ComputeCommand::UpdateConfiguration(Box::new(parameters)));
    }

    pub fn configure_storage(&mut self, catalog: &Catalog, cluster: ClusterId, replica: ReplicaId) {
        if let Some(storage) = &mut self.io.storage {
            storage.configure(mz_catalog::storage_config::replica_storage_config(
                catalog, cluster, replica,
            ));
        }
    }

    pub fn apply_progress(&mut self, catalog: &Catalog) {
        for (id, update) in std::mem::take(&mut self.io.frontiers) {
            let Some(collection) = self.installed.get_mut(&id) else {
                continue;
            };
            if let Some(input) = &update.input_frontier {
                for hold in collection.execution.values_mut() {
                    let frontier = hold.since().join(input);
                    hold.try_downgrade(frontier).expect("joined input progress");
                }
                if input.is_empty() {
                    collection.execution.clear();
                }
            }
            if update.read_frontier.is_some() {
                collection.progress.read_frontier = update.read_frontier;
            }
            if update.write_frontier.is_some() {
                collection.progress.write_frontier = update.write_frontier;
            }
            if update.input_frontier.is_some() {
                collection.progress.input_frontier = update.input_frontier;
            }
            if update.output_frontier.is_some() {
                collection.progress.output_frontier = update.output_frontier;
            }
            if update.hydrated.is_some() {
                collection.progress.hydrated = update.hydrated;
            }
        }
        // Policy changes can advance a window even without a new progress report.
        for (id, collection) in &mut self.installed {
            if !collection.retired
                && let Some(window) = &mut collection.window
                && let Some(upper) = &collection.progress.write_frontier
                && let Some(policy) = catalog.state().index_read_policy(*id)
            {
                let frontier = window.since().join(&policy.frontier(upper.borrow()));
                window
                    .try_downgrade(frontier)
                    .expect("monotone live window");
            }
        }
        self.installed.retain(|_, c| {
            !c.retired
                || !c.execution.is_empty()
                || !c
                    .progress
                    .input_frontier
                    .as_ref()
                    .is_some_and(|f| f.is_empty())
        });
    }

    pub fn apply_catalog(
        &mut self,
        catalog: &Catalog,
        metadata: &storage_metadata::Resolution,
        apply_permissions: bool,
    ) {
        let build = mz_catalog::expr_cache::expression_build_version(catalog.config().build_info)
            .to_string();
        let ready: BTreeSet<_> = self
            .installed
            .iter()
            .filter_map(|(id, c)| c.scheduled.then_some(*id))
            .collect();
        for (id, collection) in &mut self.installed {
            if collection.retired {
                continue;
            }
            let live = (id.is_transient()
                && catalog
                    .state()
                    .written_plan_replica_owner(*id, &build)
                    .is_some_and(|owner| owner.replica_id == self.replica))
                || catalog
                    .try_get_entry_by_global_id(id)
                    .is_some_and(|entry| match entry.item() {
                        CatalogItem::MaterializedView(mv) => mv.global_id_writes() == *id,
                        _ => true,
                    });
            if !live && self.local.frontier(*id).is_empty() {
                collection.retired = true;
                collection.window = None;
                if !collection.as_of.is_empty() {
                    self.io.send(ComputeCommand::AllowCompaction {
                        id: *id,
                        frontier: Antichain::new(),
                    });
                }
                continue;
            }
            if !collection.scheduled
                && collection
                    .compute_inputs
                    .iter()
                    .all(|id| ready.contains(id))
                && collection
                    .storage_inputs
                    .iter()
                    .filter(|input| *input != id)
                    .all(|input| {
                        metadata
                            .uppers
                            .get(input)
                            .is_some_and(|upper| PartialOrder::less_than(&collection.as_of, upper))
                    })
            {
                self.io.send(ComputeCommand::Schedule(*id));
                collection.scheduled = true;
            }
            let replacement = catalog.try_get_entry_by_global_id(id).is_some_and(|entry| {
                matches!(entry.item(), CatalogItem::MaterializedView(mv)
                    if mv.replacement_target.is_some())
            });
            if !collection.index && !collection.writes_allowed && !replacement {
                self.io.send(ComputeCommand::AllowWrites(*id));
                collection.writes_allowed = true;
            }
            if apply_permissions
                && collection.index
                && let Some(bound) = catalog.state().collection_compaction_bounds().get(id)
            {
                let mut frontier = bound.clone();
                frontier.extend(self.local.frontier(*id));
                frontier.join_assign(&collection.allowed);
                if frontier != collection.allowed {
                    self.io.send(ComputeCommand::AllowCompaction {
                        id: *id,
                        frontier: frontier.clone(),
                    });
                    collection.allowed = frontier;
                }
            }
        }
    }

    pub async fn publish(
        &mut self,
        catalog: &mut Catalog,
        effects: &mut ReplicaEffects,
        cluster: ClusterId,
        build: &str,
        pending: bool,
        metadata: Option<&storage_metadata::Resolution>,
    ) -> anyhow::Result<()> {
        let incarnation = self.protection.incarnation();
        let mut refreshed = false;
        while let Some(requirements) = self
            .protection
            .prepare_publication_if_needed(self.published_at.elapsed())
        {
            match self.commit_grants(catalog, effects, cluster, build, requirements).await {
                Ok(()) => break,
                Err(error) if error.downcast_ref::<mz_catalog::catalog::CatalogError>()
                    .is_some_and(|error| matches!(error,
                        mz_catalog::catalog::CatalogError::Catalog(error) if matches!(&error.kind,
                            mz_catalog::memory::error::ErrorKind::Durable(
                                mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
                            )))) => {
                    let (_, updates) = self.io.wait(catalog.sync_to_current_updates()).await?;
                    absorb_updates(effects, catalog, cluster, build, updates);
                    self.ensure_live(catalog)?;
                    refreshed = true;
                }
                Err(error) => return Err(error),
            }
        }
        // A pending install may still need a committed prefix's earlier history.
        // Heartbeats and acquisitions continue, but bounds and reclamation wait.
        if pending || refreshed {
            return Ok(());
        }
        let mut compute_proposals = BTreeMap::new();
        for (id, collection) in &self.installed {
            if collection.retired || !collection.index {
                continue;
            }
            let Some(policy) = catalog.state().index_read_policy(*id) else {
                continue;
            };
            let Some(upper) = &collection.progress.write_frontier else {
                continue;
            };
            let proposal = if catalog
                .state()
                .collection_compaction_bounds()
                .contains_key(id)
            {
                policy.frontier(upper.borrow())
            } else {
                // Logical inputs can be absent from the physical plan. First
                // permission respects their protected reconstruction floor too.
                collection
                    .window
                    .as_ref()
                    .map(|window| collection.as_of.join(window.since()))
                    .unwrap_or_else(|| collection.as_of.clone())
            };
            compute_proposals.insert(*id, proposal);
        }
        let mut frontiers = Vec::new();
        let mut storage_proposals = BTreeMap::new();
        if let Some(metadata) = metadata {
            for (id, upper) in &metadata.uppers {
                let Some(policy) = catalog.state().collection_read_policy(*id) else {
                    continue;
                };
                let Some(since) = catalog.state().collection_compaction_bounds().get(id) else {
                    continue;
                };
                let proposed = policy.frontier(upper.borrow());
                storage_proposals.insert(*id, proposed.clone());
                frontiers.push(CollectionFrontiers {
                    id: *id,
                    write_frontier: upper.clone(),
                    implied_capability: proposed,
                    read_capabilities: since.clone(),
                });
            }
        }
        let candidates = publication_candidates(
            catalog.state().maintained_read_requirements(),
            catalog.state().collection_compaction_bounds(),
            &frontiers,
            &storage_proposals,
            &compute_proposals,
            |id| {
                catalog
                    .try_get_entry_by_global_id(&id)
                    .is_some_and(|entry| match entry.item() {
                        CatalogItem::MaterializedView(mv) => {
                            mv.replacement_target.is_none() && mv.global_id_writes() == id
                        }
                        CatalogItem::Table(_) | CatalogItem::Source(_) | CatalogItem::Sink(_) => {
                            true
                        }
                        _ => false,
                    })
            },
            |id, excluding| {
                catalog
                    .state()
                    .maintained_read_frontier(id, excluding)
                    .into_iter()
                    .chain(catalog.state().client_read_frontier(id))
                    .min()
            },
        );
        let mut ops = Vec::new();
        if !candidates.bounds.is_empty() || !candidates.requirements.is_empty() {
            ops.push(Op::SetReadProtection {
                requirements: candidates.requirements,
                bounds: candidates.bounds,
            });
        }
        let expired = self.reclaimer.observe(
            catalog
                .state()
                .client_incarnations()
                .iter()
                .map(|(id, value)| (*id, value.heartbeat)),
            Instant::now(),
        );
        ops.extend(
            expired
                .into_iter()
                .filter(|(id, _)| *id != incarnation)
                .map(
                    |(incarnation, expected_heartbeat)| Op::ReclaimClientIncarnation {
                        incarnation,
                        expected_heartbeat,
                    },
                ),
        );
        if !ops.is_empty() {
            self.transact(catalog, effects, cluster, build, ops).await?;
        }
        Ok(())
    }
}
