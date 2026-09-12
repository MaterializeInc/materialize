// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Publishes durable recovery requirements and compaction permission together.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use differential_dataflow::lattice::Lattice;
use mz_catalog::durable::objects::{CollectionCompactionBound, MaintainedReadRequirement};
use mz_catalog::memory::objects::CatalogItem;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::storage_collections::CollectionFrontiers;
use timely::progress::Antichain;

use crate::AdapterError;
use crate::catalog::Op;
use crate::coord::Coordinator;

pub(super) const CATALOG_SUBSCRIPTION_INTERVAL: Duration = Duration::from_secs(1);

impl Coordinator {
    fn client_read_catalog(&self) -> &crate::catalog::Catalog {
        self.client_protection_catalog
            .as_ref()
            .unwrap_or_else(|| self.catalog())
    }

    /// Client metadata is live even when SQL uses a prewarming savepoint. This
    /// writer validates its own projection but does not enact maintained objects.
    async fn transact_client_protection(&mut self, op: Op) -> Result<Vec<u64>, AdapterError> {
        assert!(matches!(
            &op,
            Op::CreateClientIncarnation | Op::PublishClientReadRequirements { .. }
        ));
        let Some(catalog) = &mut self.client_protection_catalog else {
            return self
                .catalog_transact_with_results(None, None, vec![op])
                .await;
        };
        loop {
            if let Err(error) = catalog.sync_to_current_updates().await {
                if matches!(
                    &error,
                    mz_catalog::durable::CatalogError::Durable(
                        mz_catalog::durable::DurableCatalogError::Fence(_)
                    )
                ) {
                    // This writer cannot renew protection. Its cached projection
                    // may still contain the incarnation, so presence is not evidence
                    // that the client can continue issuing protected grants.
                    if let Some(client) = &self.query_client {
                        client.protection.mark_closed();
                    }
                }
                return Err(error.into());
            }
            let ts = catalog.current_upper().await;
            match catalog
                .transact(
                    Some(&mut self.controller.storage_collections),
                    ts,
                    None,
                    vec![op.clone()],
                )
                .await
            {
                Ok(result) => return Ok(result.created_client_incarnations),
                Err(AdapterError::Catalog(error))
                    if matches!(
                        &error.kind,
                        mz_catalog::memory::error::ErrorKind::Durable(
                            mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
                        )
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) async fn initialize_query_client(&mut self) -> Result<(), AdapterError> {
        if !self.catalog().state().catalog_read_protection_enabled() {
            return Ok(());
        }
        use crate::peek_client::CoordinatorClient;
        use crate::query_client::QueryClient;
        use crate::query_client::connections::{
            QueryReplicaConnections, QueryReplicaConnectionsConfig,
        };
        use mz_ore::collections::CollectionExt;

        let incarnation = self
            .transact_client_protection(Op::CreateClientIncarnation)
            .await?
            .into_element();
        let txns_shard = self.catalog().txn_wal_shard().await?;
        let connections = std::sync::Arc::new(QueryReplicaConnections::new(
            QueryReplicaConnectionsConfig {
                orchestrator: std::sync::Arc::clone(&self.query_orchestrator),
                deploy_generation: self.query_deploy_generation,
                build_info: self.catalog().config().build_info,
            },
        ));
        connections.sync_catalog(self.catalog());
        let client = std::sync::Arc::new(QueryClient::new(
            incarnation,
            CoordinatorClient::Background {
                tx: self.internal_cmd_tx.clone(),
                metrics: self.metrics.clone(),
            },
            self.persist_client.clone(),
            self.query_persist_location.clone(),
            txns_shard,
            connections,
        ));
        self.query_client = Some(client);

        // Keep bootstrap constraints until the client has secured its readable
        // windows. Missing replicas must not block startup or turn installation
        // into a catalog-write handshake. Their index windows remain pending.
        let bootstrap_holds: Vec<_> = self
            .global_timelines
            .values_mut()
            .map(|state| {
                state
                    .pending_read_holds
                    .extend(&state.read_holds.id_bundle());
                std::mem::take(&mut state.read_holds)
            })
            .collect();
        self.acquire_pending_query_timeline_holds().await?;
        drop(bootstrap_holds);
        Ok(())
    }

    /// Establishes client-owned oracle windows for installed, readable collections.
    /// Called outside installation, and retried by ordinary timeline maintenance.
    /// Unknown index frontiers remain pending without delaying healthy clusters.
    pub(super) async fn acquire_pending_query_timeline_holds(
        &mut self,
    ) -> Result<(), AdapterError> {
        let Some(client) = self.query_client.clone() else {
            return Ok(());
        };
        if client.protection.publication_pending() {
            return Ok(());
        }
        fn retain_live(catalog: &crate::catalog::Catalog, ids: &mut crate::CollectionIdBundle) {
            ids.storage_ids
                .retain(|id| catalog.try_get_entry_by_global_id(id).is_some());
            for ids in ids.compute_ids.values_mut() {
                ids.retain(|id| catalog.try_get_entry_by_global_id(id).is_some());
            }
        }
        // In-flight work is outside TimelineState. A publication can consume
        // catalog drops, so membership must be rechecked before returning it.
        let pending: Vec<_> = self
            .global_timelines
            .iter_mut()
            .filter(|(_, state)| !state.pending_read_holds.is_empty())
            .map(|(timeline, state)| {
                (
                    timeline.clone(),
                    std::mem::take(&mut state.pending_read_holds),
                    std::sync::Arc::clone(&state.oracle),
                )
            })
            .collect();
        let mut first_error = None;
        for (timeline, mut ids, oracle) in pending {
            retain_live(self.client_read_catalog(), &mut ids);
            let Some(state) = self.global_timelines.get(&timeline) else {
                continue;
            };
            ids = ids.difference(&state.read_holds.id_bundle());
            let mut ready = ids.clone();
            for (cluster, ids) in &mut ready.compute_ids {
                *ids = client.readable_indexes(*cluster, ids);
            }
            if !ready.is_empty() {
                match self
                    .acquire_client_read_protection(client.protection.incarnation(), ready.clone())
                    .await
                {
                    Ok((holds, _)) => {
                        // Publication can consume a peer drop. In-flight IDs were
                        // not in timeline state for its cleanup, so recheck them
                        // before installing a window or restoring pending work.
                        retain_live(self.client_read_catalog(), &mut ready);
                        // Index tokens keep their derived leaf protection. Do not
                        // insert a second direct leaf token already in the window.
                        let mut holds = holds.subset(&ready);
                        holds.downgrade(oracle.read_ts().await);
                        if let Some(state) = self.global_timelines.get_mut(&timeline) {
                            let missing = ready.difference(&state.read_holds.id_bundle());
                            state.read_holds.extend(holds.subset(&missing));
                        }
                        ids = ids.difference(&ready);
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            retain_live(self.client_read_catalog(), &mut ids);
            if let Some(state) = self.global_timelines.get_mut(&timeline) {
                state.pending_read_holds.extend(&ids);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) async fn acquire_client_read_protection(
        &mut self,
        incarnation: u64,
        bundle: crate::CollectionIdBundle,
    ) -> Result<(crate::ReadHolds, Antichain<Timestamp>), AdapterError> {
        let client = self.query_client.clone().ok_or(AdapterError::ReadOnly)?;
        if client.protection.incarnation() != incarnation {
            return Err(AdapterError::internal(
                "query read protection",
                "incarnation is no longer active",
            ));
        }
        if !self
            .client_read_catalog()
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
        {
            client.protection.mark_closed();
            return Err(AdapterError::internal(
                "query read protection",
                "incarnation is closed",
            ));
        }
        let prepared = client
            .prepare_read(self.client_read_catalog(), &bundle)
            .await?;
        if let Some(holds) = client
            .protection
            .try_acquire(
                &prepared.bundle,
                &prepared.frontiers,
                &prepared.index_inputs,
            )
            .map_err(|error| AdapterError::Unstructured(error.into()))?
        {
            return Ok((holds, prepared.upper));
        }
        let extra = self
            .client_read_catalog()
            .state()
            .expand_client_read_requirements(incarnation, prepared.frontiers.clone())?;
        let requirements = client.protection.prepare_publication(extra);
        let result = self
            .transact_client_protection(Op::PublishClientReadRequirements {
                incarnation,
                requirements,
            })
            .await;
        // Catalog transaction errors are definitive. Indeterminate commit errors
        // terminate before this point rather than releasing a publication barrier.
        client.protection.finish_publication(result.is_ok());
        if !self
            .client_read_catalog()
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
        {
            client.protection.mark_closed();
        }
        result?;
        client.published();
        let holds = client
            .protection
            .try_acquire(
                &prepared.bundle,
                &prepared.frontiers,
                &prepared.index_inputs,
            )
            .map_err(|error| AdapterError::Unstructured(error.into()))?
            .ok_or_else(|| {
                AdapterError::internal("query read protection", "published scope was not acquired")
            })?;
        Ok((holds, prepared.upper))
    }

    /// Publishes the client aggregate and heartbeat through the same transaction path.
    pub(super) async fn publish_client_read_protection(&mut self) -> Result<(), AdapterError> {
        use crate::query_client::read_protection::CLIENT_PROTECTION_PUBLICATION_INTERVAL;
        let Some(client) = self.query_client.clone() else {
            return Ok(());
        };
        if client.last_publication().elapsed() < CLIENT_PROTECTION_PUBLICATION_INTERVAL {
            return Ok(());
        }
        let incarnation = client.protection.incarnation();
        let requirements = client.protection.prepare_publication(BTreeMap::new());
        let result = self
            .transact_client_protection(Op::PublishClientReadRequirements {
                incarnation,
                requirements,
            })
            .await;
        client.protection.finish_publication(result.is_ok());
        if !self
            .client_read_catalog()
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
        {
            client.protection.mark_closed();
        }
        result?;
        client.published();
        Ok(())
    }

    pub(super) async fn reclaim_client_read_protection(&mut self) -> Result<(), AdapterError> {
        if self.controller.read_only() {
            return Ok(());
        }
        let active: Vec<_> = self
            .catalog()
            .state()
            .client_incarnations()
            .iter()
            .map(|(&id, &heartbeat)| (id, heartbeat))
            .collect();
        let expired = self
            .client_protection_reclaimer
            .observe(active, Instant::now());
        if !expired.is_empty() {
            self.catalog_transact_with_context(
                None,
                None,
                expired
                    .into_iter()
                    .map(
                        |(incarnation, expected_heartbeat)| Op::ReclaimClientIncarnation {
                            incarnation,
                            expected_heartbeat,
                        },
                    )
                    .collect(),
            )
            .await?;
        }
        Ok(())
    }

    /// Restores published compute bounds before installing clusters and dataflows.
    pub(super) async fn restore_compute_read_protection(&mut self) -> Result<(), AdapterError> {
        if !self.catalog().state().catalog_read_protection_enabled() {
            return Ok(());
        }
        let state = self.catalog().state();
        let storage = &state.storage_metadata().collection_metadata;
        let bounds = state
            .collection_compaction_bounds()
            .iter()
            .filter(|(id, _)| !storage.contains_key(id))
            .map(|(&id, bound)| (id, bound.clone()))
            .collect::<Vec<_>>();
        for (id, bound) in bounds {
            self.controller
                .compute
                .apply_compaction_bound(id, bound)
                .map_err(|error| AdapterError::Unstructured(error.into()))?;
        }
        self.sync_compute_read_protection().await
    }

    /// Delivers changed published bounds to compute lifetimes in the local catalog.
    pub(super) async fn sync_compute_read_protection(&mut self) -> Result<(), AdapterError> {
        let Some(subscriber) = &mut self.compaction_bound_subscriber else {
            return Ok(());
        };
        let changed = subscriber.sync().await?;
        for (id, bound) in changed {
            if !self
                .catalog()
                .try_get_entry_by_global_id(&id)
                .is_some_and(|entry| matches!(entry.item(), CatalogItem::Index(_)))
            {
                continue;
            }
            self.controller
                .compute
                .apply_compaction_bound(id, bound)
                .map_err(|error| AdapterError::Unstructured(error.into()))?;
        }
        Ok(())
    }

    /// Publishes recovery progress and compatible bounds in enabled, writable environments.
    pub(super) async fn publish_read_protection(&mut self) -> Result<(), AdapterError> {
        if self.controller.read_only() || !self.catalog().state().catalog_read_protection_enabled()
        {
            return Ok(());
        }

        let start = Instant::now();
        let catalog_changes = self.catalog_mut().take_read_protection_changes();
        self.read_protection_pending.extend(catalog_changes);
        let mut storage = self
            .controller
            .storage_collections
            .take_read_protection_frontiers(&self.read_protection_pending);
        let inputs = storage
            .keys()
            .filter_map(|id| {
                self.catalog()
                    .state()
                    .maintained_read_requirements()
                    .get(id)
            })
            .flat_map(|requirement| requirement.inputs.iter().copied())
            .collect();
        storage.extend(
            self.controller
                .storage_collections
                .take_read_protection_frontiers(&inputs),
        );
        let frontiers = storage
            .values()
            .map(|(frontiers, _)| frontiers.clone())
            .collect::<Vec<_>>();
        let compaction_frontiers = storage
            .iter()
            .map(|(&id, (_, frontier))| (id, frontier.clone()))
            .collect();
        let compute_changes = self
            .controller
            .compute
            .take_compaction_bound_proposals(&self.read_protection_pending);
        self.read_protection_pending.extend(storage.keys().copied());
        self.read_protection_pending
            .extend(compute_changes.keys().copied());
        let bounds = self.catalog().state().collection_compaction_bounds();
        let compute_proposals = compute_changes
            .into_iter()
            .filter_map(|(id, proposal)| {
                let entry = self.catalog().try_get_entry_by_global_id(&id)?;
                if !matches!(entry.item(), CatalogItem::Index(_)) {
                    return None;
                }
                let frontier = if bounds.contains_key(&id) {
                    proposal
                } else {
                    // First publication records installed readability.
                    self.controller
                        .compute
                        .collection_frontiers(id, None)
                        .expect("proposal belongs to installed index")
                        .read_frontier
                };
                Some((id, frontier))
            })
            .collect();
        let candidates = publication_candidates(
            self.catalog().state().maintained_read_requirements(),
            self.catalog().state().collection_compaction_bounds(),
            &frontiers,
            &compaction_frontiers,
            &compute_proposals,
            |id| {
                let Some(entry) = self.catalog().try_get_entry_by_global_id(&id) else {
                    return false;
                };
                match entry.item() {
                    // A pending replacement observes the target's shared upper, not its own
                    // progress. Retired writers' requirements are completed by replacement.
                    CatalogItem::MaterializedView(mv) => {
                        mv.replacement_target.is_none() && mv.global_id_writes() == id
                    }
                    CatalogItem::Source(_) | CatalogItem::Table(_) | CatalogItem::Sink(_) => true,
                    _ => false,
                }
            },
            |id, excluding| {
                self.catalog()
                    .state()
                    .maintained_read_frontier(id, excluding)
                    .into_iter()
                    .chain(self.catalog().state().client_read_frontier(id))
                    .min()
            },
        );
        let requirement_updates = candidates.requirements.len();
        let bound_updates = candidates.bounds.len();
        if requirement_updates == 0 && bound_updates == 0 {
            self.read_protection_pending.clear();
            return Ok(());
        }

        // Applying each record as a separate op would rescan accumulated pending
        // catalog updates for every record on the coordinator loop.
        let ops = vec![Op::SetReadProtection {
            requirements: candidates.requirements,
            bounds: candidates.bounds,
        }];
        self.catalog_transact(None, ops).await?;
        self.read_protection_pending.clear();

        tracing::info!(
            changed_storage_collections = frontiers.len(),
            requirement_updates,
            bound_updates,
            duration_seconds = start.elapsed().as_secs_f64(),
            max_policy_lag_ts = candidates.max_policy_lag_ts,
            unbounded_policy_lag = candidates.unbounded_policy_lag,
            "published catalog read protection"
        );
        Ok(())
    }
}

#[derive(Debug, Default)]
struct PublicationCandidates {
    requirements: Vec<MaintainedReadRequirement>,
    bounds: Vec<CollectionCompactionBound>,
    // Timestamp distance from the resulting bound to finite policy permission,
    // not wall-clock age. Empty policy frontiers with finite bounds count separately.
    max_policy_lag_ts: u64,
    unbounded_policy_lag: usize,
}

/// Computes changed records from committed protection and installed controller frontiers.
/// Compute proposals contain only live governed catalog indexes, using actual readability
/// for indexes without a published bound.
fn publication_candidates(
    requirements: &imbl::OrdMap<GlobalId, MaintainedReadRequirement>,
    bounds: &imbl::OrdMap<GlobalId, Antichain<Timestamp>>,
    frontiers: &[CollectionFrontiers],
    compaction_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
    compute_proposals: &BTreeMap<GlobalId, Antichain<Timestamp>>,
    owns_durable_progress: impl Fn(GlobalId) -> bool,
    committed_input_limit: impl Fn(GlobalId, &BTreeSet<GlobalId>) -> Option<Timestamp>,
) -> PublicationCandidates {
    let mut candidates = PublicationCandidates::default();
    let mut input_limits: BTreeMap<GlobalId, Timestamp> = BTreeMap::new();
    let mut advancing = BTreeSet::new();

    for output in frontiers {
        let Some(requirement) = requirements.get(&output.id) else {
            continue;
        };
        if !owns_durable_progress(requirement.id) {
            continue;
        }
        let mut frontier: Antichain<_> = requirement.frontier.into_iter().collect();
        // An upper at a refresh timestamp does not complete that refresh.
        // Its predecessor retains the pending input snapshot, including at MIN.
        let predecessor = output
            .write_frontier
            .iter()
            .map(|t| t.saturating_sub(1))
            .collect();
        frontier.join_assign(&predecessor);
        let frontier = frontier.into_option();
        if frontier == requirement.frontier {
            continue;
        }
        advancing.insert(requirement.id);
        candidates.requirements.push(MaintainedReadRequirement {
            frontier,
            ..requirement.clone()
        });
        if let Some(frontier) = frontier {
            for input in &requirement.inputs {
                input_limits
                    .entry(*input)
                    .and_modify(|limit| *limit = (*limit).min(frontier))
                    .or_insert(frontier);
            }
        }
    }

    let storage_proposals = frontiers.iter().filter_map(|collection| {
        compaction_frontiers.get(&collection.id).map(|proposal| {
            (
                collection.id,
                proposal,
                Some(&collection.implied_capability),
            )
        })
    });
    let compute_proposals = compute_proposals
        .iter()
        .map(|(&id, proposal)| (id, proposal, None));
    for (id, proposal, policy) in storage_proposals.chain(compute_proposals) {
        let old_bound = bounds.get(&id);
        if old_bound.is_none() && policy.is_some() {
            continue;
        }
        let mut bound = proposal.clone();
        // Controller proposals exclude only catalog permission. Early creation and
        // execution holds remain authoritative alongside durable input requirements.
        bound.extend(committed_input_limit(id, &advancing));
        bound.extend(input_limits.get(&id).copied());
        if let Some(old_bound) = old_bound {
            bound.join_assign(old_bound);
        }
        if let Some(bound_ts) = bound.as_option()
            && let Some(policy) = policy
        {
            match policy.as_option() {
                Some(policy_ts) => {
                    candidates.max_policy_lag_ts = candidates
                        .max_policy_lag_ts
                        .max(u64::from(policy_ts.saturating_sub(*bound_ts)));
                }
                None => candidates.unbounded_policy_lag += 1,
            }
        }
        if Some(&bound) != old_bound {
            candidates.bounds.push(CollectionCompactionBound {
                id,
                frontier: bound.into_option(),
            });
        }
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publication_candidates(
        requirements: &BTreeMap<GlobalId, MaintainedReadRequirement>,
        bounds: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        frontiers: &[CollectionFrontiers],
        compaction_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        compute_proposals: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        owns_durable_progress: impl Fn(GlobalId) -> bool,
    ) -> PublicationCandidates {
        super::publication_candidates(
            &requirements
                .iter()
                .map(|(&id, value)| (id, value.clone()))
                .collect(),
            &bounds
                .iter()
                .map(|(&id, value)| (id, value.clone()))
                .collect(),
            frontiers,
            compaction_frontiers,
            compute_proposals,
            owns_durable_progress,
            |input, excluding| {
                requirements
                    .values()
                    .filter(|requirement| {
                        requirement.inputs.contains(&input) && !excluding.contains(&requirement.id)
                    })
                    .filter_map(|requirement| requirement.frontier)
                    .min()
            },
        )
    }

    fn frontier(t: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(Timestamp::from(t))
    }

    fn policy_frontiers(
        frontiers: &[CollectionFrontiers],
    ) -> BTreeMap<GlobalId, Antichain<Timestamp>> {
        frontiers
            .iter()
            .map(|f| (f.id, f.implied_capability.clone()))
            .collect()
    }

    fn collection(id: GlobalId, upper: Option<u64>, policy: Option<u64>) -> CollectionFrontiers {
        CollectionFrontiers {
            id,
            write_frontier: upper.map(Timestamp::from).into_iter().collect(),
            implied_capability: policy.map(Timestamp::from).into_iter().collect(),
            read_capabilities: frontier(0),
        }
    }

    fn requirement(id: GlobalId, inputs: &[GlobalId], t: u64) -> MaintainedReadRequirement {
        MaintainedReadRequirement {
            id,
            inputs: inputs.iter().copied().collect(),
            frontier: Some(Timestamp::from(t)),
        }
    }

    #[mz_ore::test]
    fn durable_progress_preserves_pending_refresh_and_completion() {
        let input = GlobalId::User(1);
        let output = GlobalId::User(2);
        let requirement = requirement(output, &[input], 10);
        let requirements = BTreeMap::from([(output, requirement.clone())]);
        let bounds = BTreeMap::from([(input, frontier(10))]);

        for (upper, expected) in [
            (Some(0), Some(10)),
            (Some(10), Some(10)),
            (Some(11), Some(10)),
            (Some(12), Some(11)),
            (None, None),
        ] {
            let frontiers = [
                collection(input, None, None),
                collection(output, upper, Some(0)),
            ];
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &policy_frontiers(&frontiers),
                &BTreeMap::new(),
                |_| true,
            );
            let expected = expected.map(Timestamp::from);
            if expected == requirement.frontier {
                assert!(candidates.requirements.is_empty());
                assert!(candidates.bounds.is_empty());
            } else {
                assert_eq!(
                    candidates.requirements,
                    vec![MaintainedReadRequirement {
                        frontier: expected,
                        ..requirement.clone()
                    }]
                );
                assert_eq!(
                    candidates.bounds,
                    vec![CollectionCompactionBound {
                        id: input,
                        frontier: expected,
                    }]
                );
            }
        }

        let completed = BTreeMap::from([(
            output,
            MaintainedReadRequirement {
                frontier: None,
                ..requirement
            },
        )]);
        let candidates = publication_candidates(
            &completed,
            &bounds,
            &[collection(output, Some(20), Some(0))],
            &BTreeMap::new(),
            &BTreeMap::new(),
            |_| true,
        );
        assert!(candidates.requirements.is_empty(), "completion is final");
    }

    #[mz_ore::test]
    fn all_consumers_limit_exact_input_versions() {
        let input = GlobalId::User(1);
        let other_version = GlobalId::User(2);
        let writer = GlobalId::User(3);
        let pending = GlobalId::User(4);
        let uninstalled = GlobalId::User(5);
        let requirements = BTreeMap::from([
            (writer, requirement(writer, &[input], 10)),
            (pending, requirement(pending, &[input], 15)),
            (uninstalled, requirement(uninstalled, &[input], 18)),
        ]);
        let bounds = BTreeMap::from([(input, frontier(10)), (other_version, frontier(10))]);
        let frontiers = [
            collection(input, Some(100), Some(90)),
            collection(other_version, Some(100), Some(90)),
            collection(writer, Some(51), Some(0)),
            collection(pending, Some(100), Some(0)),
        ];
        for (requirements, limit) in [
            (requirements.clone(), 15),
            (
                requirements
                    .into_iter()
                    .filter(|(id, _)| *id != pending)
                    .collect(),
                18,
            ),
        ] {
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &policy_frontiers(&frontiers),
                &BTreeMap::new(),
                |id| id != pending,
            );
            assert_eq!(
                candidates.requirements,
                vec![requirement(writer, &[input], 50)]
            );
            assert_eq!(
                candidates.bounds,
                vec![
                    CollectionCompactionBound {
                        id: input,
                        frontier: Some(Timestamp::from(limit)),
                    },
                    CollectionCompactionBound {
                        id: other_version,
                        frontier: Some(Timestamp::from(90)),
                    },
                ]
            );
            assert_eq!(candidates.max_policy_lag_ts, 90 - limit);
        }
    }

    #[mz_ore::test]
    fn storage_consumers_protect_self_and_dependencies_until_durable_progress() {
        let remap = GlobalId::User(1);
        let source = GlobalId::User(2);
        let sink = GlobalId::User(3);
        let requirements = BTreeMap::from([
            (remap, requirement(remap, &[remap], 10)),
            (source, requirement(source, &[source, remap], 10)),
            (sink, requirement(sink, &[sink, source], 10)),
        ]);
        let bounds = BTreeMap::from([
            (remap, frontier(10)),
            (source, frontier(10)),
            (sink, frontier(10)),
        ]);
        for sink_installed in [false, true] {
            let mut frontiers = vec![
                collection(remap, Some(101), Some(200)),
                collection(source, Some(51), Some(200)),
            ];
            if sink_installed {
                frontiers.push(collection(sink, Some(21), Some(200)));
            }
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &policy_frontiers(&frontiers),
                &BTreeMap::new(),
                |_| true,
            );
            let mut expected = vec![
                requirement(remap, &[remap], 100),
                requirement(source, &[source, remap], 50),
            ];
            let mut expected_bounds = vec![CollectionCompactionBound {
                id: remap,
                frontier: Some(Timestamp::from(50)),
            }];
            if sink_installed {
                expected.push(requirement(sink, &[sink, source], 20));
                expected_bounds.extend([source, sink].map(|id| CollectionCompactionBound {
                    id,
                    frontier: Some(Timestamp::from(20)),
                }));
            }
            assert_eq!(candidates.requirements, expected);
            assert_eq!(candidates.bounds, expected_bounds);
        }
    }

    #[mz_ore::test]
    fn local_creation_holds_limit_permission() {
        let input = GlobalId::User(1);
        let frontiers = [collection(input, Some(100), Some(90))];
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &BTreeMap::from([(input, frontier(10))]),
            &frontiers,
            &BTreeMap::from([(input, frontier(20))]),
            &BTreeMap::new(),
            |_| false,
        );
        assert_eq!(
            candidates.bounds,
            vec![CollectionCompactionBound {
                id: input,
                frontier: Some(Timestamp::from(20)),
            }]
        );
    }

    #[mz_ore::test]
    fn compute_publication_preserves_existing_bounds() {
        let index = GlobalId::User(1);
        let uninstalled = GlobalId::User(3);
        for (old, proposal, expected) in [
            (Some(10), Some(20), Some(Some(20))),
            (Some(10), Some(10), None),
            (Some(10), Some(5), None),
            (Some(10), None, Some(None)),
            (None, Some(20), None),
        ] {
            let bounds = BTreeMap::from([
                (index, old.map(Timestamp::from).into_iter().collect()),
                (uninstalled, frontier(10)),
            ]);
            let proposals =
                BTreeMap::from([(index, proposal.map(Timestamp::from).into_iter().collect())]);
            let candidates = publication_candidates(
                &BTreeMap::new(),
                &bounds,
                &[],
                &BTreeMap::new(),
                &proposals,
                |_| false,
            );
            assert!(candidates.requirements.is_empty());
            assert_eq!(
                candidates.bounds,
                expected
                    .into_iter()
                    .map(|frontier| CollectionCompactionBound {
                        id: index,
                        frontier: frontier.map(Timestamp::from),
                    })
                    .collect::<Vec<_>>()
            );
        }
    }

    #[mz_ore::test]
    fn first_compute_publication_needs_no_birth_record() {
        let index = GlobalId::User(1);
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &BTreeMap::new(),
            &[],
            &BTreeMap::new(),
            &BTreeMap::from([(index, frontier(20))]),
            |_| false,
        );
        assert_eq!(
            candidates.bounds,
            vec![CollectionCompactionBound {
                id: index,
                frontier: Some(20.into()),
            }]
        );
    }

    #[mz_ore::test]
    fn mixed_publication_batches_progress_and_independently_held_bounds() {
        let input = GlobalId::User(1);
        let output = GlobalId::User(2);
        let index = GlobalId::User(3);
        let requirements = BTreeMap::from([(output, requirement(output, &[input], 10))]);
        let bounds = BTreeMap::from([(input, frontier(10)), (index, frontier(10))]);
        let frontiers = [
            collection(input, Some(100), Some(90)),
            collection(output, Some(51), Some(0)),
        ];
        // Proposals already include live holds. Releasing one controller's hold
        // must not release the other's or the logical storage input requirement.
        for (storage_proposal, compute_proposal, expected_storage) in
            [(20, 30, 20), (90, 30, 50), (20, 90, 20)]
        {
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &BTreeMap::from([(input, frontier(storage_proposal))]),
                &BTreeMap::from([(index, frontier(compute_proposal))]),
                |_| true,
            );
            assert_eq!(
                candidates.requirements,
                vec![requirement(output, &[input], 50)]
            );
            assert_eq!(
                candidates.bounds,
                vec![
                    CollectionCompactionBound {
                        id: input,
                        frontier: Some(Timestamp::from(expected_storage)),
                    },
                    CollectionCompactionBound {
                        id: index,
                        frontier: Some(Timestamp::from(compute_proposal)),
                    },
                ]
            );
        }
    }

    #[mz_ore::test]
    fn bounds_require_installation_and_governance_and_never_regress() {
        let advancing = GlobalId::User(1);
        let regressing_policy = GlobalId::User(2);
        let uninstalled = GlobalId::User(3);
        let ungoverned = GlobalId::User(4);
        let complete = GlobalId::User(5);
        let bounds = BTreeMap::from([
            (advancing, frontier(10)),
            (regressing_policy, frontier(10)),
            (uninstalled, frontier(10)),
            (complete, Antichain::new()),
        ]);
        let frontiers = [
            collection(advancing, Some(100), Some(90)),
            collection(regressing_policy, Some(100), Some(5)),
            collection(ungoverned, Some(100), Some(90)),
            collection(complete, None, Some(90)),
        ];
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &bounds,
            &frontiers,
            &policy_frontiers(&frontiers),
            &BTreeMap::new(),
            |_| false,
        );
        assert_eq!(
            candidates.bounds,
            vec![CollectionCompactionBound {
                id: advancing,
                frontier: Some(Timestamp::from(90)),
            }]
        );
        assert_eq!(candidates.max_policy_lag_ts, 0);

        let mut bounds = bounds;
        bounds.insert(advancing, frontier(90));
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &bounds,
            &frontiers,
            &policy_frontiers(&frontiers),
            &BTreeMap::new(),
            |_| false,
        );
        assert!(candidates.requirements.is_empty());
        assert!(
            candidates.bounds.is_empty(),
            "unchanged publication is a no-op"
        );
    }
}
