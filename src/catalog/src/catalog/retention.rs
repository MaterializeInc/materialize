// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Index admission and retained-plan protection at the catalog compaction boundary.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use futures::{StreamExt, TryStreamExt, stream};
use mz_adapter_types::compaction::{CompactionWindow, SINCE_GRANULARITY};
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::controller::StorageTxn;
use mz_storage_types::StorageDiff;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::SourceData;
use timely::PartialOrder;
use timely::progress::Antichain;

use super::{CatalogError, CatalogState};
use crate::durable::Transaction;
use crate::memory::objects::{CatalogItem, DataSourceDesc, TableDataSource};

struct IndexRetention {
    id: GlobalId,
    shards: BTreeSet<ShardId>,
    policy: ReadPolicy,
    floor: Antichain<Timestamp>,
}

impl CatalogState {
    /// Proposes index retention from observed input progress, including indexes
    /// with no replicas. Partial observations are only proposals: the catalog
    /// transaction checks complete durable progress and all read requirements.
    pub fn index_retention_proposals(
        &self,
        frontiers: &[mz_storage_client::storage_collections::CollectionFrontiers],
    ) -> BTreeMap<GlobalId, Antichain<Timestamp>> {
        let observed: BTreeMap<_, _> = frontiers
            .iter()
            .map(|frontier| (frontier.id, &frontier.write_frontier))
            .collect();
        affected_indexes(self, &observed.keys().copied().collect())
            .into_iter()
            .filter_map(|id| {
                let old = self.collection_compaction_bounds().get(&id)?;
                let entry = self.get_entry_by_global_id(&id);
                let CatalogItem::Index(index) = entry.item() else {
                    unreachable!("selected an index");
                };
                let inputs = self.logical_collection_inputs([index.on]);
                if inputs.iter().any(|input| {
                    matches!(
                        self.get_entry_by_global_id(input).item(),
                        CatalogItem::Log(_)
                    )
                }) {
                    return None;
                }
                let upper: Antichain<_> = inputs
                    .iter()
                    .filter_map(|input| observed.get(input))
                    .flat_map(|upper| upper.iter().copied())
                    .collect();
                let proposal = self.index_read_policy(id)?.frontier(upper.borrow());
                PartialOrder::less_than(old, &proposal).then_some((id, proposal))
            })
            .collect()
    }

    /// Returns the index's effective retention policy, including the metrics override.
    /// Object retention and replica execution windows use the same policy.
    pub fn index_read_policy(&self, id: GlobalId) -> Option<ReadPolicy> {
        if !matches!(
            self.try_get_entry_by_global_id(&id)?.item(),
            CatalogItem::Index(_)
        ) {
            return None;
        }
        self.collection_read_policy(id)
    }

    /// Effective collection retention, including metrics and ingestion inheritance.
    pub fn collection_read_policy(&self, id: GlobalId) -> Option<ReadPolicy> {
        let item = self.try_get_entry_by_global_id(&id)?.item();
        Some(if item.is_retained_metrics_object() {
            let duration = self.system_config().metrics_retention();
            ReadPolicy::lag_writes_by(
                Timestamp::new(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
                SINCE_GRANULARITY,
            )
        } else {
            let parent = match item {
                CatalogItem::Source(source) => match &source.data_source {
                    DataSourceDesc::IngestionExport { ingestion_id, .. } => Some(ingestion_id),
                    _ => None,
                },
                CatalogItem::Table(table) => match &table.data_source {
                    TableDataSource::DataSource {
                        desc: DataSourceDesc::IngestionExport { ingestion_id, .. },
                        ..
                    } => Some(ingestion_id),
                    _ => None,
                },
                _ => None,
            };
            let window = item
                .custom_logical_compaction_window()
                .or_else(|| {
                    parent
                        .and_then(|id| self.get_entry(id).item().custom_logical_compaction_window())
                })
                .or_else(|| item.initial_logical_compaction_window())
                .or_else(|| {
                    matches!(item, CatalogItem::Sink(_)).then_some(CompactionWindow::Default)
                })?;
            window.into()
        })
    }
}

/// Establishes the first readable frontier when an index's plan is selected.
/// All changes are staged in the same transaction as the selection. Resolving
/// imports first also covers batches whose index IDs are not topologically ordered.
pub(super) fn admit_index_bounds(
    tx: &mut Transaction<'_>,
    state: &CatalogState,
    selected: &BTreeSet<GlobalId>,
) -> Result<(), CatalogError> {
    if !state.catalog_read_protection_enabled() {
        return Ok(());
    }

    let mut pending = BTreeMap::new();
    for id in selected {
        let Some(entry) = state.try_get_entry_by_global_id(id) else {
            continue;
        };
        let CatalogItem::Index(index) = entry.item() else {
            continue;
        };
        if tx.proposed_compaction_bound(*id).is_some() {
            continue;
        }
        let mut inputs = state.logical_collection_inputs([index.on]);
        for (_, plan) in state.written_plans_for_owner(*id) {
            inputs.extend(plan.imports.iter().copied());
        }
        inputs.retain(|input| match state.try_get_entry_by_global_id(input) {
            Some(entry) => match entry.item() {
                CatalogItem::Log(_) => false,
                CatalogItem::Index(index) => {
                    selected.contains(input)
                        || tx.proposed_compaction_bound(*input).is_some()
                        || !state
                            .logical_collection_inputs([index.on])
                            .iter()
                            .any(|id| {
                                matches!(
                                    state.get_entry_by_global_id(id).item(),
                                    CatalogItem::Log(_)
                                )
                            })
                }
                _ => true,
            },
            // A foreign selection may await repair after an import is retired.
            // New selections are checked against the complete candidate first.
            None => false,
        });
        pending.insert(*id, inputs);
    }
    while !pending.is_empty() {
        let ready = pending.iter().find_map(|(id, inputs)| {
            inputs
                .iter()
                .all(|input| tx.proposed_compaction_bound(*input).is_some())
                .then_some(*id)
        });
        let Some(id) = ready else {
            // An input lacks permission or the selections contain a cycle.
            return Err(CatalogError::DDLTransactionRace);
        };
        let inputs = pending.remove(&id).expect("ready index exists");
        // Constants and replica-local logs initialize at MIN. Every persisted
        // or indexed input contributes its own admitted lower bound.
        let mut floor = Antichain::from_elem(Timestamp::MIN);
        for input in inputs {
            floor.join_assign(
                &tx.proposed_compaction_bound(input)
                    .expect("checked input permission"),
            );
        }
        if floor.is_empty() {
            return Err(CatalogError::internal(
                "index admission",
                format!("index {id} has no readable input frontier"),
            ));
        }
        tx.set_collection_compaction_bound(id, floor.as_option().copied())?;
    }
    Ok(())
}

/// Derives advancing index proposals from final definitions and durable input
/// progress. `constrain_plan_inputs` couples these to their dependencies and
/// client holds before the transaction can authorize compaction.
pub(super) async fn constrain_index_retention(
    persist: &PersistClient,
    tx: &mut Transaction<'_>,
    base: &CatalogState,
    candidate: &CatalogState,
    changed: &BTreeSet<GlobalId>,
) -> Result<(), CatalogError> {
    if !candidate.catalog_read_protection_enabled() {
        return Ok(());
    }
    let advancing: BTreeSet<_> = changed
        .iter()
        .copied()
        .filter(|id| {
            let Some(proposed) = tx.proposed_compaction_bound(*id) else {
                return false;
            };
            match base.collection_compaction_bounds().get(id) {
                Some(old) => !PartialOrder::less_equal(&proposed, old),
                // Initial admission is not an advance. A subsequent proposal in
                // the same batch must still respect the birth frontier.
                None => tx
                    .initial_compaction_bound(*id)
                    .is_some_and(|birth| !PartialOrder::less_equal(&proposed, &birth)),
            }
        })
        .collect();
    let indexes = affected_indexes(candidate, &advancing);
    let mut requirements = Vec::new();
    for id in indexes {
        let entry = candidate.get_entry_by_global_id(&id);
        let CatalogItem::Index(index) = entry.item() else {
            unreachable!("selected an index");
        };
        let inputs = candidate.logical_collection_inputs([index.on]);
        if inputs.iter().any(|input| {
            matches!(
                candidate.get_entry_by_global_id(input).item(),
                CatalogItem::Log(_)
            )
        }) {
            // A log-dependent index has replica-local history. Persisted catalog
            // tables are not logs and retain the normal object-owned protection.
            continue;
        }
        let policy = candidate.index_read_policy(id).expect("selected an index");
        let mut floor = Antichain::from_elem(Timestamp::MIN);
        let mut shards = BTreeSet::new();
        for input in &inputs {
            // Candidate bounds cannot justify their own advancement. A new
            // collection's birth boundary is its initial visibility boundary.
            let since = base
                .collection_compaction_bounds()
                .get(input)
                .cloned()
                .or_else(|| tx.initial_compaction_bound(*input))
                .filter(|since| !since.is_empty())
                .ok_or_else(|| {
                    CatalogError::internal(
                        "index retention",
                        format!("input {input} of index {id} has no readable permission"),
                    )
                })?;
            floor.join_assign(&since);
            let transactional = matches!(
                candidate.get_entry_by_global_id(input).item(),
                CatalogItem::Table(table)
                    if matches!(table.data_source, TableDataSource::TableWrites { .. })
            );
            let shard = if transactional {
                tx.get_txn_wal_shard()
            } else {
                tx.retention_input_shard(*input)
            };
            let shard = shard.ok_or_else(|| {
                CatalogError::internal(
                    "index retention",
                    format!("input {input} of index {id} has no durable progress shard"),
                )
            })?;
            shards.insert(shard);
        }
        requirements.push(IndexRetention {
            id,
            shards,
            policy,
            floor,
        });
    }
    let shards: BTreeSet<_> = requirements
        .iter()
        .flat_map(|r| r.shards.iter().copied())
        .collect();
    // Bound metadata fanout without serializing a round trip for every input.
    // Failed observations abort publication, retaining permission and allowing
    // the publisher to retry the complete candidate against a fresh prefix.
    let uppers: BTreeMap<_, _> = stream::iter(shards)
        .map(|shard| async move {
            let upper = persist
                .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                    shard,
                    Diagnostics {
                        shard_name: shard.to_string(),
                        handle_purpose: "object retention".into(),
                    },
                )
                .await
                .map_err(|error| CatalogError::Unstructured(error.into()))?;
            Ok::<_, CatalogError>((shard, upper))
        })
        .buffer_unordered(32)
        .try_collect()
        .await?;
    for IndexRetention {
        id,
        shards,
        policy,
        floor,
    } in requirements
    {
        let upper: Antichain<_> = shards
            .iter()
            .flat_map(|shard| uppers[shard].iter().copied())
            .collect();
        // Constants have no leaf requirements. Their own empty-upper policy
        // remains authoritative, without inventing a ticking progress source.
        let mut limit = policy.frontier(upper.borrow());
        limit.join_assign(&floor);
        let Some(current) = tx.proposed_compaction_bound(id) else {
            // Definitions without a selected plan are not admitted collections.
            continue;
        };
        let mut bound = limit;
        if let Some(old) = base.collection_compaction_bounds().get(&id) {
            if &current != old {
                bound.meet_assign(&current);
            }
            // A stronger policy cannot restore already-discarded history.
            bound.join_assign(old);
        } else {
            bound.join_assign(&current);
        }
        if bound != current {
            tx.set_collection_compaction_bound(id, bound.into_option())?;
        }
    }
    Ok(())
}

/// Couples proposed permission to the history needed by logical recovery and
/// every selected build's actual imports. Dependencies are metadata, not another
/// durable copy of an index's advancing frontier.
pub(super) fn constrain_plan_inputs(
    tx: &mut Transaction<'_>,
    base: &CatalogState,
    state: &CatalogState,
    selected: &BTreeSet<GlobalId>,
) -> Result<(), CatalogError> {
    if !state.catalog_read_protection_enabled() {
        return Ok(());
    }
    let changed: BTreeSet<_> = tx
        .changed_compaction_bounds()
        .chain(tx.changed_maintained_read_requirements())
        .chain(selected.iter().copied())
        .collect();
    let mut owners = affected_indexes(state, &changed);
    owners.extend(selected.iter().copied());
    owners.extend(tx.changed_maintained_read_requirements());
    for input in &changed {
        owners.extend(state.written_plan_importers(*input).map(|(owner, _)| owner));
    }
    let mut bounds: BTreeMap<_, _> = tx
        .changed_compaction_bounds()
        .filter_map(|id| tx.proposed_compaction_bound(id).map(|bound| (id, bound)))
        .collect();
    let mut requirements = BTreeMap::new();
    for owner in owners {
        let Some(entry) = state.try_get_entry_by_global_id(&owner) else {
            continue;
        };
        let mut inputs = match entry.item() {
            CatalogItem::Index(index) => {
                let Some(mut bound) = tx.proposed_compaction_bound(owner) else {
                    continue;
                };
                bound.extend(state.client_read_frontier(owner));
                bounds.insert(owner, bound);
                state.logical_collection_inputs([index.on])
            }
            CatalogItem::MaterializedView(_) => {
                let Some(requirement) = state.maintained_read_requirements().get(&owner) else {
                    return Err(CatalogError::DDLTransactionRace);
                };
                if requirement.frontier.is_none() {
                    continue;
                }
                // Logical storage inputs are governed by the maintained
                // requirement validator. Do not clip away an admission failure
                // when a new requirement asks for already-disallowed history.
                BTreeSet::new()
            }
            _ => continue,
        };
        for (_, plan) in state.written_plans_for_owner(owner) {
            inputs.extend(plan.imports.iter().copied().filter(|input| {
                !matches!(entry.item(), CatalogItem::MaterializedView(_))
                    || !state.maintained_read_requirements()[&owner]
                        .inputs
                        .contains(input)
            }));
        }
        let mut governed = BTreeSet::new();
        for input in inputs {
            let Some(entry) = state.try_get_entry_by_global_id(&input) else {
                // A foreign build repairs its invalidated selection independently.
                // A retired import has no lifetime whose compaction can be governed.
                continue;
            };
            if matches!(entry.item(), CatalogItem::Log(_)) {
                continue;
            }
            if let CatalogItem::Index(index) = entry.item()
                && tx.proposed_compaction_bound(input).is_none()
                && state
                    .logical_collection_inputs([index.on])
                    .iter()
                    .any(|id| {
                        matches!(state.get_entry_by_global_id(id).item(), CatalogItem::Log(_))
                    })
            {
                continue;
            }
            let bound = tx
                .proposed_compaction_bound(input)
                .ok_or(CatalogError::DDLTransactionRace)?;
            bounds.entry(input).or_insert(bound);
            governed.insert(input);
        }
        requirements.insert(owner, governed);
    }
    // Reducing an importer's proposal can constrain its imports in turn. The
    // iteration only meets a finite set of proposed and required frontiers.
    loop {
        let mut changed = false;
        for (owner, inputs) in &requirements {
            let frontier = match state.get_entry_by_global_id(owner).item() {
                CatalogItem::Index(_) => bounds[owner].clone(),
                _ => state.maintained_read_requirements()[owner]
                    .frontier
                    .into_iter()
                    .collect(),
            };
            for input in inputs {
                let bound = bounds.get_mut(input).expect("governed input has a bound");
                if !PartialOrder::less_equal(bound, &frontier) {
                    bound.meet_assign(&frontier);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    for (id, bound) in bounds {
        let floor = base
            .collection_compaction_bounds()
            .get(&id)
            .cloned()
            .or_else(|| tx.initial_compaction_bound(id));
        if floor.is_some_and(|floor| !PartialOrder::less_equal(&floor, &bound)) {
            // Selecting an import cannot recover history it was already allowed
            // to discard. The planner must choose another access path.
            return Err(CatalogError::DDLTransactionRace);
        }
        if tx.proposed_compaction_bound(id).as_ref() != Some(&bound) {
            tx.set_collection_compaction_bound(id, bound.into_option())?;
        }
    }
    Ok(())
}

/// Visits only dependents of changed permissions. Both reference and use edges
/// matter: name resolution retains optimized-away inputs, while raw expressions
/// can introduce dependencies. Persisted collections stop logical expansion.
fn affected_indexes(state: &CatalogState, changed: &BTreeSet<GlobalId>) -> BTreeSet<GlobalId> {
    let mut pending = Vec::new();
    let mut indexes = BTreeSet::new();
    for id in changed {
        let Some(entry) = state.try_get_entry_by_global_id(id) else {
            continue;
        };
        if matches!(entry.item(), CatalogItem::Index(_)) {
            indexes.insert(*id);
        } else {
            pending.extend(entry.referenced_by().iter().copied());
            pending.extend(entry.used_by().iter().copied());
        }
    }
    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let entry = state.get_entry(&id);
        match entry.item() {
            CatalogItem::Index(index) => {
                indexes.insert(index.global_id);
            }
            CatalogItem::View(_) => {
                pending.extend(entry.referenced_by().iter().copied());
                pending.extend(entry.used_by().iter().copied());
            }
            _ => (),
        }
    }
    indexes
}

#[cfg(test)]
mod tests;
