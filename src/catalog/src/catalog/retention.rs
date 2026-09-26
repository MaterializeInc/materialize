// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Object-owned index retention at the catalog compaction boundary.

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
    inputs: BTreeSet<GlobalId>,
    shards: BTreeSet<ShardId>,
    policy: ReadPolicy,
    floor: Antichain<Timestamp>,
}

impl CatalogState {
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

/// Clips advancing proposals against the final definitions and durable input
/// progress. The transaction's compare-and-append must still match `base`.
/// Incarnation requirements are additional constraints, validated separately.
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
                // First index publication records installed readability, not an
                // advance. Storage birth followed by advancement in this batch
                // must still respect the original visibility boundary.
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
            inputs,
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
        inputs,
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
        for target in inputs.iter().copied().chain([id]) {
            if !advancing.contains(&target) {
                continue;
            }
            let Some(mut bound) = tx.proposed_compaction_bound(target) else {
                continue;
            };
            bound.extend(limit.iter().copied());
            // Neither a stronger policy nor terminal input progress can restore
            // discarded history. Existing permission remains monotone.
            if let Some(old) = base.collection_compaction_bounds().get(&target) {
                bound.join_assign(old);
            }
            tx.set_collection_compaction_bound(target, bound.into_option())?;
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
