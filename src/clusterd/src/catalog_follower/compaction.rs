// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apply committed storage permission without owning storage lifecycle.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::controller::StorageMetadata;
use mz_storage_client::read_protection::{self, CriticalSinceHandle};
use timely::progress::Antichain;

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(super) struct Compaction {
    handles: BTreeMap<ShardId, (CriticalSinceHandle, Antichain<Timestamp>)>,
}

impl Compaction {
    /// Apply one complete committed snapshot in a catalog-protected environment.
    /// `wanted` selects shards, not the aliases participating in permission.
    /// Call periodically even without catalog changes to retry rate limiting and
    /// opaque contention. Returns the number of shards needing another attempt.
    ///
    /// Work is one metadata scan and at most one open and downgrade per relevant
    /// shard, with one Persist operation in flight. The caller owns timeouts and
    /// cancellation. No local read capability authorizes advancement. Missing
    /// mappings or bounds cannot authorize advancement. Dropping cached handles
    /// neither expires the shared critical reader nor finalizes its shard.
    pub async fn reconcile(
        &mut self,
        persist: &PersistClient,
        metadata: &StorageMetadata,
        wanted: &BTreeSet<GlobalId>,
    ) -> anyhow::Result<usize> {
        let bounds = shard_bounds(metadata, wanted);
        self.handles.retain(|shard, _| bounds.contains_key(shard));
        let mut pending = 0;
        for (shard, bound) in bounds {
            let Some(bound) = bound else {
                pending += 1;
                continue;
            };
            if !self.handles.contains_key(&shard) {
                let handle = read_protection::open_critical_handle(
                    persist,
                    shard,
                    Diagnostics {
                        shard_name: shard.to_string(),
                        handle_purpose: "catalog follower compaction".into(),
                    },
                )
                .await?;
                self.handles.insert(shard, (handle, bound.clone()));
            }
            let (handle, target) = self.handles.get_mut(&shard).expect("opened above");
            // Keep previously committed permission through retries and lagging
            // snapshots. Catalog admission must respect already authorized bounds.
            // An incomplete snapshot takes the branch above and applies nothing.
            target.join_assign(&bound);
            if !matches!(
                read_protection::downgrade_since(handle, target).await,
                Some(Ok(_))
            ) {
                pending += 1;
            }
        }
        Ok(pending)
    }
}

fn shard_bounds(
    metadata: &StorageMetadata,
    wanted: &BTreeSet<GlobalId>,
) -> BTreeMap<ShardId, Option<Antichain<Timestamp>>> {
    let mut bounds: BTreeMap<_, _> = wanted
        .iter()
        .filter_map(|id| metadata.collection_metadata.get(id))
        .map(|shard| (*shard, Some(Antichain::new())))
        .collect();
    for (id, shard) in &metadata.collection_metadata {
        if let Some(bound) = bounds.get_mut(shard) {
            match (bound.as_mut(), metadata.compaction_bounds.get(id)) {
                (Some(meet), Some(alias)) => {
                    meet.extend(alias.iter().copied());
                }
                (_, None) => *bound = None,
                (None, Some(_)) => {}
            }
        }
    }
    bounds
}
