// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_catalog::durable::{CatalogError, DurableCatalogState};
use mz_catalog::memory::objects::{StateDiff, StateUpdateKind};
use mz_repr::{GlobalId, Timestamp};
use timely::progress::Antichain;

/// Tracks committed compaction bounds independently of the SQL catalog.
#[derive(Debug)]
pub(super) struct CompactionBoundSubscriber {
    catalog: Box<dyn DurableCatalogState>,
    bounds: BTreeMap<GlobalId, Antichain<Timestamp>>,
}

impl CompactionBoundSubscriber {
    /// Takes a dedicated, live readonly handle whose initial updates have not been consumed.
    pub(super) fn new(catalog: Box<dyn DurableCatalogState>) -> Self {
        assert!(catalog.is_read_only());
        Self {
            catalog,
            bounds: BTreeMap::new(),
        }
    }

    /// Consumes committed updates and returns changed surviving bounds.
    /// The first call includes the initial durable state.
    pub(super) async fn sync(
        &mut self,
    ) -> Result<BTreeMap<GlobalId, Antichain<Timestamp>>, CatalogError> {
        // Opening queues the durable initial state. Draining the entire stream, rather
        // than reading snapshots, preserves its continuity and bounds queue growth.
        let updates = self.catalog.sync_to_current_updates().await?;
        let mut bounds: Vec<_> = updates
            .into_iter()
            .filter_map(|update| match update.kind {
                StateUpdateKind::CollectionCompactionBound(bound) => {
                    Some((update.ts, update.diff, bound))
                }
                _ => None,
            })
            .collect();
        // A call can span transactions. Retractions precede additions only within
        // each timestamp, and no await exposes a partially applied transaction.
        bounds.sort_by_key(|(ts, diff, _)| (*ts, *diff));
        let mut changed = BTreeMap::new();
        for (_, diff, bound) in bounds {
            match diff {
                StateDiff::Retraction => {
                    self.bounds.remove(&bound.id);
                    changed.remove(&bound.id);
                }
                StateDiff::Addition => {
                    changed.insert(bound.id, Antichain::from_iter(bound.frontier));
                    self.bounds
                        .insert(bound.id, Antichain::from_iter(bound.frontier));
                }
            }
        }
        Ok(changed)
    }

    /// Returns committed bounds, not permission to read or to drop a collection.
    ///
    /// Record absence is not a drop notification. Fresh indexes have no record,
    /// and retractions must not release execution protection in a frozen catalog.
    pub(super) fn bounds(&self) -> &BTreeMap<GlobalId, Antichain<Timestamp>> {
        &self.bounds
    }

    /// Releases the durable catalog handle.
    pub(super) async fn expire(self) {
        self.catalog.expire().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mz_catalog::durable::{TestCatalogStateBuilder, test_bootstrap_args};
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::{PersistClient, ShardId};
    use mz_storage_client::controller::StorageTxn;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn committed_bounds_follow_writer_independently_of_savepoint() {
        let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
            .with_default_deploy_generation();
        let mut writer = builder
            .clone()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .expect("failed to open writer catalog");
        let _ = writer
            .sync_to_current_updates()
            .await
            .expect("failed to sync writer catalog");

        let id = GlobalId::User(1000);
        let completed = GlobalId::User(1001);
        let mut txn = writer
            .transaction()
            .await
            .expect("failed to start initial bounds transaction");
        txn.insert_collection_metadata(BTreeMap::from([
            (id, ShardId::new()),
            (completed, ShardId::new()),
        ]))
        .expect("failed to insert collection metadata");
        txn.set_collection_compaction_bound(id, Some(10.into()))
            .expect("failed to set initial compaction bound");
        txn.set_collection_compaction_bound(completed, None)
            .expect("failed to set completed compaction bound");
        let _ = txn.get_and_commit_op_updates();
        let ts = txn.upper();
        txn.commit(ts)
            .await
            .expect("failed to commit initial bounds transaction");

        let mut savepoint = builder
            .clone()
            .unwrap_build()
            .await
            .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .expect("failed to open savepoint catalog");
        let _ = savepoint
            .sync_to_current_updates()
            .await
            .expect("failed to sync initial savepoint catalog");
        let frozen_snapshot = savepoint
            .snapshot()
            .await
            .expect("failed to snapshot initial savepoint catalog");
        let reader = builder
            .unwrap_build()
            .await
            .open_read_only(&test_bootstrap_args())
            .await
            .expect("failed to open subscriber catalog");
        let mut subscriber = CompactionBoundSubscriber::new(reader);
        assert!(subscriber.bounds().is_empty());
        let changed = subscriber
            .sync()
            .await
            .expect("failed to sync initial subscriber bounds");
        assert_eq!(&changed, subscriber.bounds());
        assert_eq!(
            subscriber.bounds(),
            &BTreeMap::from([
                (id, Antichain::from_elem(10.into())),
                (completed, Antichain::new()),
            ])
        );

        // Multiple replacements between syncs must not retract the final addition.
        for frontier in [20, 30] {
            let mut txn = writer
                .transaction()
                .await
                .expect("failed to start bound replacement transaction");
            txn.set_collection_compaction_bound(id, Some(frontier.into()))
                .expect("failed to replace compaction bound");
            let _ = txn.get_and_commit_op_updates();
            let ts = txn.upper();
            txn.commit(ts)
                .await
                .expect("failed to commit bound replacement transaction");
        }
        let changed = subscriber
            .sync()
            .await
            .expect("failed to sync replaced subscriber bounds");
        assert_eq!(
            changed,
            BTreeMap::from([(id, Antichain::from_elem(30.into()))])
        );
        assert_eq!(subscriber.bounds()[&id], Antichain::from_elem(30.into()));

        let mut txn = writer
            .transaction()
            .await
            .expect("failed to start pre-deletion bound transaction");
        txn.set_collection_compaction_bound(id, Some(40.into()))
            .expect("failed to advance compaction bound before deletion");
        let _ = txn.get_and_commit_op_updates();
        let ts = txn.upper();
        txn.commit(ts)
            .await
            .expect("failed to commit pre-deletion bound transaction");
        let mut txn = writer
            .transaction()
            .await
            .expect("failed to start collection deletion transaction");
        txn.delete_collection_metadata(BTreeSet::from([id]));
        let _ = txn.get_and_commit_op_updates();
        let ts = txn.upper();
        txn.commit(ts)
            .await
            .expect("failed to commit collection deletion transaction");
        let changed = subscriber
            .sync()
            .await
            .expect("failed to sync subscriber after collection deletion");
        assert!(changed.is_empty(), "a dropped lifetime is not delivered");
        assert_eq!(
            subscriber.bounds(),
            &BTreeMap::from([(completed, Antichain::new())])
        );
        let changed = subscriber
            .sync()
            .await
            .expect("failed to sync subscriber without new updates");
        assert!(changed.is_empty(), "unchanged bounds are not redelivered");
        assert_eq!(
            subscriber.bounds(),
            &BTreeMap::from([(completed, Antichain::new())])
        );
        assert!(
            savepoint
                .sync_to_current_updates()
                .await
                .expect("failed to sync savepoint after writer updates")
                .is_empty()
        );
        assert_eq!(
            savepoint
                .snapshot()
                .await
                .expect("failed to snapshot savepoint after writer updates"),
            frozen_snapshot
        );

        subscriber.expire().await;
        savepoint.expire().await;
        writer.expire().await;
    }
}
