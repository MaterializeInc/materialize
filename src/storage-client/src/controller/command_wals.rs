// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tracks persist shards ready to be finalized, i.e. remove the ability to read
//! or write them. This identifies shards we no longer use, but did not have the
//! opportunity to finalize before e.g. crashing.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use mz_ore::halt;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use mz_ore::now::EpochMillis;
use mz_persist_client::ShardId;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::TimestampManipulation;

use crate::client::{StorageCommand, StorageResponse};
use crate::controller::{ProtoStorageCommand, ProtoStorageResponse};

use super::Controller;

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: super::StorageController<Timestamp = T>,
{
    /// `true` if shard is in register for shards marked for finalization.
    pub(super) fn is_shard_registered_for_finalization(&mut self, shard: ShardId) -> bool {
        self.durable_state.is_in_finalization_wal(&shard)
    }

    /// Register shards for finalization. This must be called if you intend to
    /// finalize shards, before you perform any work to e.g. replace one shard
    /// with another.
    ///
    /// The reasoning behind this is that we need to identify the intent to
    /// finalize a shard so we can perform the finalization on reboot if we
    /// crash and do not find the shard in use in any collection.
    #[allow(dead_code)]
    pub(super) async fn register_shards_for_finalization<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = ShardId>,
    {
        let mut updates = Vec::new();

        for shard in entries.into_iter() {
            updates.append(&mut self.durable_state.prepare_insert_finalized_shard(shard));
        }

        let res = self.durable_state.commit_updates(updates).await;

        match res {
            Ok(()) => {
                // All's well!
            }
            Err(upper_mismatch) => {
                // TODO(aljoscha): We could do a number of things in here:
                // Assume there is only one controller and halt once we get a
                // mismatch, because it means someone else took over. Or we
                // could retry and use the updates that the other controller
                // committed.
                //
                // One problem is figuring out fencing. Before, we used the
                // stash to fence out older controller versions.
                halt!(
                    "could not update durable controller state: {:?}",
                    upper_mismatch
                );
            }
        }
    }

    /// Removes the shard from the finalization register.
    ///
    /// This is appropriate to do if you can guarantee that the shard has been
    /// finalized or find the shard is still in use by some collection.
    pub(super) async fn clear_from_shard_finalization_register(
        &mut self,
        shards: BTreeSet<ShardId>,
    ) {
        let mut updates = Vec::new();

        for shard in shards.into_iter() {
            updates.append(&mut self.durable_state.prepare_remove_finalized_shard(shard));
        }

        let res = self.durable_state.commit_updates(updates).await;

        match res {
            Ok(()) => {
                // All's well!
            }
            Err(upper_mismatch) => {
                // TODO(aljoscha): We could do a number of things in here:
                // Assume there is only one controller and halt once we get a
                // mismatch, because it means someone else took over. Or we
                // could retry and use the updates that the other controller
                // committed.
                //
                // One problem is figuring out fencing. Before, we used the
                // stash to fence out older controller versions.
                halt!(
                    "could not update durable controller state: {:?}",
                    upper_mismatch
                );
            }
        }
    }

    /// Reconcile the state of `SHARD_FINALIZATION_WAL` with
    /// `super::METADATA_COLLECTION` on boot.
    pub(super) async fn reconcile_shards(&mut self) {
        // Get all shards in the WAL.
        let registered_shards: BTreeSet<_> = self
            .durable_state
            .get_finalization_wal()
            .into_iter()
            .collect();

        if registered_shards.is_empty() {
            return;
        }

        // Get all shards we're aware of from durable state.
        let all_data_shards = self.durable_state.get_all_data_shards();
        let all_remap_shards = self.durable_state.get_all_remap_shards();
        let all_shard_data: BTreeMap<_, _> = all_data_shards
            .into_iter()
            .chain(all_remap_shards.into_iter())
            .collect();

        // Consider shards in use if they are still attached to a collection.
        let in_use_shards: BTreeSet<_> = all_shard_data
            .into_iter()
            .filter_map(|(id, shard)| self.state.collections.get(&id).map(|_| Some(shard)))
            .flatten()
            .collect();

        // Determine all shards that were marked to drop but are still in use by
        // some present collection.
        let shard_ids_to_keep = registered_shards
            .intersection(&in_use_shards)
            .cloned()
            .collect();

        // Remove any shards that are currently in use from shard finalization register.
        self.clear_from_shard_finalization_register(shard_ids_to_keep)
            .await;

        // Determine all shards that are registered that are not in use.
        let shard_id_desc_to_truncate: Vec<_> = registered_shards
            .difference(&in_use_shards)
            .cloned()
            .map(|shard_id| (shard_id, "finalizing unused shard".to_string()))
            .collect();

        self.finalize_shards(&shard_id_desc_to_truncate).await;
    }
}
