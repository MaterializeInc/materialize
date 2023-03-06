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
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use mz_ore::now::EpochMillis;
use mz_persist_client::ShardId;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::TimestampManipulation;
use mz_stash::{self, TypedCollection};

use crate::client::{StorageCommand, StorageResponse};
use crate::controller::{ProtoStorageCommand, ProtoStorageResponse};

use super::Controller;

pub(super) static SHARD_FINALIZATION: TypedCollection<ShardId, ()> =
    TypedCollection::new("storage-shards-to-finalize");

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: super::StorageController<Timestamp = T>,
{
    /// `true` if shard is in register for shards marked for finalization.
    pub(super) async fn is_shard_registered_for_finalization(&mut self, shard: ShardId) -> bool {
        SHARD_FINALIZATION
            .peek_key_one(&mut self.state.stash, shard)
            .await
            .expect("must be able to connect to stash")
            .is_some()
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
        SHARD_FINALIZATION
            .insert_without_overwrite(
                &mut self.state.stash,
                entries.into_iter().map(|key| (key, ())),
            )
            .await
            .expect("must be able to write to stash")
    }

    /// Removes the shard from the finalization register.
    ///
    /// This is appropriate to do if you can guarantee that the shard has been
    /// finalized or find the shard is still in use by some collection.
    pub(super) async fn clear_from_shard_finalization_register(
        &mut self,
        shards: BTreeSet<ShardId>,
    ) {
        SHARD_FINALIZATION
            .delete(&mut self.state.stash, move |k, _v| shards.contains(k))
            .await
            .expect("must be able to write to stash")
    }

    /// Reconcile the state of `SHARD_FINALIZATION_WAL` with
    /// `super::METADATA_COLLECTION` on boot.
    pub(super) async fn reconcile_shards(&mut self) {
        // Get all shards in the WAL.
        let registered_shards: BTreeSet<_> = SHARD_FINALIZATION
            .peek_one(&mut self.state.stash)
            .await
            .expect("must be able to read from stash")
            .into_iter()
            .map(|(shard_id, _)| shard_id)
            .collect();

        if registered_shards.is_empty() {
            return;
        }

        // Get all shards we're aware of from stash.
        let all_shard_data: BTreeMap<_, _> = super::METADATA_COLLECTION
            .peek_one(&mut self.state.stash)
            .await
            .expect("must be able to read from stash")
            .into_iter()
            .map(
                |(
                    id,
                    // TODO(guswynn): produce the schema for each shard.
                    super::DurableCollectionMetadata {
                        remap_shard,
                        data_shard,
                    },
                )| { [(id, [remap_shard, Some(data_shard)])] },
            )
            .flatten()
            .collect();

        // Consider shards in use if they are still attached to a collection.
        let in_use_shards: BTreeSet<_> = all_shard_data
            .into_iter()
            .filter_map(|(id, shards)| self.state.collections.get(&id).map(|_| shards.to_vec()))
            .flatten()
            .filter_map(|shard| shard)
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
        self.finalize_shards(registered_shards.difference(&in_use_shards).cloned())
            .await;
    }
}
