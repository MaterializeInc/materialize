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

use std::collections::BTreeSet;

use differential_dataflow::lattice::Lattice;
use mz_ore::now::EpochMillis;
use mz_persist_client::ShardId;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::{Diff, TimestampManipulation};
use mz_stash::{self, TypedCollection};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use crate::client::{StorageCommand, StorageResponse};
use crate::controller::{
    Controller, DurableCollectionMetadata, ProtoStorageCommand, ProtoStorageResponse,
    StorageController,
};

pub(super) type ProtoShardId = String;
pub(super) static SHARD_FINALIZATION: TypedCollection<ProtoShardId, ()> =
    TypedCollection::new("storage-shards-to-finalize");

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: super::StorageController<Timestamp = T>,
{
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
                entries.into_iter().map(|key| (key.into_proto(), ())),
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
        let proto_shards = shards
            .into_iter()
            .map(|s| RustType::into_proto(&s))
            .collect();
        SHARD_FINALIZATION
            .delete_keys(&mut self.state.stash, proto_shards)
            .await
            .expect("must be able to write to stash")
    }

    pub(super) async fn reconcile_state_inner(&mut self) {
        // Convenience method for reading from a collection in parallel.
        async fn tx_peek<'tx, KP, VP, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            typed: &TypedCollection<KP, VP>,
        ) -> Vec<(K, V, Diff)>
        where
            KP: mz_stash::Data,
            VP: mz_stash::Data,
            K: RustType<KP>,
            V: RustType<VP>,
        {
            let collection = tx
                .collection::<KP, VP>(typed.name())
                .await
                .expect("named collection must exist");
            tx.peek(collection)
                .await
                .expect("peek succeeds")
                .into_iter()
                .map(|(k, v, diff)| {
                    let k = K::from_proto(k).expect("deserialization to succeed");
                    let v = V::from_proto(v).expect("deserialization to succeed");
                    (k, v, diff)
                })
                .collect()
        }

        // Get stash metadata.
        let (metadata, shard_finalization) = self
            .state
            .stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    // Query all collections in parallel.
                    Ok(futures::join!(
                        tx_peek(&tx, &super::METADATA_COLLECTION),
                        tx_peek(&tx, &SHARD_FINALIZATION),
                    ))
                })
            })
            .await
            .expect("stash operation succeeds");

        // Partition metadata into the collections we want to have and those we failed to drop.
        let (in_use_collections, leaked_collections): (Vec<_>, Vec<_>) =
            metadata.into_iter().partition(|(id, _, diff)| {
                assert_eq!(
                    *diff, 1,
                    "expected METADATA_COLLECTION to contain reconciled state"
                );

                self.collection(*id).is_ok()
            });

        // Get all shard IDs
        let shard_finalization: BTreeSet<_> = shard_finalization
            .into_iter()
            .map(|(id, _, diff): (_, (), _)| {
                assert_eq!(
                    diff, 1,
                    "expected SHARD_FINALIZATION to contain reconciled state"
                );

                id
            })
            .collect();

        // Collect all shards from in-use collections
        let in_use_shards: BTreeSet<_> = in_use_collections
            .iter()
            // n.b we do not include remap shards here because they are the data shards of their own
            // collections.
            .map(|(_, DurableCollectionMetadata { data_shard, .. }, _)| *data_shard)
            .collect();

        // Determine if there are any shards that belong to in-use collections
        // that we have marked for finalization. This is usually inconceivable,
        // but could happen if we e.g. crash during a migration.
        let in_use_shards_registered_for_finalization: BTreeSet<_> = shard_finalization
            .intersection(&in_use_shards)
            .cloned()
            .collect();

        // Fixup shard finalization WAL if necessary.
        if !in_use_shards_registered_for_finalization.is_empty() {
            self.clear_from_shard_finalization_register(in_use_shards_registered_for_finalization)
                .await;
        }

        // If we know about collections that the adapter has forgotten about, clean that up.
        if !leaked_collections.is_empty() {
            let mut shards_to_finalize = Vec::with_capacity(leaked_collections.len());
            let mut ids_to_drop = BTreeSet::new();

            for (id, DurableCollectionMetadata { data_shard, .. }, _) in leaked_collections {
                shards_to_finalize.push(data_shard);
                ids_to_drop.insert(id);
            }

            // Note that we register the shards for finalization but do not
            // finalize them here; this is meant to speed up startup times, as
            // we can defer actually finalizing shards.
            self.register_shards_for_finalization(shards_to_finalize)
                .await;

            let proto_ids_to_drop = ids_to_drop
                .into_iter()
                .map(|id| RustType::into_proto(&id))
                .collect();
            super::METADATA_COLLECTION
                .delete_keys(&mut self.state.stash, proto_ids_to_drop)
                .await
                .expect("stash operation must succeed");
        }
    }
}
