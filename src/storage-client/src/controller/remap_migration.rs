// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;

use timely::order::TotalOrder;
use timely::progress::Timestamp;

use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::{GlobalId, TimestampManipulation};
use tracing::info;

use crate::client::{StorageCommand, StorageResponse};
use crate::controller::{ProtoStorageCommand, ProtoStorageResponse};
use crate::types::sources::IngestionDescription;

use super::{
    CollectionDescription, Controller, DataSource, DurableCollectionMetadata, StorageController,
};

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: StorageController<Timestamp = T>,
{
    /// Determine the delta between `durable_metadata` and `collections` such
    /// that:
    /// - Each remap collection's data shard is its parent collection's remap
    ///   shard.
    /// - No collection contains a `Some` value for its remap shard.
    ///
    /// Apply this delta using
    /// `Controller::upsert_collection_metadata(durable_metadata, <this
    /// function's return value>)`.
    ///
    /// This approach is safe/backward compatible because:
    /// - Every ingestion collection previously had a remap shard
    /// - Every ingestion collection now has a progress collection subsource,
    ///   whose data shard should be the remap shard
    /// - No other type of collection used their remap shards, so dropping them
    ///   entirely is essentially a nop.
    ///
    /// MIGRATION: v0.44
    pub(super) fn remap_shard_migration(
        &mut self,
        durable_metadata: &BTreeMap<GlobalId, DurableCollectionMetadata>,
        collections: &[(GlobalId, CollectionDescription<T>)],
    ) -> BTreeMap<GlobalId, DurableCollectionMetadata> {
        info!("applying remap shard migration");

        let mut state_to_update = BTreeMap::new();

        let mut progress_collections_pending = BTreeSet::new();

        for (id, desc) in collections {
            // Track that we might be adding metadata for progress collections.
            if matches!(desc.data_source, DataSource::Progress) {
                progress_collections_pending.insert(*id);
            }

            let mut current_metadata = match durable_metadata.get(id) {
                Some(c) => {
                    // If we see any existing collection with a retired remap
                    // shard, we know the migration has already completed.
                    if c.remap_shard.is_none() {
                        return BTreeMap::new();
                    }

                    c.clone()
                }
                // If the item has not yet been added to `durable_metadata`,
                // skip it. If this is a progress collection, we will add it to
                // `durable_metadata` once we see its data collection.
                None => {
                    continue;
                }
            };

            if let DataSource::Ingestion(IngestionDescription {
                remap_collection_id,
                ..
            }) = &desc.data_source
            {
                let current_remap_shard = current_metadata
                    .remap_shard
                    .expect("must have remap shard and must not have been migrated yet");

                // Insert metadata for progress collection.
                state_to_update.insert(
                    *remap_collection_id,
                    DurableCollectionMetadata {
                        remap_shard: None,
                        data_shard: current_remap_shard,
                    },
                );
            };

            // Fixup this collection such that it no longer contains reference
            // to a remap shard; if we needed it for the progress collection,
            // we've already used it by this point.
            current_metadata.remap_shard = None;
            state_to_update.insert(*id, current_metadata);
        }

        assert!(
            progress_collections_pending
                .into_iter()
                .all(|id| state_to_update.contains_key(&id)),
            "all pending progress collections received their parent collection's prev remap \
            shards as their data shard"
        );

        state_to_update
    }
}
