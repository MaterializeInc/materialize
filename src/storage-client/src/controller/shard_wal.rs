// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Handles tracking persist shards. Currently only identifies shards we might
//! reap but does nothing to actually reclaim their resources.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use differential_dataflow::lattice::Lattice;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
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

/// Metadata about retired shards
#[derive(Arbitrary, Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize, Hash)]
pub struct ReapableShardMetadata {
    pub desc: String,
    pub first_seen: u64,
    // We use `REAPABLE_SHARDS` like a WAL, if you expect all shards to be
    // reclaimed in the fullness of time.
    pub reapable: bool,
}

pub static SHARD_WAL: TypedCollection<ShardId, ReapableShardMetadata> =
    TypedCollection::new("storage-retired-shards");

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,

    // Required to setup grpc clients for new storaged instances.
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    pub(super) async fn register_shards<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = (ShardId, String)>,
    {
        let ts = (self.now)();
        SHARD_WAL
            .insert_without_overwrite(
                &mut self.state.stash,
                entries.into_iter().map(|(shard_id, desc)| {
                    (
                        shard_id,
                        ReapableShardMetadata {
                            desc,
                            first_seen: ts,
                            reapable: false,
                        },
                    )
                }),
            )
            .await
            .expect("must be able to write to stash");
    }

    // Eagerly marking shards reapable currently does nothing, but makes us feel
    // like it might one day.
    pub(super) async fn mark_shards_reapable(&mut self, reapable_shards: &BTreeSet<ShardId>) {
        SHARD_WAL
            .update(
                &mut self.state.stash,
                |k, v| {
                    if reapable_shards.contains(k) {
                        Some(ReapableShardMetadata {
                            reapable: true,
                            ..v.clone()
                        })
                    } else {
                        None
                    }
                },
                |_, _| false,
            )
            .await
            .expect("must be able to write to stash");
    }

    /// Reconcile the state of `REAPABLE_SHARDS` on boot.
    pub(super) async fn reconcile_shards(&mut self) {
        if super::METADATA_COLLECTION
            .upper(&mut self.state.stash)
            .await
            .expect("must be able to connect to stash")
            .elements()
            == [mz_stash::Timestamp::MIN]
        {
            // No metadata yet.
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
                    super::DurableCollectionMetadata {
                        remap_shard,
                        data_shard,
                    },
                )| {
                    [
                        (id, (remap_shard, format!("{:?} remap shard", id))),
                        (id, (data_shard, format!("{:?} data shard", id))),
                    ]
                },
            )
            .flatten()
            .collect();

        // Remove all shards not in collections or with dropped read
        // capabilities.
        let in_use_shards: BTreeSet<_> = all_shard_data
            .iter()
            .filter_map(|(id, (shard_id, _desc))| {
                self.state
                    .collections
                    .get(id)
                    .map(|c| {
                        if c.implied_capability.is_empty() {
                            None
                        } else {
                            Some(*shard_id)
                        }
                    })
                    .flatten()
            })
            .collect();

        // Register all shards to ensure they're in the shard WAL (this should
        // not write any new values).
        self.register_shards(
            all_shard_data
                .into_iter()
                .map(|(_id, (shard_id, desc))| (shard_id, desc)),
        )
        .await;

        // Get all shard IDs we've ever seen that we haven't already determined
        // are reapable.
        let all_shards: BTreeSet<_> = SHARD_WAL
            .peek_one(&mut self.state.stash)
            .await
            .expect("must be able to read from stash")
            .into_iter()
            .filter_map(|(shard_id, metadata)| {
                if metadata.reapable {
                    None
                } else {
                    Some(shard_id)
                }
            })
            .collect();

        // Mark all shards reapable that are no longer in use.
        self.mark_shards_reapable(&all_shards.difference(&in_use_shards).cloned().collect())
            .await;
    }
}
