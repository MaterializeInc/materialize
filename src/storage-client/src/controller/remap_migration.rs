// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to migrate from a non-Timely-friendly remap shard schema to a
//! Timely-friendly one.

use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Context;
use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_persist_client::error::UpperMismatch;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use tokio::sync::Mutex;
use tracing::info;

use mz_expr::PartitionId;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::{Subscribe, ListenEvent};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::ShardId;
use mz_persist_types::Codec64;
use mz_repr::{Datum, Diff, GlobalId};
use mz_timely_util::order::Partitioned;

use crate::types::sources::MzOffset;
use crate::types::sources::{SourceData, SourceTimestamp};
use crate::util::antichain::MutableOffsetAntichain;
use crate::util::remap_handle::RemapHandleReader;

use super::CollectionMetadata;

/// A struct capable of performing remap shard migrations. See comment on
/// [`RemapHandleMigrator::migrate`].
pub(super) struct RemapHandleMigrator<
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + Codec64 + TotalOrder,
> {
    subscription: Subscribe<SourceData, (), IntoTime, Diff>,
    new_shard_write_handle: WriteHandle<SourceData, (), IntoTime, Diff>,
    since: Antichain<IntoTime>,
    upper: Antichain<IntoTime>,
    _phantom: PhantomData<FromTime>,
}

#[async_trait::async_trait(?Send)]
/// Remap handle migration for partition sources, i.e. Kafka.
impl<IntoTime> RemapHandleReader for RemapHandleMigrator<Partitioned<i32, MzOffset>, IntoTime>
where
    IntoTime: Timestamp + Lattice + Codec64 + TotalOrder,
{
    type FromTime = Partitioned<i32, MzOffset>;
    type IntoTime = IntoTime;

    async fn next(
        &mut self,
    ) -> Option<(
        Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        Antichain<Self::IntoTime>,
    )> {
        let mut updates = vec![];

        let mut progress = Antichain::from_elem(IntoTime::minimum());
        while PartialOrder::less_than(&progress, &self.upper) {
            for event in self.subscription.next().await {
                match event {
                    ListenEvent::Updates(bound_updates) => {
                        for ((update, _), ts, diff) in bound_updates.into_iter() {
                            let binding = match unpack_kafka_remap_binding(update.expect("invalid row")) {
                                Some(b) => b,
                                None => {
                                    assert!(updates.is_empty(), "cannot return once encountered updates");
                                    return None;
                                }
                            };
                            updates.push((binding, ts, diff))
                        }
                    }
                    ListenEvent::Progress(new_progress) => {
                        progress = new_progress;
                    }
                }
            }
        }

        let mut compat_source_upper = MutableOffsetAntichain::new();

        // The following section performs the opposite transalation as the one that
        // happened during compare_and_append.
        // Vector holding the result of the translation
        let mut native_frontier_updates = vec![];

        // First, we will sort the updates by time to be able to iterate over the groups
        updates.sort_unstable_by(|a, b| a.1.cmp(&b.1));

        // This is very subtle. An empty collection of native Antichain elements
        // represents something different than an empty collection of compat
        // OffsetAntichain elements. The former represents the empty antichain and the
        // latter the Antichain containing the minimum element. Therefore we need to
        // always synthesize a minimum timestamp element that happens once at the as_of
        // before processing any updates from the shard.
        let mut first_ts = IntoTime::minimum();
        first_ts.advance_by(self.since.borrow());
        native_frontier_updates.push((Partitioned::minimum(), first_ts, 1));

        // Then, we will iterate over the group of updates in time order and produce
        // the OffsetAntichain at each point, convert it to a normal Antichain, and
        // diff it with current Antichain representation.
        for (ts, updates) in &updates.into_iter().group_by(|update| update.1.clone()) {
            let prev_native_frontier = Antichain::from(compat_source_upper.frontier());
            native_frontier_updates.extend(
                prev_native_frontier
                    .into_iter()
                    .map(|src_ts| (src_ts, ts.clone(), -1)),
            );

            compat_source_upper.update_iter(
                updates
                    .into_iter()
                    .map(|(binding, _ts, diff)| (binding, diff)),
            );
            let new_native_frontier = Antichain::from(compat_source_upper.frontier());

            native_frontier_updates.extend(
                new_native_frontier
                    .into_iter()
                    .map(|src_ts| (src_ts, ts.clone(), 1)),
            );
        }

        // Then, consolidate the native updates and we're done
        consolidation::consolidate_updates(&mut native_frontier_updates);

        Some((native_frontier_updates, progress))
    }
}

#[async_trait::async_trait(?Send)]
/// Remap handle migration for partitionless sources, e.g. Postgres.
impl<IntoTime> RemapHandleReader for RemapHandleMigrator<MzOffset, IntoTime>
where
    IntoTime: Timestamp + Lattice + Codec64 + TotalOrder,
{
    type FromTime = MzOffset;
    type IntoTime = IntoTime;
    /// Look through all of the shard's data and return a 0 offset to be written
    /// if the shard is otherwise empty.
    async fn next(
        &mut self,
    ) -> Option<(
        Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        Antichain<Self::IntoTime>,
    )> {
        let mut progress = Antichain::from_elem(IntoTime::minimum());

        let mut populated = false;

        // Determine whether or not shard is empty and ensure all of its data is
        // properly formatted as `MzOffset` data.
        while !self.upper.less_equal(progress.as_option().unwrap()) {
            for event in self.subscription.next().await {
                match event {
                    ListenEvent::Updates(bound_updates) => {
                        for ((update, _), _, _) in bound_updates.into_iter() {
                            populated = true;
                            MzOffset::decode_row(&update.expect("invalid row").0.expect("invalid row"));
                        }
                    }
                    ListenEvent::Progress(new_progress) => {
                        // Progress is closing shard.
                        if new_progress.elements().is_empty() {
                            break;
                        }
                        progress = new_progress;
                    }
                }
            }
        }

        if populated {
            None
        } else {
            Some((
                vec![(
                    MzOffset { offset: 0 },
                    self.since.as_option().unwrap().clone(),
                    1,
                )],
                progress,
            ))
        }
    }
}

impl<FromTime, IntoTime> RemapHandleMigrator<FromTime, IntoTime>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + Codec64 + TotalOrder,
    RemapHandleMigrator<FromTime, IntoTime>:
        RemapHandleReader<FromTime = FromTime, IntoTime = IntoTime>,
{
    /// Prior to this commit, remap shards contained data of schema (Int32?,
    /// Uint64). Int32 was present only for Kafka sources where it represented
    /// the Kafka partition.
    ///
    /// This schema introduced an impedance mismatch between the remap data and
    /// native Timely types; tl;dr is that an empty remap shard, if taken
    /// directly to Timely time types, produces an empty Antichain (i.e. the
    /// shard is closed from further modification), when in reality we wanted it
    /// to express the minimum Antichain (i.e. any future input is valid).
    ///
    /// To resolve this, we designed a migration to instead use the schema
    /// (Range(Int32)?, UInt64). Range(Int32) represents ranges of Kafka
    /// partitions, where seen partitions are values equivalent to points, and
    /// we include ranges of unseen partitions whose offset is 0 e.g.:
    ///
    /// - (Bottom, 0) -> 0 // trivially empty sub-zero elements in domain
    /// - [0, 1) -> 12 // partition 0 offset
    /// - [1, Top) -> 0 // all unseen partitions
    ///
    /// In addition to migrating Kafka sources to this schema, we also introduce
    /// the mimum element (i.e. the 0 lsn) for Postgres sources that are not new
    /// but whose remap shards are empty.
    pub async fn migrate(
        persist_clients: Arc<Mutex<PersistClientCache>>,
        read_metadata: CollectionMetadata,
        write_shard: ShardId,
        // additional information to improve logging
        id: GlobalId,
    ) -> Option<ShardId> {
        let mut migrator = Self::new(persist_clients, read_metadata, write_shard, id)
            .await
            .unwrap()?;

        let (migrated_form, upper) = migrator.next().await?;
        assert_eq!(upper, migrator.upper);

        info!(
            "remap shard migration({id:?}): writing reformatted data to shard {:?}",
            write_shard
        );

        migrator
            .write_to_new_shard(migrated_form, upper)
            .await
            .unwrap();

        Some(write_shard)
    }

    /// Generates a new `RemapHandleMigrator` if there is content in
    /// `read_metadata`'s remap shard, else returns None.
    pub async fn new(
        persist_clients: Arc<Mutex<PersistClientCache>>,
        read_metadata: CollectionMetadata,
        write_shard: ShardId,
        // additional information to improve logging
        id: GlobalId,
    ) -> anyhow::Result<Option<Self>> {
        let mut persist_clients = persist_clients.lock().await;
        let persist_client = persist_clients
            .open(read_metadata.persist_location.clone())
            .await
            .context("error creating persist client")?;
        drop(persist_clients);

        let (current_shard_write_handle, current_shard_read_handle) = persist_client
            .open::<crate::types::sources::SourceData, (), IntoTime, Diff>(
                read_metadata.remap_shard,
                &format!("reclock {}", id),
            )
            .await
            .context("error opening persist shard")?;

        let upper = current_shard_write_handle.upper().clone();

        if upper.elements() == &[IntoTime::minimum()] {
            // shard not written yet, can skip
            return Ok(None);
        }

        let since = current_shard_read_handle.since().clone();

        tracing::info!(
            ?since,
            ?upper,
            ?read_metadata.remap_shard,
            ?write_shard,
            "RemapHandleMigrator({id}) initializing"
        );

        let subscription = current_shard_read_handle
            .subscribe(since.clone())
            .await
            .expect("subscribing must succeed");

        let (new_shard_write_handle, mut new_shard_read_handle) = persist_client
            .open::<crate::types::sources::SourceData, (), IntoTime, Diff>(
                write_shard,
                "remap shard migration",
            )
            .await
            .expect("error opening persist shard");

        new_shard_read_handle.downgrade_since(&since).await;

        Ok(Some(Self {
            subscription,
            new_shard_write_handle,
            since,
            upper,
            _phantom: PhantomData,
        }))
    }

    async fn write_to_new_shard(
        &mut self,
        updates: Vec<(FromTime, IntoTime, Diff)>,
        new_upper: Antichain<IntoTime>,
    ) -> Result<(), UpperMismatch<IntoTime>> {
        let row_updates = updates.into_iter().map(|(from_ts, ts, diff)| {
            (
                (SourceData(Ok(FromTime::encode_row(&from_ts))), ()),
                ts,
                diff,
            )
        });

        self.new_shard_write_handle
            .append(
                row_updates,
                Antichain::from_elem(IntoTime::minimum()),
                new_upper,
            )
            .await
            .expect("invalid usage")
    }
}

/// Unpacks a Kafka remap binding from a Row. Returns `None` if encounters the
/// new format, in which case the migration doesn't need to go any further.
///
/// # Panics
/// If called on data that would not be generated in either the legacy or new
/// schema of a Kafka source's remap shard.
fn unpack_kafka_remap_binding(data: SourceData) -> Option<(PartitionId, MzOffset)> {
    let row = data.0.expect("invalid binding");
    let mut datums = row.iter();
    match (datums.next(), datums.next()) {
        // Legacy binding
        (Some(Datum::Int32(p)), Some(Datum::UInt64(offset))) => {
            Some((PartitionId::Kafka(p), MzOffset::from(offset)))
        }
        // Current binding
        (Some(Datum::Range(_)), Some(Datum::UInt64(_))) => None,
        _ => panic!("invalid Kafka remap data in collection"),
    }
}
