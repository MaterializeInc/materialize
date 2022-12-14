// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to migrate from the prior vers
use std::sync::Arc;

use anyhow::Context;
use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::progress::Timestamp;
use tokio::sync::Mutex;
use tracing::info;

use mz_expr::PartitionId;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::{PersistClient, ShardId, Upper};
use mz_repr::{Datum, Diff, GlobalId, TimestampManipulation};
use mz_timely_util::order::Partitioned;

use crate::types::sources::data::SourceData;
use crate::types::sources::MzOffset;
use crate::util::antichain::MutableOffsetAntichain;
use crate::util::remap_handle::RemapHandle;

use super::CollectionMetadata;

/// A handle to a persist shard that stores remap bindings
pub struct RemapHandleMigrator<
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
> {
    persist_client: PersistClient,
    current_shard_read_handle: ReadHandle<SourceData, (), T, Diff>,
    as_of: Antichain<T>,
    write_shard: ShardId,
}

impl<T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation>
    RemapHandleMigrator<T>
{
    pub async fn migrate(
        persist_clients: Arc<Mutex<PersistClientCache>>,
        read_metadata: CollectionMetadata,
        write_shard: ShardId,
        // additional information to improve logging
        id: GlobalId,
    ) -> Option<()> {
        let mut migrator = Self::new(persist_clients, read_metadata.clone(), write_shard, id)
            .await
            .unwrap()?;

        let (migrated_form, upper) = migrator.next().await?;

        assert!(
            !migrated_form
                .iter()
                .any(|(partition, _, _)| matches!(partition.partition(), Some(PartitionId::None))),
            "only Kafka sources' remap shards are migrated"
        );

        info!(
            "migrating {:?}'s remap shard {:?} to {:?}",
            id, read_metadata.remap_shard, write_shard
        );

        migrator
            .compare_and_append(migrated_form, Antichain::from_elem(T::minimum()), upper)
            .await
            .expect("writing to new shard must succeed");

        info!("migration of {id:?}'s remap shard complete");

        Some(())
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
            .open::<crate::types::sources::data::SourceData, (), T, Diff>(
                read_metadata.remap_shard,
                &format!("reclock {}", id),
            )
            .await
            .context("error opening persist shard")?;

        let (since, upper) = (
            current_shard_read_handle.since(),
            current_shard_write_handle.upper().clone(),
        );

        let as_of = match upper.as_option() {
            Some(u) if u > &T::minimum() => Antichain::from_elem(u.step_back().unwrap()),
            // Source is terminated or new; nothing to migrate
            _ => return Ok(None),
        };

        assert!(
            PartialOrder::less_equal(since, &as_of),
            "invalid as_of: as_of({as_of:?}) < since({since:?}), \
            source {id}, \
            remap_shard: {}",
            read_metadata.remap_shard
        );

        assert!(
            as_of.elements() == [T::minimum()] || PartialOrder::less_than(&as_of, &upper),
            "invalid as_of: upper({upper:?}) <= as_of({as_of:?})",
        );

        tracing::info!(
            ?since,
            ?as_of,
            ?upper,
            "RemapHandleMigrator({id}) initializing"
        );

        Ok(Some(Self {
            persist_client,
            current_shard_read_handle,
            as_of,
            write_shard,
        }))
    }
}

/// Unpacks a binding from a Row
fn unpack_binding(data: SourceData) -> Option<(PartitionId, MzOffset)> {
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

#[async_trait::async_trait(?Send)]
impl<T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation>
    RemapHandle for RemapHandleMigrator<T>
{
    type FromTime = Partitioned<PartitionId, MzOffset>;
    type IntoTime = T;

    async fn compare_and_append(
        &mut self,
        updates: Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        upper: Antichain<Self::IntoTime>,
        new_upper: Antichain<Self::IntoTime>,
    ) -> Result<(), Upper<Self::IntoTime>> {
        let (mut new_shard_write_handle, mut new_shard_read_handle) = self
            .persist_client
            .open::<crate::types::sources::data::SourceData, (), T, Diff>(
                self.write_shard,
                "remap shard migration",
            )
            .await
            .expect("error opening persist shard");

        let row_updates = updates
            .into_iter()
            .map(|(partition, ts, diff)| ((SourceData::from(partition), ()), ts, diff));

        new_shard_write_handle
            .append(row_updates, upper, new_upper)
            .await
            .expect("invalid usage")?;

        new_shard_read_handle
            .downgrade_since(self.current_shard_read_handle.since())
            .await;

        Ok(())
    }

    async fn next(
        &mut self,
    ) -> Option<(
        Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        Antichain<Self::IntoTime>,
    )> {
        let snapshot = self
            .current_shard_read_handle
            .snapshot_and_fetch(self.as_of.clone())
            .await
            .expect("local since is not beyond read handle's since");

        let mut updates = Vec::with_capacity(snapshot.len());

        for ((update, _), ts, diff) in snapshot.into_iter() {
            let binding = unpack_binding(update.expect("invalid row"))?;
            updates.push((binding, ts, diff))
        }

        let mut native_source_upper = MutableAntichain::new();
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
        let mut first_ts = T::minimum();
        first_ts.advance_by(self.as_of.borrow());
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

        // Finally, apply the updates to our local view of the native frontier
        native_source_upper.update_iter(
            native_frontier_updates
                .iter()
                .map(|(src_ts, _ts, diff)| (src_ts.clone(), *diff)),
        );

        let upper = self
            .as_of
            .as_option()
            .expect("as of previously verified to be valid")
            .step_forward();

        Some((native_frontier_updates, Antichain::from_elem(upper)))
    }

    async fn compact(&mut self, _new_since: Antichain<Self::IntoTime>) {
        unimplemented!()
    }

    fn upper(&self) -> &Antichain<Self::IntoTime> {
        unimplemented!()
    }

    fn since(&self) -> &Antichain<Self::IntoTime> {
        unimplemented!()
    }
}
