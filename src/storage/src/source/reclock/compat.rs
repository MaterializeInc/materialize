// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reclocking compatibility code until the whole ingestion pipeline is transformed to native
//! timestamps

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use differential_dataflow::lattice::Lattice;
use futures::{stream::LocalBoxStream, StreamExt};
use mz_persist_client::error::UpperMismatch;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_expr::PartitionId;
use mz_ore::halt;
use mz_ore::vec::VecExt;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::PersistClient;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::controller::PersistEpoch;
use mz_storage_client::types::sources::{MzOffset, SourceData, SourceTimestamp};
use mz_storage_client::util::antichain::OffsetAntichain;
use mz_storage_client::util::remap_handle::{RemapHandle, RemapHandleReader};

use crate::source::reclock::{ReclockBatch, ReclockError, ReclockFollower, ReclockOperator};

impl<FromTime, IntoTime> ReclockFollower<FromTime, IntoTime>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + TotalOrder,
{
    pub fn reclock_compat<'a, M>(
        &'a self,
        batch: &'a mut HashMap<PartitionId, Vec<(M, MzOffset)>>,
    ) -> Result<impl Iterator<Item = (M, IntoTime)> + 'a, ReclockError<FromTime>> {
        let mut reclock_results = HashMap::with_capacity(batch.len());
        // Eagerly compute all the reclocked times to check if we need to report an error
        for (pid, updates) in batch.iter() {
            let mut pid_results = Vec::with_capacity(updates.len());
            for (_msg, offset) in updates.iter() {
                let src_ts = FromTime::from_compat_ts(pid.clone(), *offset);
                pid_results.push(self.reclock_time_total(&src_ts)?);
            }
            reclock_results.insert(pid.clone(), pid_results);
        }
        Ok(batch.iter_mut().flat_map(move |(pid, updates)| {
            let results = reclock_results.remove(pid).expect("created above");
            updates
                .drain(..)
                .zip(results)
                .map(|((msg, _offset), ts)| (msg, ts))
        }))
    }
}

impl<FromTime, IntoTime, Clock, Handle> ReclockOperator<FromTime, IntoTime, Handle, Clock>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + TotalOrder,
    Handle: RemapHandle<FromTime = FromTime, IntoTime = IntoTime>,
    Clock: futures::Stream<Item = (IntoTime, Antichain<IntoTime>)> + Unpin,
{
    pub async fn mint_compat(
        &mut self,
        source_frontier: &OffsetAntichain,
    ) -> ReclockBatch<FromTime, IntoTime> {
        // The old API didn't require that each mint request is beyond the current upper but it was
        // doing some sort of max calculation for all the partitions. This is not possible do to
        // for generic partially order timestamps so this is what this compatibility function is
        // trying to bridge.
        //
        // First, take the current source upper and converting it to an OffsetAntichain
        let mut current_upper = FromTime::into_compat_frontier(self.source_upper.frontier());
        for (pid, offset) in source_frontier.iter() {
            // Then, for each offset in the frontier that we called with try to insert it into the
            // OffsetAntichain. `maybe_insert` will only insert it if it's larger than what's
            // already there
            current_upper.maybe_insert(pid.clone(), *offset);
        }
        // Finally, convert it back to a native frontier and mint. The frontier we produce here is
        // guaranteed to be greater than or equal to the existing source upper so the mint call
        // will never panic
        self.mint(FromTime::from_compat_frontier(current_upper).borrow())
            .await
    }
}

/// A handle to a persist shard that stores remap bindings
pub struct PersistHandle<FromTime: SourceTimestamp, IntoTime: Timestamp + Lattice + Codec64> {
    id: GlobalId,
    since_handle: SinceHandle<SourceData, (), IntoTime, Diff, PersistEpoch>,
    events: LocalBoxStream<'static, ListenEvent<IntoTime, ((Result<SourceData, String>, Result<(), String>), IntoTime, Diff)>>,
    write_handle: WriteHandle<SourceData, (), IntoTime, Diff>,
    pending_batch: Vec<(FromTime, IntoTime, Diff)>,
}

impl<FromTime: Timestamp, IntoTime: Timestamp> PersistHandle<FromTime, IntoTime>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + Codec64,
{
    pub async fn new(
        persist_clients: Arc<Mutex<PersistClientCache>>,
        metadata: CollectionMetadata,
        as_of: Antichain<IntoTime>,
        // additional information to improve logging
        id: GlobalId,
        operator: &str,
        worker_id: usize,
        worker_count: usize,
    ) -> anyhow::Result<Self> {
        let mut persist_clients = persist_clients.lock().await;
        let persist_client = persist_clients
            .open(metadata.persist_location.clone())
            .await
            .context("error creating persist client")?;
        drop(persist_clients);

        let since_handle: SinceHandle<_, _, _, _, PersistEpoch> = persist_client
            .open_critical_since(
                metadata.remap_shard.clone(),
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                &format!("reclock {}", id),
            )
            .await
            .expect("invalid persist usage");

        let since = since_handle.since();

        let (write_handle, mut read_handle) = persist_client
            .open(metadata.remap_shard.clone(), &format!("reclock {}", id))
            .await
            .context("error opening persist shard")?;

        let upper = write_handle.upper();

        assert!(
            PartialOrder::less_equal(since, &as_of),
            "invalid as_of: as_of({as_of:?}) < since({since:?}), \
            source {id}, \
            remap_shard: {}",
            metadata.remap_shard
        );

        assert!(
            as_of.elements() == [IntoTime::minimum()] || PartialOrder::less_than(&as_of, upper),
            "invalid as_of: upper({upper:?}) <= as_of({as_of:?})",
        );

        tracing::info!(
            ?since,
            ?as_of,
            ?upper,
            "{operator}({id}) {worker_id}/{worker_count} initializing PersistHandle"
        );

        use futures::stream;
        let events = stream::once(async move {
            let updates = read_handle
                .snapshot_and_fetch(as_of.clone())
                .await
                .expect("since <= as_of asserted");
            let snapshot = stream::once(std::future::ready(ListenEvent::Updates(updates)));

            let listener = read_handle
                .listen(as_of.clone())
                .await
                .expect("since <= as_of asserted");

            let listen_stream = stream::unfold(listener, |mut listener| async move {
                let events = stream::iter(listener.next().await);
                Some((events, listener))
            })
            .flatten();

            snapshot.chain(listen_stream)
        })
        .flatten()
        .boxed_local();

        Ok(Self {
            id,
            since_handle,
            events,
            write_handle,
            pending_batch: vec![],
        })
    }
}

#[async_trait::async_trait(?Send)]
impl<FromTime, IntoTime> RemapHandleReader for PersistHandle<FromTime, IntoTime>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + Codec64,
{
    type FromTime = FromTime;
    type IntoTime = IntoTime;

    async fn next(
        &mut self,
    ) -> Option<(
        Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        Antichain<Self::IntoTime>,
    )> {
        while let Some(event) = self.events.next().await {
            match event {
                ListenEvent::Progress(new_upper) => {
                    // Peel off a batch of pending data
                    let batch = self
                        .pending_batch
                        .drain_filter_swapping(|(_, ts, _)| !new_upper.less_equal(ts))
                        .collect();
                    return Some((batch, new_upper));
                }
                ListenEvent::Updates(msgs) => {
                    for ((update, _), into_ts, diff) in msgs {
                        let from_ts = FromTime::decode_row(
                            &update.expect("invalid row").0.expect("invalid row"),
                        );
                        self.pending_batch.push((from_ts, into_ts, diff));
                    }
                }
            }
        }
        None
    }
}

#[async_trait::async_trait(?Send)]
impl<FromTime, IntoTime> RemapHandle for PersistHandle<FromTime, IntoTime>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + Codec64,
{
    async fn compare_and_append(
        &mut self,
        updates: Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        upper: Antichain<Self::IntoTime>,
        new_upper: Antichain<Self::IntoTime>,
    ) -> Result<(), UpperMismatch<Self::IntoTime>> {
        let row_updates = updates.into_iter().map(|(from_ts, into_ts, diff)| {
            ((SourceData(Ok(from_ts.encode_row())), ()), into_ts, diff)
        });

        match self
            .write_handle
            .compare_and_append(row_updates, upper, new_upper)
            .await
        {
            Ok(result) => return result,
            Err(invalid_use) => panic!("compare_and_append failed: {invalid_use}"),
        }
    }

    async fn compact(&mut self, new_since: Antichain<Self::IntoTime>) {
        if !PartialOrder::less_equal(self.since_handle.since(), &new_since) {
            panic!(
                "ReclockFollower: `new_since` ({:?}) is not beyond \
                `self.since` ({:?}).",
                new_since,
                self.since_handle.since(),
            );
        }
        let epoch = self.since_handle.opaque().clone();
        let result = self
            .since_handle
            .maybe_compare_and_downgrade_since(&epoch, (&epoch, &new_since))
            .await;

        if let Some(result) = result {
            match result {
                Ok(_) => {
                    // All's well!
                }
                Err(current_epoch) => {
                    // TODO(aljoscha): In the future, we might want to be
                    // smarter about being fenced off. Or maybe not? For now,
                    // halting seems to be the only option, but we want to get
                    // rid of halting in sources/sinks. On the other hand, when
                    // we have been fenced off, it seems fine to halt the whole
                    // process?
                    //
                    // SUBTLE: It's fine if multiple/concurrent remap
                    // operators/source advance the since of the remap shard.
                    // They would only do that once both the data shard and
                    // remap shard are sufficiently advanced, meaning we will
                    // always be in a state from which we can safely restart.
                    halt!(
                        "We have been fenced off! source_id: {}, current epoch: {:?}",
                        self.id,
                        current_epoch
                    );
                }
            }
        }
    }

    fn upper(&self) -> &Antichain<Self::IntoTime> {
        self.write_handle.upper()
    }

    fn since(&self) -> &Antichain<Self::IntoTime> {
        self.since_handle.since()
    }
}
