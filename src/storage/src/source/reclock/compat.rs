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
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_expr::PartitionId;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::{ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::{Diff, GlobalId};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::sources::data::SourceData;
use mz_storage_client::types::sources::MzOffset;
use mz_storage_client::util::antichain::OffsetAntichain;
use mz_storage_client::util::remap_handle::RemapHandle;
use mz_timely_util::order::Partitioned;

use crate::source::reclock::{ReclockBatch, ReclockError, ReclockFollower, ReclockOperator};

impl ReclockFollower<Partitioned<PartitionId, MzOffset>, mz_repr::Timestamp> {
    pub fn reclock_compat<'a, M>(
        &'a self,
        batch: &'a mut HashMap<PartitionId, Vec<(M, MzOffset)>>,
    ) -> Result<
        impl Iterator<Item = (M, mz_repr::Timestamp)> + 'a,
        ReclockError<Partitioned<PartitionId, MzOffset>>,
    > {
        let mut reclock_results = HashMap::with_capacity(batch.len());
        // Eagerly compute all the reclocked times to check if we need to report an error
        for (pid, updates) in batch.iter() {
            let mut pid_results = Vec::with_capacity(updates.len());
            for (_msg, offset) in updates.iter() {
                let src_ts = Partitioned::with_partition(pid.clone(), *offset);
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

impl<IntoTime, Clock, Handle>
    ReclockOperator<Partitioned<PartitionId, MzOffset>, IntoTime, Handle, Clock>
where
    IntoTime: Timestamp + Lattice + TotalOrder,
    Handle: RemapHandle<FromTime = Partitioned<PartitionId, MzOffset>, IntoTime = IntoTime>,
    Clock: futures::Stream<Item = (IntoTime, Antichain<IntoTime>)> + Unpin,
{
    pub async fn mint_compat(
        &mut self,
        source_frontier: &OffsetAntichain,
    ) -> ReclockBatch<Partitioned<PartitionId, MzOffset>, IntoTime> {
        // The old API didn't require that each mint request is beyond the current upper but it was
        // doing some sort of max calculation for all the partitions. This is not possible do to
        // for generic partially order timestamps so this is what this compatibility function is
        // trying to bridge.
        //
        // First, take the current source upper and converting it to an OffsetAntichain
        let mut current_upper = OffsetAntichain::from(self.source_upper.frontier().to_owned());
        for (pid, offset) in source_frontier.iter() {
            // Then, for each offset in the frontier that we called with try to insert it into the
            // OffsetAntichain. `maybe_insert` will only insert it if it's larger than what's
            // already there
            current_upper.maybe_insert(pid.clone(), *offset);
        }

        // Finally, convert it back to a native frontier and mint. The frontier we produce here is
        // guaranteed to be greater than or equal to the existing source upper so the mint call
        // will never panic
        self.mint(Antichain::from(current_upper).borrow()).await
    }
}

/// A handle to a persist shard that stores remap bindings
pub struct PersistHandle {
    read_handle: ReadHandle<SourceData, (), mz_repr::Timestamp, Diff>,
    events: LocalBoxStream<'static, ListenEvent<SourceData, (), mz_repr::Timestamp, Diff>>,
    write_handle: WriteHandle<SourceData, (), mz_repr::Timestamp, Diff>,
    snapshot_produced: bool,
    as_of: Antichain<mz_repr::Timestamp>,
    pending_batch: Vec<(Partitioned<PartitionId, MzOffset>, mz_repr::Timestamp, Diff)>,
    synthesize_partion_minimums: bool,
    partitioned_source: bool,
}

impl PersistHandle {
    pub async fn new(
        persist_clients: Arc<Mutex<PersistClientCache>>,
        metadata: CollectionMetadata,
        as_of: Antichain<mz_repr::Timestamp>,
        // additional information to improve logging
        id: GlobalId,
        operator: &str,
        worker_id: usize,
        worker_count: usize,
        partitioned_source: bool,
    ) -> anyhow::Result<Self> {
        let mut persist_clients = persist_clients.lock().await;
        let persist_client = persist_clients
            .open(metadata.persist_location)
            .await
            .context("error creating persist client")?;
        drop(persist_clients);

        let (write_handle, read_handle) = persist_client
            .open(metadata.remap_shard, &format!("reclock {}", id))
            .await
            .context("error opening persist shard")?;

        let (since, upper) = (read_handle.since(), write_handle.upper().clone());

        assert!(
            PartialOrder::less_equal(since, &as_of),
            "invalid as_of: as_of({as_of:?}) < since({since:?}), \
            source {id}, \
            remap_shard: {}",
            metadata.remap_shard
        );

        assert!(
            as_of.elements() == [mz_repr::Timestamp::minimum()]
                || PartialOrder::less_than(&as_of, &upper),
            "invalid as_of: upper({upper:?}) <= as_of({as_of:?})",
        );

        let listener = read_handle
            .clone(&format!("reclock::listener {}", id))
            .await
            .listen(as_of.clone())
            .await
            .expect("since <= as_of asserted");

        tracing::info!(
            ?since,
            ?as_of,
            ?upper,
            "{operator}({id}) {worker_id}/{worker_count} initializing PersistHandle"
        );

        let events = futures::stream::unfold(listener, |mut listener| async move {
            let events = futures::stream::iter(listener.next().await);
            Some((events, listener))
        })
        .flatten()
        .boxed_local();

        Ok(Self {
            read_handle,
            events,
            write_handle,
            as_of,
            snapshot_produced: false,
            pending_batch: vec![],
            synthesize_partion_minimums: !partitioned_source,
            partitioned_source,
        })
    }
}

#[async_trait::async_trait(?Send)]
impl RemapHandle for PersistHandle {
    type FromTime = Partitioned<PartitionId, MzOffset>;
    type IntoTime = mz_repr::Timestamp;

    async fn compare_and_append(
        &mut self,
        updates: Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        upper: Antichain<Self::IntoTime>,
        new_upper: Antichain<Self::IntoTime>,
    ) -> Result<(), Upper<Self::IntoTime>> {
        let row_updates: Vec<_> = updates
            .into_iter()
            .filter_map(|(partitioned, ts, diff)| {
                // If we are not dealing with a partitioned source and the
                // confusingly-named partition() function is none, it means the
                // underlying partition is an interval, which is serialized a
                // range. We do not encode ranges in non-partitioned sources, so
                // drop it on the floor.
                //
                // Not that it is because of this that `Self::next` has to act
                // as if it's read intervals, though it hasn't.
                if !self.partitioned_source && partitioned.partition().is_none() {
                    None
                } else {
                    Some(((SourceData::from(partitioned), ()), ts, diff))
                }
            })
            .collect();

        match self
            .write_handle
            .compare_and_append(&row_updates, upper, new_upper)
            .await
        {
            Ok(result) => return result,
            Err(invalid_use) => panic!("compare_and_append failed: {invalid_use}"),
        }
    }

    async fn next(
        &mut self,
    ) -> Option<(
        Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        Antichain<Self::IntoTime>,
    )> {
        if !std::mem::replace(&mut self.snapshot_produced, true) {
            for ((update, _), ts, diff) in self
                .read_handle
                .snapshot_and_fetch(self.as_of.clone())
                .await
                .expect("local since is not beyond read handle's since")
            {
                let binding = update.expect("invalid row");
                let binding = Self::FromTime::try_from(binding).expect("invalid binding");
                assert!(
                    binding.partition().is_some() || self.partitioned_source,
                    "invalid binding stored in persist {:?}",
                    binding
                );
                self.pending_batch.push((binding, ts, diff));
            }

            // Partitionless collections don't store FromTime::minimum() values
            // in their remap collections, so we have to pretend that we read it
            // on initialization.
            if !self.partitioned_source {
                self.pending_batch.push((
                    Self::FromTime::minimum(),
                    *self.as_of.as_option().unwrap(),
                    1,
                ));
            }
        }

        while let Some(event) = self.events.next().await {
            match event {
                ListenEvent::Progress(new_upper) => {
                    // Peel off a batch of pending data
                    let mut native_frontier_updates = vec![];

                    self.pending_batch.retain(|(binding, ts, diff)| {
                        if !new_upper.less_equal(ts) {
                            native_frontier_updates.push((binding.clone(), *ts, *diff));
                            false
                        } else {
                            true
                        }
                    });

                    if self.synthesize_partion_minimums
                        && native_frontier_updates
                            .iter()
                            .any(|(p, _ts, _diff)| p != &Self::FromTime::minimum())
                    {
                        // Pretend that we read the data that separates
                        // `Self::FromTime::minimum()` into a domain-covering
                        // set featuring a point, even though that data is never
                        // serialized in cases where
                        // `synthesize_partion_minimums` is true on boot.
                        self.synthesize_partion_minimums = false;
                        let min_ts = native_frontier_updates
                            .iter()
                            .filter_map(|(p, ts, _)| {
                                if p == &Self::FromTime::minimum() {
                                    None
                                } else {
                                    Some(*ts)
                                }
                            })
                            .min()
                            .expect("must have > 0 elements != FromTime::minimum");
                        native_frontier_updates.extend([
                            (
                                Partitioned::with_range(
                                    None,
                                    Some(PartitionId::None),
                                    MzOffset::from(0),
                                ),
                                min_ts,
                                1,
                            ),
                            (
                                Partitioned::with_range(
                                    Some(PartitionId::None),
                                    None,
                                    MzOffset::from(0),
                                ),
                                min_ts,
                                1,
                            ),
                            (Self::FromTime::minimum(), min_ts, -1),
                        ]);
                    }

                    return Some((native_frontier_updates, new_upper));
                }
                ListenEvent::Updates(msgs) => {
                    for ((update, _), ts, diff) in msgs {
                        let binding = Self::FromTime::try_from(update.expect("invalid row"))
                            .expect("invalid binding");
                        self.pending_batch.push((binding, ts, diff));
                    }
                }
            }
        }
        None
    }

    async fn compact(&mut self, new_since: Antichain<Self::IntoTime>) {
        if !PartialOrder::less_equal(self.read_handle.since(), &new_since) {
            panic!(
                "ReclockFollower: `new_since` ({:?}) is not beyond \
                `self.since` ({:?}).",
                new_since,
                self.read_handle.since(),
            );
        }
        self.read_handle.maybe_downgrade_since(&new_since).await;
    }

    fn upper(&self) -> &Antichain<Self::IntoTime> {
        self.write_handle.upper()
    }

    fn since(&self) -> &Antichain<Self::IntoTime> {
        self.read_handle.since()
    }
}
