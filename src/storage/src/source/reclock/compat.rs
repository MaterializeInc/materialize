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
use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use futures::{stream::LocalBoxStream, StreamExt};
use itertools::Itertools;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_expr::PartitionId;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::{ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::sources::{MzOffset, SourceData};
use mz_timely_util::order::Partitioned;

use crate::source::antichain::{MutableOffsetAntichain, OffsetAntichain};
use crate::source::reclock::{
    ReclockBatch, ReclockError, ReclockFollower, ReclockOperator, RemapHandle,
};

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
    upper: Antichain<mz_repr::Timestamp>,
    as_of: Antichain<mz_repr::Timestamp>,
    pending_batch: Vec<((PartitionId, MzOffset), mz_repr::Timestamp, Diff)>,
    native_source_upper: MutableAntichain<Partitioned<PartitionId, MzOffset>>,
    compat_source_upper: MutableOffsetAntichain,
    minimum_produced: bool,
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
    ) -> anyhow::Result<Self> {
        let mut persist_clients = persist_clients.lock().await;
        let persist_client = persist_clients
            .open(metadata.persist_location)
            .await
            .context("error creating persist client")?;
        drop(persist_clients);

        let (write_handle, read_handle) = persist_client
            .open(metadata.remap_shard)
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
            .clone()
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
            upper: Antichain::from_elem(mz_repr::Timestamp::minimum()),
            pending_batch: vec![],
            native_source_upper: MutableAntichain::new(),
            compat_source_upper: MutableOffsetAntichain::new(),
            minimum_produced: false,
        })
    }
}

/// Packs a binding into a Row.
///
/// A binding of None partition is encoded as a single datum containing the offset.
///
/// A binding of a Kafka partition is encoded as the partition datum followed by the offset datum.
fn pack_binding(pid: PartitionId, offset: MzOffset) -> SourceData {
    let mut row = Row::with_capacity(2);
    let mut packer = row.packer();
    match pid {
        PartitionId::None => {}
        PartitionId::Kafka(pid) => packer.push(Datum::Int32(pid)),
    }
    packer.push(Datum::UInt64(offset.offset));
    SourceData(Ok(row))
}

/// Unpacks a binding from a Row
/// See documentation of [pack_binding] for the encoded format
fn unpack_binding(data: SourceData) -> (PartitionId, MzOffset) {
    let row = data.0.expect("invalid binding");
    let mut datums = row.iter();
    let (pid, offset) = match (datums.next(), datums.next()) {
        (Some(Datum::Int32(p)), Some(Datum::UInt64(offset))) => (PartitionId::Kafka(p), offset),
        (Some(Datum::UInt64(offset)), None) => (PartitionId::None, offset),
        _ => panic!("invalid binding"),
    };

    (pid, MzOffset::from(offset))
}

#[async_trait::async_trait(?Send)]
impl RemapHandle for PersistHandle {
    type FromTime = Partitioned<PartitionId, MzOffset>;
    type IntoTime = mz_repr::Timestamp;

    async fn compare_and_append(
        &mut self,
        mut updates: Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        upper: Antichain<Self::IntoTime>,
        new_upper: Antichain<Self::IntoTime>,
    ) -> Result<(), Upper<Self::IntoTime>> {
        // The following section performs a translation of the native timely timestamps to the
        // progress format presented to users which at the moment is not compatible with how
        // Antichians work. A proper migration and subsequent deletion of this section will happen
        // soon.
        //
        // Now, we need to come up with the updates to the OffsetAntichain representation that is
        // already in the shard. In our state we store the latest value of the source frontier in
        // both the native format (Antichain<Partitioned<PartitionId, MzOffset>>) and the compat
        // one (OffsetAntichain).
        //
        // In order to calculate the updates to the OffsetAntichain that correspond to the change
        // in native timestamps we will accumulate the provided diffs into concrete Antichains for
        // each time that the frontier changed, convert into an OffsetAntichain, and diff those
        // with the OffsetAntichain.

        // Vector holding the result of the translation
        let mut compat_frontier_updates = vec![];

        // First, we will sort the updates by time to be able to iterate over the groups
        updates.sort_unstable_by(|a, b| a.1.cmp(&b.1));
        let mut native_frontier = self.native_source_upper.clone();
        let mut compat_frontier = self.compat_source_upper.frontier();

        // Then, we will iterate over the group of updates in time order and produce the native
        // Antichain at each point, convert it to a compat OffsetAntichain, and diff it with
        // current OffsetAntichain representation.
        for (ts, updates) in &updates.into_iter().group_by(|update| update.1) {
            native_frontier.update_iter(
                updates
                    .into_iter()
                    .map(|(src_ts, _ts, diff)| (src_ts, diff)),
            );
            let new_compat_frontier = OffsetAntichain::from(native_frontier.frontier().to_owned());

            compat_frontier_updates.extend(
                compat_frontier
                    .iter()
                    .map(|(pid, offset)| ((pid.clone(), *offset), ts, -1)),
            );
            compat_frontier_updates.extend(
                new_compat_frontier
                    .iter()
                    .map(|(pid, offset)| ((pid.clone(), *offset), ts, 1)),
            );

            compat_frontier = new_compat_frontier;
        }
        // Then, consolidate the compat updates and we're done
        consolidation::consolidate_updates(&mut compat_frontier_updates);

        // And finally convert into rows and attempt to append to the shard
        let mut row_updates = vec![];
        for ((pid, offset), ts, diff) in compat_frontier_updates {
            row_updates.push((pack_binding(pid, offset), ts, diff));
        }

        loop {
            let updates = row_updates
                .iter()
                .map(|(data, time, diff)| ((data, ()), time, diff));
            let upper = upper.clone();
            let new_upper = new_upper.clone();
            match self
                .write_handle
                .compare_and_append(updates, upper, new_upper)
                .await
            {
                Ok(Ok(result)) => return result,
                Ok(Err(invalid_use)) => panic!("compare_and_append failed: {invalid_use}"),
                // An external error means that the operation might have suceeded or failed but we
                // don't know. In either case it is safe to retry because:
                // * If it succeeded, then on retry we'll get an `Upper(_)` error as if some other
                //   process raced us (but we actually raced ourselves). Since the operator is
                //   built to handle concurrent instances of itself this safe to do and will
                //   correctly re-sync its state. Once it resyncs we'll re-enter `mint` and notice
                //   that there are no updates to add (because we just added them and don't know
                //   it!) and the reclock operation will proceed normally.
                // * If it failed, then we'll succeed on retry and proceed normally.
                Err(external_err) => {
                    tracing::debug!("compare_and_append failed: {external_err}");
                    continue;
                }
            }
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
                let binding = unpack_binding(update.expect("invalid row"));
                self.pending_batch.push((binding, ts, diff));
            }
        }
        while let Some(event) = self.events.next().await {
            match event {
                ListenEvent::Progress(new_upper) => {
                    // Now it's the time to peel off a batch of pending data
                    let mut updates = vec![];
                    self.pending_batch.retain(|(binding, ts, diff)| {
                        if !new_upper.less_equal(ts) {
                            updates.push((binding.clone(), *ts, *diff));
                            false
                        } else {
                            true
                        }
                    });

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
                    if !std::mem::replace(&mut self.minimum_produced, true) {
                        let mut first_ts = mz_repr::Timestamp::minimum();
                        first_ts.advance_by(self.as_of.borrow());
                        native_frontier_updates.push((Partitioned::minimum(), first_ts, 1));
                    }

                    // Then, we will iterate over the group of updates in time order and produce
                    // the OffsetAntichain at each point, convert it to a normal Antichain, and
                    // diff it with current Antichain representation.
                    for (ts, updates) in &updates.into_iter().group_by(|update| update.1) {
                        let prev_native_frontier =
                            Antichain::from(self.compat_source_upper.frontier());
                        native_frontier_updates.extend(
                            prev_native_frontier
                                .into_iter()
                                .map(|src_ts| (src_ts, ts, -1)),
                        );

                        self.compat_source_upper.update_iter(
                            updates
                                .into_iter()
                                .map(|(binding, _ts, diff)| (binding, diff)),
                        );
                        let new_native_frontier =
                            Antichain::from(self.compat_source_upper.frontier());

                        native_frontier_updates.extend(
                            new_native_frontier
                                .into_iter()
                                .map(|src_ts| (src_ts, ts, 1)),
                        );
                    }
                    // Then, consolidate the native updates and we're done
                    consolidation::consolidate_updates(&mut native_frontier_updates);

                    // Finally, apply the updates to our local view of the native frontier
                    self.native_source_upper.update_iter(
                        native_frontier_updates
                            .iter()
                            .map(|(src_ts, _ts, diff)| (src_ts.clone(), *diff)),
                    );
                    self.upper = new_upper.clone();

                    return Some((native_frontier_updates, new_upper));
                }
                ListenEvent::Updates(msgs) => {
                    for ((update, _), ts, diff) in msgs {
                        let binding = unpack_binding(update.expect("invalid row"));
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
