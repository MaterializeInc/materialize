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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use differential_dataflow::lattice::Lattice;
use fail::fail_point;
use futures::stream::LocalBoxStream;
use futures::StreamExt;
use mz_ore::vec::VecExt;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, RelationDesc};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::sources::{SourceData, SourceTimestamp};
use mz_storage_client::util::remap_handle::{RemapHandle, RemapHandleReader};
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;

/// A handle to a persist shard that stores remap bindings
pub struct PersistHandle<FromTime: SourceTimestamp, IntoTime: Timestamp + Lattice + Codec64> {
    events: LocalBoxStream<
        'static,
        ListenEvent<
            IntoTime,
            (
                (Result<SourceData, String>, Result<(), String>),
                IntoTime,
                Diff,
            ),
        >,
    >,
    write_handle: WriteHandle<SourceData, (), IntoTime, Diff>,
    pending_batch: Vec<(FromTime, IntoTime, Diff)>,
    // Reports `self`'s write frontier.
    shared_write_frontier: Rc<RefCell<Antichain<IntoTime>>>,
}

impl<FromTime: Timestamp, IntoTime: Timestamp> PersistHandle<FromTime, IntoTime>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + Codec64,
{
    pub async fn new(
        persist_clients: Arc<PersistClientCache>,
        metadata: CollectionMetadata,
        as_of: Antichain<IntoTime>,
        shared_write_frontier: Rc<RefCell<Antichain<IntoTime>>>,
        // additional information to improve logging
        id: GlobalId,
        operator: &str,
        worker_id: usize,
        worker_count: usize,
        // Must match the `FromTime`. Ideally we would be able to discover this
        // from `SourceTimestamp`, but each source would need a specific `SourceTimestamp`
        // implementation, as they do not share remap `RelationDesc`'s (columns names
        // are different).
        //
        // TODO(guswynn): use the type-system to prevent misuse here.
        remap_relation_desc: RelationDesc,
        remap_collection_id: GlobalId,
    ) -> anyhow::Result<Self> {
        let remap_shard = metadata.remap_shard.ok_or_else(|| {
            anyhow!("cannot create remap PersistHandle for collection without remap shard")
        })?;

        let persist_client = persist_clients
            .open(metadata.persist_location.clone())
            .await
            .context("error creating persist client")?;

        let (write_handle, mut read_handle) = persist_client
            .open(
                remap_shard,
                Arc::new(remap_relation_desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: remap_collection_id.to_string(),
                    handle_purpose: format!("reclock for {}", id),
                },
            )
            .await
            .context("error opening persist shard")?;

        let upper = write_handle.upper();
        // We want a leased reader because elsewhere in the code the `as_of`
        // time may also be determined by another `ReadHandle`, and the pair of
        // them offer the invariant that we need (that the `as_of` if <= this
        // `since`). Using a `SinceHandle` here does not offer the same
        // invariant when paired with a `ReadHandle`.
        let since = read_handle.since();

        // Allow manually simulating the scenario where the since of the remap
        // shard has advanced too far.
        fail_point!("invalid_remap_as_of");
        assert!(
            PartialOrder::less_equal(since, &as_of),
            "invalid as_of: as_of({as_of:?}) < since({since:?}), \
            source {id}, \
            remap_shard: {:?}",
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
                let events = stream::iter(listener.fetch_next().await);
                Some((events, listener))
            })
            .flatten();

            snapshot.chain(listen_stream)
        })
        .flatten()
        .boxed_local();

        Ok(Self {
            events,
            write_handle,
            pending_batch: vec![],
            shared_write_frontier,
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
            .compare_and_append(row_updates, upper, new_upper.clone())
            .await
        {
            Ok(result) => {
                *self.shared_write_frontier.borrow_mut() = new_upper;
                return result;
            }
            Err(invalid_use) => panic!("compare_and_append failed: {invalid_use}"),
        }
    }

    fn upper(&self) -> &Antichain<Self::IntoTime> {
        self.write_handle.upper()
    }
}
