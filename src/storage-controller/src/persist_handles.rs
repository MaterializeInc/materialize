// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tokio tasks (and support machinery) for dealing with the persist handles
//! that the storage controller needs to hold.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::fmt::Write;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::ShardId;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, TimestampManipulation};
use mz_storage_client::client::{TableData, Update};
use mz_storage_types::controller::{InvalidUpper, TxnsCodecRow};
use mz_storage_types::sources::SourceData;
use mz_txn_wal::txns::{Tidy, TxnsHandle};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::{debug, info_span, Instrument, Span};

use crate::{PersistEpoch, StorageError};

mod read_only_table_worker;

#[derive(Debug, Clone)]
pub struct PersistTableWriteWorker<T: Timestamp + Lattice + Codec64 + TimestampManipulation> {
    inner: Arc<PersistTableWriteWorkerInner<T>>,
}

/// Commands for [PersistTableWriteWorker].
#[derive(Debug)]
enum PersistTableWriteCmd<T: Timestamp + Lattice + Codec64> {
    Register(
        T,
        Vec<(GlobalId, WriteHandle<SourceData, (), T, Diff>)>,
        tokio::sync::oneshot::Sender<()>,
    ),
    Update {
        /// Existing collection for the table.
        existing_collection: GlobalId,
        /// New collection we'll emit writes to.
        new_collection: GlobalId,
        /// Timestamp to forget the original handle at.
        forget_ts: T,
        /// Timestamp to register the new handle at.
        register_ts: T,
        /// New write handle to register.
        handle: WriteHandle<SourceData, (), T, Diff>,
        /// Notifies us when the handle has been updated.
        tx: oneshot::Sender<()>,
    },
    DropHandles {
        forget_ts: T,
        /// Tables that we want to drop our handle for.
        ids: Vec<GlobalId>,
        /// Notifies us when all resources have been cleaned up.
        tx: oneshot::Sender<()>,
    },
    Append {
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TableData>)>,
        tx: tokio::sync::oneshot::Sender<Result<(), StorageError<T>>>,
    },
    Shutdown,
}

impl<T: Timestamp + Lattice + Codec64> PersistTableWriteCmd<T> {
    fn name(&self) -> &'static str {
        match self {
            PersistTableWriteCmd::Register(_, _, _) => "PersistTableWriteCmd::Register",
            PersistTableWriteCmd::Update { .. } => "PersistTableWriteCmd::Update",
            PersistTableWriteCmd::DropHandles { .. } => "PersistTableWriteCmd::DropHandle",
            PersistTableWriteCmd::Append { .. } => "PersistTableWriteCmd::Append",
            PersistTableWriteCmd::Shutdown => "PersistTableWriteCmd::Shutdown",
        }
    }
}

async fn append_work<T2: Timestamp + Lattice + Codec64 + Sync>(
    write_handles: &mut BTreeMap<GlobalId, WriteHandle<SourceData, (), T2, Diff>>,
    mut commands: BTreeMap<
        GlobalId,
        (tracing::Span, Vec<Update<T2>>, Antichain<T2>, Antichain<T2>),
    >,
) -> Result<(), Vec<(GlobalId, Antichain<T2>)>> {
    let futs = FuturesUnordered::new();

    // We cannot iterate through the updates and then set off a persist call
    // on the write handle because we cannot mutably borrow the write handle
    // multiple times.
    //
    // Instead, we first group the update by ID above and then iterate
    // through all available write handles and see if there are any updates
    // for it. If yes, we send them all in one go.
    for (id, write) in write_handles.iter_mut() {
        if let Some((span, updates, expected_upper, new_upper)) = commands.remove(id) {
            let updates = updates
                .into_iter()
                .map(|u| ((SourceData(Ok(u.row)), ()), u.timestamp, u.diff));

            futs.push(async move {
                write
                    .compare_and_append(updates.clone(), expected_upper.clone(), new_upper.clone())
                    .instrument(span.clone())
                    .await
                    .expect("cannot append updates")
                    .or_else(|upper_mismatch| Err((*id, upper_mismatch.current)))?;

                Ok::<_, (GlobalId, Antichain<T2>)>((*id, new_upper))
            })
        }
    }

    // Ensure all futures run to completion, and track status of each of them individually
    let (_new_uppers, failed_appends): (Vec<_>, Vec<_>) = futs
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .partition_result();

    if failed_appends.is_empty() {
        Ok(())
    } else {
        Err(failed_appends)
    }
}

impl<T: Timestamp + Lattice + Codec64 + TimestampManipulation> PersistTableWriteWorker<T> {
    /// Create a new read-only table worker that continually bumps the upper of
    /// it's tables. It is expected that we only register migrated builtin
    /// tables, that cannot yet be registered in the txns system in read-only
    /// mode.
    ///
    /// This takes a [WriteHandle] for the txns shard so that it can follow the
    /// upper and continually bump the upper of registered tables to follow the
    /// upper of the txns shard.
    pub(crate) fn new_read_only_mode(txns_handle: WriteHandle<SourceData, (), T, Diff>) -> Self {
        let (tx, rx) =
            tokio::sync::mpsc::unbounded_channel::<(tracing::Span, PersistTableWriteCmd<T>)>();
        mz_ore::task::spawn(
            || "PersistTableWriteWorker",
            read_only_table_worker::read_only_mode_table_worker(rx, txns_handle),
        );
        Self {
            inner: Arc::new(PersistTableWriteWorkerInner::new(tx)),
        }
    }

    pub(crate) fn new_txns(
        txns: TxnsHandle<SourceData, (), T, i64, PersistEpoch, TxnsCodecRow>,
    ) -> Self {
        let (tx, rx) =
            tokio::sync::mpsc::unbounded_channel::<(tracing::Span, PersistTableWriteCmd<T>)>();
        mz_ore::task::spawn(|| "PersistTableWriteWorker", async move {
            let mut worker = TxnsTableWorker {
                txns,
                write_handles: BTreeMap::new(),
                tidy: Tidy::default(),
            };
            worker.run(rx).await
        });
        Self {
            inner: Arc::new(PersistTableWriteWorkerInner::new(tx)),
        }
    }

    pub(crate) fn register(
        &self,
        register_ts: T,
        ids_handles: Vec<(GlobalId, WriteHandle<SourceData, (), T, Diff>)>,
    ) -> tokio::sync::oneshot::Receiver<()> {
        // We expect this to be awaited, so keep the span connected.
        let span = info_span!("PersistTableWriteCmd::Register");
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cmd = PersistTableWriteCmd::Register(register_ts, ids_handles, tx);
        self.inner.send_with_span(span, cmd);
        rx
    }

    /// Update the existing write handle associated with `id` to `write_handle`.
    ///
    /// Note that this should only be called when updating a write handle; to
    /// initially associate an `id` to a write handle, use [`Self::register`].
    ///
    /// # Panics
    /// - If `id` is not currently associated with any write handle.
    #[allow(dead_code)]
    pub(crate) fn update(
        &self,
        existing_collection: GlobalId,
        new_collection: GlobalId,
        forget_ts: T,
        register_ts: T,
        handle: WriteHandle<SourceData, (), T, Diff>,
    ) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.send(PersistTableWriteCmd::Update {
            existing_collection,
            new_collection,
            forget_ts,
            register_ts,
            handle,
            tx,
        });
        rx
    }

    pub(crate) fn append(
        &self,
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TableData>)>,
    ) -> tokio::sync::oneshot::Receiver<Result<(), StorageError<T>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if updates.is_empty() {
            tx.send(Ok(()))
                .expect("rx has not been dropped at this point");
            rx
        } else {
            self.send(PersistTableWriteCmd::Append {
                write_ts,
                advance_to,
                updates,
                tx,
            });
            rx
        }
    }

    /// Drops the handles associated with `ids` from this worker.
    ///
    /// Note that this does not perform any other cleanup, such as finalizing
    /// the handle's shard.
    pub(crate) fn drop_handles(&self, ids: Vec<GlobalId>, forget_ts: T) -> BoxFuture<'static, ()> {
        let (tx, rx) = oneshot::channel();
        self.send(PersistTableWriteCmd::DropHandles { forget_ts, ids, tx });
        Box::pin(rx.map(|_| ()))
    }

    fn send(&self, cmd: PersistTableWriteCmd<T>) {
        self.inner.send(cmd);
    }
}

struct TxnsTableWorker<T: Timestamp + Lattice + TotalOrder + Codec64> {
    txns: TxnsHandle<SourceData, (), T, i64, PersistEpoch, TxnsCodecRow>,
    write_handles: BTreeMap<GlobalId, ShardId>,
    tidy: Tidy,
}

impl<T: Timestamp + Lattice + Codec64 + TimestampManipulation> TxnsTableWorker<T> {
    async fn run(
        &mut self,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<(Span, PersistTableWriteCmd<T>)>,
    ) {
        while let Some((span, command)) = rx.recv().await {
            match command {
                PersistTableWriteCmd::Register(register_ts, ids_handles, tx) => {
                    self.register(register_ts, ids_handles)
                        .instrument(span)
                        .await;
                    // We don't care if our waiter has gone away.
                    let _ = tx.send(());
                }
                PersistTableWriteCmd::Update {
                    existing_collection,
                    new_collection,
                    forget_ts,
                    register_ts,
                    handle,
                    tx,
                } => {
                    async {
                        self.drop_handles(vec![existing_collection], forget_ts)
                            .await;
                        self.register(register_ts, vec![(new_collection, handle)])
                            .await;
                    }
                    .instrument(span)
                    .await;
                    // We don't care if our waiter has gone away.
                    let _ = tx.send(());
                }
                PersistTableWriteCmd::DropHandles { forget_ts, ids, tx } => {
                    self.drop_handles(ids, forget_ts).instrument(span).await;
                    // We don't care if our waiter has gone away.
                    let _ = tx.send(());
                }
                PersistTableWriteCmd::Append {
                    write_ts,
                    advance_to,
                    updates,
                    tx,
                } => {
                    self.append(write_ts, advance_to, updates, tx)
                        .instrument(span)
                        .await
                }
                PersistTableWriteCmd::Shutdown => {
                    tracing::info!("PersistTableWriteWorker shutting down via command");
                    return;
                }
            }
        }

        tracing::info!("PersistTableWriteWorker shutting down via input exhaustion");
    }

    async fn register(
        &mut self,
        register_ts: T,
        mut ids_handles: Vec<(GlobalId, WriteHandle<SourceData, (), T, i64>)>,
    ) {
        // As tables evolve (e.g. columns are added) we treat the older versions as
        // "views" on the later versions. While it's not required, it's easier to reason
        // about table registration if we do it in GlobalId order.
        ids_handles.sort_unstable_by_key(|(gid, _handle)| *gid);

        for (id, write_handle) in ids_handles.iter() {
            debug!(
                "tables register {} {:.9}",
                id,
                write_handle.shard_id().to_string()
            );
            let previous = self.write_handles.insert(*id, write_handle.shard_id());
            if previous.is_some() {
                panic!("already registered a WriteHandle for collection {:?}", id);
            }
        }

        // Registering also advances the logical upper of all shards in the txns set.
        let new_ids = ids_handles.iter().map(|(id, _)| *id).collect_vec();
        let handles = ids_handles.into_iter().map(|(_, handle)| handle);
        let res = self.txns.register(register_ts.clone(), handles).await;
        match res {
            Ok(tidy) => {
                self.tidy.merge(tidy);
            }
            Err(current) => {
                panic!(
                    "cannot register {:?} at {:?} because txns is at {:?}",
                    new_ids, register_ts, current
                );
            }
        }
    }

    async fn drop_handles(&mut self, ids: Vec<GlobalId>, forget_ts: T) {
        tracing::info!(?ids, "drop tables");
        let data_ids = ids
            .iter()
            // n.b. this should only remove the handle from the persist
            // worker and not take any additional action such as closing
            // the shard it's connected to because dataflows might still
            // be using it.
            .filter_map(|id| self.write_handles.remove(id))
            .collect::<BTreeSet<_>>();
        if !data_ids.is_empty() {
            match self.txns.forget(forget_ts.clone(), data_ids.clone()).await {
                Ok(tidy) => {
                    self.tidy.merge(tidy);
                }
                Err(current) => {
                    panic!(
                        "cannot forget {:?} at {:?} because txns is at {:?}",
                        ids, forget_ts, current
                    );
                }
            }
        }
    }

    async fn append(
        &mut self,
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TableData>)>,
        tx: tokio::sync::oneshot::Sender<Result<(), StorageError<T>>>,
    ) {
        debug!(
            "tables append timestamp={:?} advance_to={:?} len={} ids={:?}{}",
            write_ts,
            advance_to,
            updates.iter().flat_map(|(_, x)| x).count(),
            updates
                .iter()
                .map(|(x, _)| x.to_string())
                .collect::<BTreeSet<_>>(),
            updates.iter().filter(|(_, v)| !v.is_empty()).fold(
                String::new(),
                |mut output, (k, v)| {
                    let _ = write!(output, "\n  {}: {:?}", k, v.first());
                    output
                }
            )
        );
        // TODO: txn-wal doesn't take an advance_to yet, it uses
        // timestamp.step_forward. This is the same in all cases, so just assert that
        // for now. Note that this uses the _persist_ StepForward, not the
        // TimestampManipulation one (the impls are the same) because that's what
        // txn-wal uses.
        assert_eq!(
            advance_to,
            mz_persist_types::StepForward::step_forward(&write_ts)
        );

        let mut txn = self.txns.begin();
        for (id, updates) in updates {
            let Some(data_id) = self.write_handles.get(&id) else {
                // HACK: When creating a table we get an append that includes it
                // before it's been registered. When this happens there are no
                // updates, so it's ~fine to ignore it.
                assert!(
                    updates.iter().all(|u| u.is_empty()),
                    "{}: {:?}",
                    id,
                    updates
                );
                continue;
            };
            for update in updates {
                match update {
                    TableData::Rows(updates) => {
                        for (row, diff) in updates {
                            let () = txn.write(data_id, SourceData(Ok(row)), (), diff).await;
                        }
                    }
                    TableData::Batches(batches) => {
                        for batch in batches {
                            let () = txn.write_batch(data_id, batch);
                        }
                    }
                }
            }
        }
        // Sneak in any txns shard tidying from previous commits.
        txn.tidy(std::mem::take(&mut self.tidy));
        let txn_res = txn.commit_at(&mut self.txns, write_ts.clone()).await;
        let response = match txn_res {
            Ok(apply) => {
                // TODO: Do the applying in a background task. This will be a
                // significant INSERT latency performance win.
                debug!("applying {:?}", apply);
                let tidy = apply.apply(&mut self.txns).await;
                self.tidy.merge(tidy);

                // We don't serve any reads out of this TxnsHandle, so go ahead
                // and compact as aggressively as we can (i.e. to the time we
                // just wrote).
                let () = self.txns.compact_to(write_ts).await;

                Ok(())
            }
            Err(current) => {
                self.tidy.merge(txn.take_tidy());
                debug!(
                    "unable to commit txn at {:?} current={:?}",
                    write_ts, current
                );
                Err(StorageError::InvalidUppers(
                    self.write_handles
                        .keys()
                        .copied()
                        .map(|id| InvalidUpper {
                            id,
                            current_upper: Antichain::from_elem(current.clone()),
                        })
                        .collect(),
                ))
            }
        };
        // It is not an error for the other end to hang up.
        let _ = tx.send(response);
    }
}

/// Contains the components necessary for sending commands to a `PersistTableWriteWorker`.
///
/// When `Drop`-ed sends a shutdown command, as such this should _never_ implement `Clone` because
/// if one clone is dropped, the other clones will be unable to send commands. If you need this
/// to be `Clone`-able, wrap it in an `Arc` or `Rc` first.
///
/// #[derive(Clone)] <-- do not do this.
///
#[derive(Debug)]
struct PersistTableWriteWorkerInner<T: Timestamp + Lattice + Codec64 + TimestampManipulation> {
    /// Sending side of a channel that we can use to send commands.
    tx: UnboundedSender<(tracing::Span, PersistTableWriteCmd<T>)>,
}

impl<T> Drop for PersistTableWriteWorkerInner<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    fn drop(&mut self) {
        self.send(PersistTableWriteCmd::Shutdown);
        // TODO: Can't easily block on shutdown occurring.
    }
}

impl<T> PersistTableWriteWorkerInner<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    fn new(tx: UnboundedSender<(tracing::Span, PersistTableWriteCmd<T>)>) -> Self {
        PersistTableWriteWorkerInner { tx }
    }

    fn send(&self, cmd: PersistTableWriteCmd<T>) {
        let span =
            info_span!(parent: None, "PersistTableWriteWorkerInner::send", otel.name = cmd.name());
        OpenTelemetryContext::obtain().attach_as_parent_to(&span);
        self.send_with_span(span, cmd)
    }

    fn send_with_span(&self, span: Span, cmd: PersistTableWriteCmd<T>) {
        match self.tx.send((span, cmd)) {
            Ok(()) => (), // All good!
            Err(e) => {
                tracing::trace!("could not forward command: {:?}", e);
            }
        }
    }
}
