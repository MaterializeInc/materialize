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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
use mz_storage_client::client::{TimestamplessUpdate, Update};
use mz_storage_types::controller::{InvalidUpper, TxnsCodecRow};
use mz_storage_types::sources::SourceData;
use mz_txn_wal::txns::{Tidy, TxnsHandle};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::{debug, info_span, Instrument, Span};

use crate::{PersistEpoch, StorageError};

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
        /// Table to update.
        table_id: GlobalId,
        /// Timestamp to apply the update at.
        update_ts: T,
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
        updates: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
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

async fn append_work<T2: Timestamp + Lattice + Codec64>(
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
    pub(crate) fn new_read_only_mode() -> Self {
        let (tx, rx) =
            tokio::sync::mpsc::unbounded_channel::<(tracing::Span, PersistTableWriteCmd<T>)>();
        mz_ore::task::spawn(
            || "PersistTableWriteWorker",
            read_only_mode_table_worker(rx),
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
        table_id: GlobalId,
        update_ts: T,
        handle: WriteHandle<SourceData, (), T, Diff>,
    ) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.send(PersistTableWriteCmd::Update {
            table_id,
            update_ts,
            handle,
            tx,
        });
        rx
    }

    pub(crate) fn append(
        &self,
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
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
                    table_id,
                    update_ts,
                    handle,
                    tx,
                } => {
                    async {
                        let forget_ts = update_ts;
                        let register_ts = mz_persist_types::StepForward::step_forward(&forget_ts);

                        self.drop_handles(vec![table_id], forget_ts).await;
                        self.register(register_ts, vec![(table_id, handle)]).await;
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
        ids_handles: Vec<(GlobalId, WriteHandle<SourceData, (), T, i64>)>,
    ) {
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
            .collect::<Vec<_>>();
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
        updates: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
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
                assert!(updates.is_empty(), "{}: {:?}", id, updates);
                continue;
            };
            for update in updates {
                let () = txn
                    .write(data_id, SourceData(Ok(update.row)), (), update.diff)
                    .await;
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

/// Handles table updates in read only mode.
///
/// In read only mode, we write to tables outside of the txn-wal system. This is
/// a gross hack, but it is a quick fix to allow us to perform migrations of the
/// built-in tables in the new generation during a deployment. We need to write
/// to the new shards for migrated built-in tables so that dataflows that depend
/// on those tables can catch up, but we don't want to register them into the
/// existing txn-wal shard, as that would mutate the state of the old generation
/// while it's still running. We could instead create a new txn shard in the new
/// generation for *just* system catalog tables, but then we'd have to do a
/// complicated dance to move the system catalog tables back to the original txn
/// shard during promotion, without ever losing track of a shard or registering
/// it in two txn shards simultaneously.
///
/// This code is a nearly line-for-line reintroduction of the code that managed
/// writing to tables before the txn-wal system. This code can (again) be
/// deleted when we switch to using native persist schema migrations to perform
/// mgirations of built-in tables.
async fn read_only_mode_table_worker<T: Timestamp + Lattice + Codec64 + TimestampManipulation>(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(Span, PersistTableWriteCmd<T>)>,
) {
    let mut write_handles = BTreeMap::<GlobalId, WriteHandle<SourceData, (), T, Diff>>::new();

    let mut shutdown = false;
    while let Some(cmd) = rx.recv().await {
        // Peel off all available commands.
        // We do this in case we can consolidate commands.
        // It would be surprising to receive multiple concurrent `Append` commands,
        // but we might receive multiple *empty* `Append` commands.
        let mut commands = VecDeque::new();
        commands.push_back(cmd);
        while let Ok(cmd) = rx.try_recv() {
            commands.push_back(cmd);
        }

        // Accumulated updates and upper frontier.
        let mut all_updates = BTreeMap::default();
        let mut all_responses = Vec::default();

        while let Some((span, command)) = commands.pop_front() {
            match command {
                PersistTableWriteCmd::Register(_register_ts, ids_handles, tx) => {
                    for (id, write_handle) in ids_handles {
                        let previous = write_handles.insert(id, write_handle);
                        if previous.is_some() {
                            panic!("already registered a WriteHandle for collection {:?}", id);
                        }
                    }
                    // We don't care if our waiter has gone away.
                    let _ = tx.send(());
                }
                PersistTableWriteCmd::Update(id, write_handle) => {
                    write_handles.insert(id, write_handle).expect(
                        "PersistTableWriteCmd::Update only valid for updating extant write handles",
                    );
                }
                PersistTableWriteCmd::DropHandles {
                    forget_ts: _,
                    ids,
                    tx,
                } => {
                    // n.b. this should only remove the
                    // handle from the persist worker and
                    // not take any additional action such
                    // as closing the shard it's connected
                    // to because dataflows might still be
                    // using it.
                    for id in ids {
                        write_handles.remove(&id);
                    }
                    // We don't care if our waiter has gone away.
                    let _ = tx.send(());
                }
                PersistTableWriteCmd::Append {
                    write_ts,
                    advance_to,
                    updates,
                    tx,
                } => {
                    let mut ids = BTreeSet::new();
                    for (id, updates_no_ts) in updates {
                        ids.insert(id);
                        let (old_span, updates, _expected_upper, old_new_upper) =
                            all_updates.entry(id).or_insert_with(|| {
                                (
                                    span.clone(),
                                    Vec::default(),
                                    Antichain::from_elem(write_ts.clone()),
                                    Antichain::from_elem(T::minimum()),
                                )
                            });

                        if old_span.id() != span.id() {
                            // Link in any spans for `Append` operations that we
                            // lump together by doing this. This is not ideal,
                            // because we only have a true tracing history for
                            // the "first" span that we process, but it's better
                            // than nothing.
                            old_span.follows_from(span.id());
                        }
                        let updates_with_ts = updates_no_ts.into_iter().map(|x| Update {
                            row: x.row,
                            timestamp: write_ts.clone(),
                            diff: x.diff,
                        });
                        updates.extend(updates_with_ts);
                        old_new_upper.join_assign(&Antichain::from_elem(advance_to.clone()));
                    }
                    all_responses.push((ids, tx));
                }
                PersistTableWriteCmd::Shutdown => shutdown = true,
            }
        }

        let result = append_work(&mut write_handles, all_updates).await;

        for (ids, response) in all_responses {
            let result = match &result {
                Err(bad_ids) => {
                    let filtered: Vec<_> = bad_ids
                        .iter()
                        .filter(|(id, _)| ids.contains(id))
                        .cloned()
                        .map(|(id, current_upper)| InvalidUpper { id, current_upper })
                        .collect();
                    if filtered.is_empty() {
                        Ok(())
                    } else {
                        Err(StorageError::InvalidUppers(filtered))
                    }
                }
                Ok(()) => Ok(()),
            };
            // It is not an error for the other end to hang up.
            let _ = response.send(result);
        }

        if shutdown {
            tracing::trace!("shutting down persist write append task");
            break;
        }
    }

    tracing::info!("PersistTableWriteWorker shutting down");
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
        let mut span =
            info_span!(parent: None, "PersistTableWriteWorkerInner::send", otel.name = cmd.name());
        OpenTelemetryContext::obtain().attach_as_parent_to(&mut span);
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

#[derive(Debug, Clone)]
pub struct PersistMonotonicWriteWorker<T: Timestamp + Lattice + Codec64 + TimestampManipulation> {
    inner: Arc<PersistMonotonicWriteWorkerInner<T>>,
}

/// Commands for [PersistMonotonicWriteWorker].
#[derive(Debug)]
enum PersistMonotonicWriteCmd<T: Timestamp + Lattice + Codec64> {
    Register(GlobalId, WriteHandle<SourceData, (), T, Diff>),
    Update(GlobalId, WriteHandle<SourceData, (), T, Diff>),
    DropHandle {
        /// Object that we want to drop our handle for.
        id: GlobalId,
        /// Notifies us when all resources have been cleaned up.
        tx: oneshot::Sender<()>,
    },
    RecentUpper(
        GlobalId,
        tokio::sync::oneshot::Sender<Result<Antichain<T>, StorageError<T>>>,
    ),
    Append(
        Vec<(GlobalId, Vec<Update<T>>, T, T)>,
        tokio::sync::oneshot::Sender<Result<(), StorageError<T>>>,
    ),
    /// Appends `Vec<TimelessUpdate>` to `GlobalId` at, essentially,
    /// `max(write_frontier, T)`.
    MonotonicAppend(
        Vec<(GlobalId, Vec<TimestamplessUpdate>, T)>,
        tokio::sync::oneshot::Sender<Result<(), StorageError<T>>>,
    ),
    Shutdown,
}

impl<T: Timestamp + Lattice + Codec64 + TimestampManipulation> PersistMonotonicWriteWorker<T> {
    pub(crate) fn new() -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(tracing::Span, _)>();

        mz_ore::task::spawn(|| "PersistMonotonicWriteWorker", async move {
            let mut write_handles =
                BTreeMap::<GlobalId, WriteHandle<SourceData, (), T, Diff>>::new();

            let mut shutdown = false;
            while let Some(cmd) = rx.recv().await {
                // Peel off all available commands.
                // We do this in case we can consolidate commands.
                // It would be surprising to receive multiple concurrent `Append` commands,
                // but we might receive multiple *empty* `Append` commands.
                let mut commands = VecDeque::new();
                commands.push_back(cmd);
                while let Ok(cmd) = rx.try_recv() {
                    commands.push_back(cmd);
                }

                // Accumulated updates and upper frontier.
                let mut all_updates = BTreeMap::default();
                let mut all_responses = Vec::default();

                while let Some((span, command)) = commands.pop_front() {
                    match command {
                        PersistMonotonicWriteCmd::Register(id, write_handle) => {
                            let previous = write_handles.insert(id, write_handle);
                            if previous.is_some() {
                                panic!("already registered a WriteHandle for collection {:?}", id);
                            }
                        }
                        PersistMonotonicWriteCmd::Update(id, write_handle) => {
                            write_handles.insert(id, write_handle).expect("PersistMonotonicWriteCmd::Update only valid for updating extant write handles");
                        }
                        PersistMonotonicWriteCmd::DropHandle { id, tx } => {
                            // n.b. this should only remove the
                            // handle from the persist worker and
                            // not take any additional action such
                            // as closing the shard it's connected
                            // to because dataflows might still be
                            // using it.
                            write_handles.remove(&id);
                            // We don't care if our listener went away.
                            let _ = tx.send(());
                        }
                        PersistMonotonicWriteCmd::RecentUpper(id, response) => {
                            let write = write_handles.get_mut(&id);

                            if let Some(write) = write {
                                let upper = write.fetch_recent_upper().await;
                                let _ = response.send(Ok(upper.clone()));
                            } else {
                                let _ = response.send(Err(StorageError::IdentifierMissing(id)));
                            }
                        }
                        PersistMonotonicWriteCmd::Append(updates, response) => {
                            let mut ids = BTreeSet::new();
                            for (id, update, expected_upper, new_upper) in updates {
                                ids.insert(id);
                                let (old_span, updates, old_expected_upper, old_new_upper) =
                                    all_updates.entry(id).or_insert_with(|| {
                                        (
                                            span.clone(),
                                            Vec::default(),
                                            Antichain::from_elem(T::minimum()),
                                            Antichain::from_elem(T::minimum()),
                                        )
                                    });

                                if old_span.id() != span.id() {
                                    // Link in any spans for `Append`
                                    // operations that we lump together by
                                    // doing this. This is not ideal,
                                    // because we only have a true tracing
                                    // history for the "first" span that we
                                    // process, but it's better than
                                    // nothing.
                                    old_span.follows_from(span.id());
                                }
                                updates.extend(update);
                                old_new_upper.join_assign(&Antichain::from_elem(new_upper));
                                old_expected_upper
                                    .join_assign(&Antichain::from_elem(expected_upper));
                            }
                            all_responses.push((ids, response));
                        }
                        PersistMonotonicWriteCmd::MonotonicAppend(updates, response) => {
                            let mut updates_outer = Vec::with_capacity(updates.len());
                            for (id, update, at_least) in updates {
                                let current_upper = write_handles[&id].upper().clone();
                                if update.is_empty() && current_upper.is_empty() {
                                    // Ignore timestamp advancement for
                                    // closed collections. TODO? Make this a
                                    // correctable error
                                    continue;
                                }

                                let current_upper = current_upper
                                    .into_option()
                                    .expect("cannot append data to closed collection");

                                let lower = if current_upper.less_than(&at_least) {
                                    at_least
                                } else {
                                    current_upper.clone()
                                };

                                let upper = TimestampManipulation::step_forward(&lower);
                                let update = update
                                    .into_iter()
                                    .map(|TimestamplessUpdate { row, diff }| Update {
                                        row,
                                        diff,
                                        timestamp: lower.clone(),
                                    })
                                    .collect::<Vec<_>>();

                                updates_outer.push((id, update, current_upper, upper));
                            }
                            commands.push_front((
                                span,
                                PersistMonotonicWriteCmd::Append(updates_outer, response),
                            ));
                        }
                        PersistMonotonicWriteCmd::Shutdown => {
                            shutdown = true;
                        }
                    }
                }

                let result = append_work(&mut write_handles, all_updates).await;

                for (ids, response) in all_responses {
                    let result = match &result {
                        Err(bad_ids) => {
                            let filtered: Vec<_> = bad_ids
                                .iter()
                                .filter(|(id, _)| ids.contains(id))
                                .cloned()
                                .map(|(id, current_upper)| InvalidUpper { id, current_upper })
                                .collect();
                            if filtered.is_empty() {
                                Ok(())
                            } else {
                                Err(StorageError::InvalidUppers(filtered))
                            }
                        }
                        Ok(()) => Ok(()),
                    };
                    // It is not an error for the other end to hang up.
                    let _ = response.send(result);
                }

                if shutdown {
                    tracing::trace!("shutting down persist write append task");
                    break;
                }
            }

            tracing::info!("PersistMonotonicWriteWorker shutting down");
        });

        Self {
            inner: Arc::new(PersistMonotonicWriteWorkerInner::new(tx)),
        }
    }

    pub(crate) fn register(
        &self,
        id: GlobalId,
        write_handle: WriteHandle<SourceData, (), T, Diff>,
    ) {
        self.send(PersistMonotonicWriteCmd::Register(id, write_handle))
    }

    /// Update the existing write handle associated with `id` to `write_handle`.
    ///
    /// Note that this should only be called when updating a write handle; to
    /// initially associate an `id` to a write handle, use [`Self::register`].
    ///
    /// # Panics
    /// - If `id` is not currently associated with any write handle.
    #[allow(dead_code)]
    pub(crate) fn update(&self, id: GlobalId, write_handle: WriteHandle<SourceData, (), T, Diff>) {
        self.send(PersistMonotonicWriteCmd::Update(id, write_handle))
    }

    /// TODO!
    pub(crate) fn recent_upper(
        &self,
        id: GlobalId,
    ) -> tokio::sync::oneshot::Receiver<Result<Antichain<T>, StorageError<T>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(PersistMonotonicWriteCmd::RecentUpper(id, tx));
        rx
    }

    /// TODO!
    pub(crate) fn compare_and_append(
        &self,
        updates: Vec<(GlobalId, Vec<Update<T>>, T, T)>,
    ) -> tokio::sync::oneshot::Receiver<Result<(), StorageError<T>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if updates.is_empty() {
            tx.send(Ok(()))
                .expect("rx has not been dropped at this point");
            rx
        } else {
            self.send(PersistMonotonicWriteCmd::Append(updates, tx));
            rx
        }
    }

    /// Appends values to collections associated with `GlobalId`, but lets
    /// the persist worker chose timestamps guaranteed to be monotonic and
    /// that the time will be at least `T`.
    ///
    /// This lets the writer influence how far forward the timestamp will be
    /// advanced, while still guaranteeing that it will advance.
    ///
    /// Note it is still possible for the append operation to fail in the
    /// face of contention from other writers.
    ///
    /// # Panics
    /// - If appending non-empty `TimelessUpdate` to closed collections
    ///   (i.e. those with empty uppers), whose uppers cannot be
    ///   monotonically increased.
    ///
    ///   Collections with empty uppers can continue receiving empty
    ///   updates, i.e. those used soley to advance collections' uppers.
    pub(crate) fn monotonic_append(
        &self,
        updates: Vec<(GlobalId, Vec<TimestamplessUpdate>, T)>,
    ) -> tokio::sync::oneshot::Receiver<Result<(), StorageError<T>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if updates.is_empty() {
            tx.send(Ok(()))
                .expect("rx has not been dropped at this point");
            rx
        } else {
            self.send(PersistMonotonicWriteCmd::MonotonicAppend(updates, tx));
            rx
        }
    }

    /// Drops the handle associated with `id` from this worker.
    ///
    /// Note that this does not perform any other cleanup, such as finalizing
    /// the handle's shard.
    pub(crate) fn drop_handle(&self, id: GlobalId) -> BoxFuture<'static, ()> {
        let (tx, rx) = oneshot::channel();
        self.send(PersistMonotonicWriteCmd::DropHandle { id, tx });
        Box::pin(rx.map(|_| ()))
    }

    fn send(&self, cmd: PersistMonotonicWriteCmd<T>) {
        self.inner.send(cmd);
    }
}

/// Contains the components necessary for sending commands to a `PersistMonotonicWriteWorker`.
///
/// When `Drop`-ed sends a shutdown command, as such this should _never_ implement `Clone` because
/// if one clone is dropped, the other clones will be unable to send commands. If you need this
/// to be `Clone`-able, wrap it in an `Arc` or `Rc` first.
///
/// #[derive(Clone)] <-- do not do this.
///
#[derive(Debug)]
struct PersistMonotonicWriteWorkerInner<T: Timestamp + Lattice + Codec64 + TimestampManipulation> {
    /// Sending side of a channel that we can use to send commands.
    tx: UnboundedSender<(tracing::Span, PersistMonotonicWriteCmd<T>)>,
}

impl<T> Drop for PersistMonotonicWriteWorkerInner<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    fn drop(&mut self) {
        self.send(PersistMonotonicWriteCmd::Shutdown);
        // TODO: Can't easily block on shutdown occurring.
    }
}

impl<T> PersistMonotonicWriteWorkerInner<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    fn new(tx: UnboundedSender<(tracing::Span, PersistMonotonicWriteCmd<T>)>) -> Self {
        PersistMonotonicWriteWorkerInner { tx }
    }

    fn send(&self, cmd: PersistMonotonicWriteCmd<T>) {
        let mut span = info_span!(parent: None, "PersistMonotonicWriteCmd::send");
        OpenTelemetryContext::obtain().attach_as_parent_to(&mut span);
        match self.tx.send((span, cmd)) {
            Ok(()) => (), // All good!
            Err(e) => {
                tracing::trace!("could not forward command: {:?}", e);
            }
        }
    }
}
