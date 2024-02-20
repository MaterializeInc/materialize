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
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::Since;
use mz_persist_client::stats::SnapshotStats;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::ShardId;
use mz_persist_txn::txn_read::DataSnapshot;
use mz_persist_txn::txns::{Tidy, TxnsHandle};
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, TimestampManipulation};
use mz_storage_client::client::{StorageResponse, TimestamplessUpdate, Update};
use mz_storage_types::controller::{PersistTxnTablesImpl, TxnsCodecRow};
use mz_storage_types::sources::SourceData;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::{debug, info_span, Instrument, Span};

use crate::{PersistEpoch, StorageError};

/// A wrapper that holds on to backing persist shards/collections that the
/// storage controller is aware of. The handles hold back the since frontier and
/// we need to downgrade them when the read capabilities change.
///
/// Internally, this has an async task and the methods for registering a handle
/// and downgrading sinces add commands to a queue that this task is working
/// off. This makes the methods non-blocking and moves the work outside the main
/// coordinator task, meaning the coordinator is spending less time waiting on
/// persist calls.
#[derive(Debug)]
pub struct PersistReadWorker<T: Timestamp + Lattice + Codec64> {
    tx: UnboundedSender<(tracing::Span, PersistReadWorkerCmd<T>)>,
}

#[derive(Debug)]
pub(crate) enum SnapshotStatsAsOf<T: Timestamp + Lattice + Codec64> {
    /// Stats for a shard with an "eager" upper (one that continually advances
    /// as time passes, even if no writes are coming in).
    Direct(Antichain<T>),
    /// Stats for a shard with a "lazy" upper (one that only physically advances
    /// in response to writes).
    Txns(DataSnapshot<T>),
}

/// Commands for [PersistReadWorker].
#[derive(Debug)]
enum PersistReadWorkerCmd<T: Timestamp + Lattice + Codec64> {
    Register(GlobalId, SinceHandle<SourceData, (), T, Diff, PersistEpoch>),
    Update(GlobalId, SinceHandle<SourceData, (), T, Diff, PersistEpoch>),
    Downgrade(BTreeMap<GlobalId, (Antichain<T>, oneshot::Sender<Result<(), ()>>)>),
    SnapshotStats(
        GlobalId,
        SnapshotStatsAsOf<T>,
        oneshot::Sender<SnapshotStatsRes>,
    ),
}

/// A newtype wrapper to hang a Debug impl off of.
pub(crate) struct SnapshotStatsRes(BoxFuture<'static, Result<SnapshotStats, StorageError>>);

impl Debug for SnapshotStatsRes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotStatsRes").finish_non_exhaustive()
    }
}

impl<T: Timestamp + Lattice + TotalOrder + Codec64> PersistReadWorker<T> {
    pub(crate) fn new() -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(tracing::Span, _)>();

        mz_ore::task::spawn(|| "PersistWorker", async move {
            let mut since_handles = BTreeMap::new();

            while let Some(cmd) = rx.recv().await {
                // Peel off all available commands.
                // This allows us to catch up if we fall behind on downgrade commands.
                let mut commands = vec![cmd];
                while let Ok(cmd) = rx.try_recv() {
                    commands.push(cmd);
                }
                // Collect all downgrade requests and apply them last.
                let mut downgrades = BTreeMap::default();

                for (span, command) in commands {
                    match command {
                        PersistReadWorkerCmd::Register(id, since_handle) => {
                            let previous = since_handles.insert(id, since_handle);
                            if previous.is_some() {
                                panic!("already registered a SinceHandle for collection {id}");
                            }
                        }
                        PersistReadWorkerCmd::Update(id, since_handle) => {
                            since_handles.insert(id, since_handle).expect("PersistReadWorkerCmd::Update only valid for updating extant since handles");
                        }
                        PersistReadWorkerCmd::Downgrade(since_frontiers) => {
                            for (id, frontier_and_notif) in since_frontiers {
                                downgrades.insert(id, (span.clone(), frontier_and_notif));
                            }
                        }
                        PersistReadWorkerCmd::SnapshotStats(id, as_of, tx) => {
                            // NB: The requested as_of could be arbitrarily far in the future. So,
                            // in order to avoid blocking the PersistReadWorker loop until it's
                            // available and the `snapshot_stats` call resolves, instead return the
                            // future to the caller and await it there.
                            let res = match since_handles.get(&id) {
                                Some(x) => {
                                    let fut: BoxFuture<
                                        'static,
                                        Result<SnapshotStats, StorageError>,
                                    > = match as_of {
                                        SnapshotStatsAsOf::Direct(as_of) => {
                                            Box::pin(x.snapshot_stats(Some(as_of)).map(move |x| {
                                                x.map_err(|_: Since<T>| {
                                                    StorageError::ReadBeforeSince(id)
                                                })
                                            }))
                                        }
                                        SnapshotStatsAsOf::Txns(data_snapshot) => Box::pin(
                                            data_snapshot.snapshot_stats(x).map(move |x| {
                                                x.map_err(|_| StorageError::ReadBeforeSince(id))
                                            }),
                                        ),
                                    };
                                    SnapshotStatsRes(fut)
                                }
                                None => SnapshotStatsRes(Box::pin(futures::future::ready(Err(
                                    StorageError::IdentifierMissing(id),
                                )))),
                            };
                            // It's fine if the listener hung up.
                            let _ = tx.send(res);
                        }
                    }
                }

                let mut futs = FuturesUnordered::new();

                for (id, (span, (since, tx))) in downgrades {
                    let Some(mut since_handle) = since_handles.remove(&id) else {
                        panic!("downgrade command for absent collection {id}");
                    };

                    futs.push(async move {
                        let epoch = since_handle.opaque().clone();

                        let result = if since.is_empty() {
                            // A shard's since reaching the empty frontier is a prereq for being
                            // able to finalize a shard, so the final downgrade should never be
                            // rate-limited.
                            Some(
                                since_handle
                                    .compare_and_downgrade_since(&epoch, (&epoch, &since))
                                    .instrument(span)
                                    .await,
                            )
                        } else {
                            since_handle
                                .maybe_compare_and_downgrade_since(&epoch, (&epoch, &since))
                                .instrument(span)
                                .await
                        };

                        if let Some(Err(other_epoch)) = &result {
                            mz_ore::halt!("fenced by envd @ {other_epoch:?}. ours = {epoch:?}");
                        }

                        // Notify listeners of the result of the downgrade.
                        let notify_result = match result {
                            None => Err(()),
                            Some(Err(_)) => Err(()),
                            Some(Ok(_)) => Ok(()),
                        };
                        let _ = tx.send(notify_result);

                        // If we're not done we put the handle back
                        if !since.is_empty() {
                            Some((id, (since_handle)))
                        } else {
                            None
                        }
                    });
                }

                while let Some(entry) = futs.next().await {
                    since_handles.extend(entry);
                }
            }
            tracing::trace!("shutting down persist since downgrade task");
        });

        Self { tx }
    }

    pub(crate) fn register(
        &self,
        id: GlobalId,
        since_handle: SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    ) {
        self.send(PersistReadWorkerCmd::Register(id, since_handle))
    }

    /// Update the existing since handle associated with `id` to `since_handle`.
    ///
    /// Note that this should only be called when updating a since handle; to
    /// initially associate an `id` to a since handle, use [`Self::register`].
    ///
    /// # Panics
    /// - If `id` is not currently associated with any since handle.
    #[allow(dead_code)]
    pub(crate) fn update(
        &self,
        id: GlobalId,
        since_handle: SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    ) {
        self.send(PersistReadWorkerCmd::Update(id, since_handle))
    }

    pub(crate) fn downgrade(
        &self,
        frontiers: BTreeMap<GlobalId, (Antichain<T>, oneshot::Sender<Result<(), ()>>)>,
    ) {
        self.send(PersistReadWorkerCmd::Downgrade(frontiers));
    }

    pub(crate) async fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: SnapshotStatsAsOf<T>,
    ) -> Result<SnapshotStats, StorageError> {
        // TODO: Pull this out of PersistReadWorker. Unlike the other methods,
        // the caller of this one drives it to completion.
        //
        // We'd need to either share the critical handle somehow or maybe have
        // two instances around, one in the worker and one in the controller.
        let (tx, rx) = oneshot::channel();
        self.send(PersistReadWorkerCmd::SnapshotStats(id, as_of, tx));
        rx.await.expect("PersistReadWorker should be live").0.await
    }

    fn send(&self, cmd: PersistReadWorkerCmd<T>) {
        let mut span = info_span!(parent: None, "PersistReadWorkerCmd::send");
        OpenTelemetryContext::obtain().attach_as_parent_to(&mut span);
        self.tx
            .send((span, cmd))
            .expect("persist worker exited while its handle was alive")
    }
}

#[derive(Debug, Clone)]
pub struct PersistTableWriteWorker<T: Timestamp + Lattice + Codec64 + TimestampManipulation> {
    inner: Arc<PersistTableWriteWorkerInner<T>>,
}

/// Commands for [PersistTableWriteWorker].
#[derive(Debug)]
enum PersistTableWriteCmd<T: Timestamp + Lattice + Codec64> {
    Register(T, Vec<(GlobalId, WriteHandle<SourceData, (), T, Diff>)>),
    Update(GlobalId, WriteHandle<SourceData, (), T, Diff>),
    DropHandle {
        /// Table that we want to drop our handle for.
        id: GlobalId,
        /// Notifies us when all resources have been cleaned up.
        tx: oneshot::Sender<()>,
    },
    Append {
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
        tx: tokio::sync::oneshot::Sender<Result<(), StorageError>>,
    },
    Shutdown,
}

async fn append_work<T2: Timestamp + Lattice + Codec64>(
    frontier_responses: &tokio::sync::mpsc::UnboundedSender<StorageResponse<T2>>,
    write_handles: &mut BTreeMap<GlobalId, WriteHandle<SourceData, (), T2, Diff>>,
    mut commands: BTreeMap<GlobalId, (tracing::Span, Vec<Update<T2>>, Antichain<T2>)>,
) -> Result<(), Vec<GlobalId>> {
    let futs = FuturesUnordered::new();

    // We cannot iterate through the updates and then set off a persist call
    // on the write handle because we cannot mutably borrow the write handle
    // multiple times.
    //
    // Instead, we first group the update by ID above and then iterate
    // through all available write handles and see if there are any updates
    // for it. If yes, we send them all in one go.
    for (id, write) in write_handles.iter_mut() {
        if let Some((span, updates, new_upper)) = commands.remove(id) {
            let persist_upper = write.upper().clone();
            let updates = updates
                .into_iter()
                .map(|u| ((SourceData(Ok(u.row)), ()), u.timestamp, u.diff));

            futs.push(async move {
                let persist_upper = persist_upper.clone();
                write
                    .compare_and_append(updates.clone(), persist_upper.clone(), new_upper.clone())
                    .instrument(span.clone())
                    .await
                    .expect("cannot append updates")
                    .or(Err(*id))?;

                Ok::<_, GlobalId>((*id, new_upper))
            })
        }
    }

    // Ensure all futures run to completion, and track status of each of them individually
    let (new_uppers, failed_appends): (Vec<_>, Vec<_>) = futs
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .partition_result();

    // It is not strictly an error for the controller to hang up.
    let _ = frontier_responses.send(StorageResponse::FrontierUppers(new_uppers));

    if failed_appends.is_empty() {
        Ok(())
    } else {
        Err(failed_appends)
    }
}

impl<T: Timestamp + Lattice + Codec64 + TimestampManipulation> PersistTableWriteWorker<T> {
    pub(crate) fn new_txns(
        frontier_responses: tokio::sync::mpsc::UnboundedSender<StorageResponse<T>>,
        txns: TxnsHandle<SourceData, (), T, i64, PersistEpoch, TxnsCodecRow>,
        persist_txn_tables: PersistTxnTablesImpl,
    ) -> Self {
        let (tx, rx) =
            tokio::sync::mpsc::unbounded_channel::<(tracing::Span, PersistTableWriteCmd<T>)>();
        mz_ore::task::spawn(|| "PersistTableWriteWorker", async move {
            let mut worker = TxnsTableWorker {
                persist_txn_tables,
                frontier_responses,
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
    ) {
        self.send(PersistTableWriteCmd::Register(register_ts, ids_handles))
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
        self.send(PersistTableWriteCmd::Update(id, write_handle))
    }

    pub(crate) fn append(
        &self,
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
    ) -> tokio::sync::oneshot::Receiver<Result<(), StorageError>> {
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

    /// Drops the handle associated with `id` from this worker.
    ///
    /// Note that this does not perform any other cleanup, such as finalizing
    /// the handle's shard.
    pub(crate) fn drop_handle(&self, id: GlobalId) -> BoxFuture<'static, ()> {
        let (tx, rx) = oneshot::channel();
        self.send(PersistTableWriteCmd::DropHandle { id, tx });
        Box::pin(rx.map(|_| ()))
    }

    fn send(&self, cmd: PersistTableWriteCmd<T>) {
        self.inner.send(cmd);
    }
}

struct TxnsTableWorker<T: Timestamp + Lattice + TotalOrder + Codec64> {
    persist_txn_tables: PersistTxnTablesImpl,
    frontier_responses: tokio::sync::mpsc::UnboundedSender<StorageResponse<T>>,
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
                PersistTableWriteCmd::Register(register_ts, ids_handles) => {
                    self.register(register_ts, ids_handles)
                        .instrument(span)
                        .await
                }
                PersistTableWriteCmd::Update(_id, _write_handle) => {
                    unimplemented!("TODO: Support migrations on persist-txn backed collections")
                }
                PersistTableWriteCmd::DropHandle { id, tx } => {
                    self.drop_handle(id).instrument(span).await;
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
        let new_uppers = ids_handles
            .iter()
            .map(|(id, _)| {
                (
                    *id,
                    Antichain::from_elem(TimestampManipulation::step_forward(&register_ts)),
                )
            })
            .collect();
        let handles = ids_handles.into_iter().map(|(_, handle)| handle);
        let res = self.txns.register(register_ts.clone(), handles).await;
        match res {
            Ok(tidy) => {
                self.tidy.merge(tidy);
                // If we using eager uppers, make sure to advance the physical
                // upper of the all data shards past the
                // register_ts. This protects against something like:
                //
                // - Upper of some shard s is at 5.
                // - Apply txns at 10.
                // - Crash.
                // - Reboot.
                // - Try and read s at 10.
                match self.persist_txn_tables {
                    PersistTxnTablesImpl::Eager => {
                        self.tidy
                            .merge(self.txns.apply_eager_le(&register_ts).await);
                    }
                    PersistTxnTablesImpl::Lazy => {}
                };
                self.send_new_uppers(new_uppers);
            }
            Err(current) => {
                panic!(
                    "cannot register {:?} at {:?} because txns is at {:?}",
                    new_uppers.iter().map(|(id, _)| id).collect::<Vec<_>>(),
                    register_ts,
                    current
                );
            }
        }
    }

    async fn drop_handle(&mut self, id: GlobalId) {
        tracing::info!(?id, "drop tables");
        // n.b. this should only remove the handle from the persist
        // worker and not take any additional action such as closing
        // the shard it's connected to because dataflows might still
        // be using it.
        if let Some(data_id) = self.write_handles.remove(&id) {
            // We don't currently get a timestamp allocated for this, thread one
            // through if inventing one here becomes an issue.
            let mut ts = T::minimum();
            loop {
                match self.txns.forget(ts, data_id).await {
                    Ok(tidy) => {
                        self.tidy.merge(tidy);
                        break;
                    }
                    Err(new_ts) => ts = new_ts,
                }
            }
        }
    }

    async fn append(
        &mut self,
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
        tx: tokio::sync::oneshot::Sender<Result<(), StorageError>>,
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
        // TODO: persist-txn doesn't take an advance_to yet, it uses
        // timestamp.step_forward. This is the same in all cases, so just assert that
        // for now. Note that this uses the _persist_ StepForward, not the
        // TimestampManipulation one (the impls are the same) because that's what
        // persist-txn uses.
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
                let tidy = match self.persist_txn_tables {
                    PersistTxnTablesImpl::Lazy => apply.apply(&mut self.txns).await,
                    PersistTxnTablesImpl::Eager => apply.apply_eager(&mut self.txns).await,
                };
                self.tidy.merge(tidy);
                // Committing a txn advances the logical upper of _every_ data
                // shard in the txns set, not just the ones that were written
                // to, so send new upper information for all registered data
                // shards.
                let new_uppers = self
                    .write_handles
                    .keys()
                    .map(|id| (*id, Antichain::from_elem(advance_to.clone())))
                    .collect();
                self.send_new_uppers(new_uppers);

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
                    self.write_handles.keys().copied().collect(),
                ))
            }
        };
        // It is not an error for the other end to hang up.
        let _ = tx.send(response);
    }

    fn send_new_uppers(&self, new_uppers: Vec<(GlobalId, Antichain<T>)>) {
        // It is not strictly an error for the controller to hang up.
        let _ = self
            .frontier_responses
            .send(StorageResponse::FrontierUppers(new_uppers));
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
        let mut span = info_span!(parent: None, "PersistTableWriteWorkerInner::send");
        OpenTelemetryContext::obtain().attach_as_parent_to(&mut span);
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
    Append(
        Vec<(GlobalId, Vec<Update<T>>, T)>,
        tokio::sync::oneshot::Sender<Result<(), StorageError>>,
    ),
    /// Appends `Vec<TimelessUpdate>` to `GlobalId` at, essentially,
    /// `max(write_frontier, T)`.
    MonotonicAppend(
        Vec<(GlobalId, Vec<TimestamplessUpdate>, T)>,
        tokio::sync::oneshot::Sender<Result<(), StorageError>>,
    ),
    Shutdown,
}

impl<T: Timestamp + Lattice + Codec64 + TimestampManipulation> PersistMonotonicWriteWorker<T> {
    pub(crate) fn new(
        frontier_responses: tokio::sync::mpsc::UnboundedSender<StorageResponse<T>>,
    ) -> Self {
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
                        PersistMonotonicWriteCmd::Append(updates, response) => {
                            let mut ids = BTreeSet::new();
                            for (id, update, upper) in updates {
                                ids.insert(id);
                                let (old_span, updates, old_upper) =
                                    all_updates.entry(id).or_insert_with(|| {
                                        (
                                            span.clone(),
                                            Vec::default(),
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
                                old_upper.join_assign(&Antichain::from_elem(upper));
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

                                let lower = if current_upper.less_than(&at_least) {
                                    at_least
                                } else {
                                    current_upper
                                        .into_option()
                                        .expect("cannot append data to closed collection")
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

                                updates_outer.push((id, update, upper));
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

                let result =
                    append_work(&frontier_responses, &mut write_handles, all_updates).await;

                for (ids, response) in all_responses {
                    let result = match &result {
                        Err(bad_ids) => {
                            let filtered: Vec<_> = bad_ids
                                .iter()
                                .filter(|id| ids.contains(id))
                                .copied()
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
    ) -> tokio::sync::oneshot::Receiver<Result<(), StorageError>> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_repr::{RelationDesc, Row};

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: integer-to-pointer casts and `ptr::from_exposed_addr`
    async fn snapshot_stats(&self) {
        let client = PersistClientCache::new(
            PersistConfig::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        )
        .open(PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        })
        .await
        .unwrap();
        let shard_id = ShardId::new();
        let since_handle = client
            .open_critical_since(
                shard_id,
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();
        let mut write_handle = client
            .open_writer::<SourceData, (), u64, i64>(
                shard_id,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();

        let worker = PersistReadWorker::<u64>::new();
        worker.register(GlobalId::User(1), since_handle);

        // No stats for unknown GlobalId.
        let stats = worker
            .snapshot_stats(
                GlobalId::User(2),
                SnapshotStatsAsOf::Direct(Antichain::from_elem(0)),
            )
            .await;
        assert!(stats.is_err());

        // Stats don't resolve for as_of past the upper.
        let stats_fut = worker.snapshot_stats(
            GlobalId::User(1),
            SnapshotStatsAsOf::Direct(Antichain::from_elem(1)),
        );
        assert!(stats_fut.now_or_never().is_none());
        // Call it again because now_or_never consumed our future and it's not clone-able.
        let stats_ts1_fut = worker.snapshot_stats(
            GlobalId::User(1),
            SnapshotStatsAsOf::Direct(Antichain::from_elem(1)),
        );

        // Write some data.
        let data = ((SourceData(Ok(Row::default())), ()), 0u64, 1i64);
        let () = write_handle
            .compare_and_append(&[data], Antichain::from_elem(0), Antichain::from_elem(1))
            .await
            .unwrap()
            .unwrap();

        // Verify that we can resolve stats for ts 0 while the ts 1 stats call is outstanding.
        let stats = worker
            .snapshot_stats(
                GlobalId::User(1),
                SnapshotStatsAsOf::Direct(Antichain::from_elem(0)),
            )
            .await
            .unwrap();
        assert_eq!(stats.num_updates, 1);

        // Write more data and unblock the ts 1 call
        let data = ((SourceData(Ok(Row::default())), ()), 1u64, 1i64);
        let () = write_handle
            .compare_and_append(&[data], Antichain::from_elem(1), Antichain::from_elem(2))
            .await
            .unwrap()
            .unwrap();
        let stats = stats_ts1_fut.await.unwrap();
        assert_eq!(stats.num_updates, 2);
    }
}
