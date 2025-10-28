// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Interfaces for reading txn shards as well as data shards.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures::Stream;
use mz_ore::instrument;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_TXN;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::{Cursor, LazyPartStats, ListenEvent, ReadHandle, Since, Subscribe};
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::txn::{TxnsCodec, TxnsEntry};
use mz_persist_types::{Codec, Codec64, Opaque, StepForward};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::TxnsCodecDefault;
use crate::txn_cache::{TxnsCache, TxnsCacheState};

/// A token exchangeable for a data shard snapshot.
///
/// - Invariant: `latest_write <= as_of < empty_to`
/// - Invariant: `(latest_write, empty_to)` and `(as_of, empty_to)` have no
///   unapplied writes (which means we can do an empty CaA of those times if we
///   like).
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DataSnapshot<T> {
    /// The id of the data shard this snapshot is for.
    pub(crate) data_id: ShardId,
    /// The latest possibly unapplied write <= as_of. None if there are no
    /// writes via txns or if they're all known to be applied.
    pub(crate) latest_write: Option<T>,
    /// The as_of asked for.
    pub(crate) as_of: T,
    /// Some timestamp s.t. [as_of, empty_to) is known to be empty of
    /// unapplied writes via txns.
    pub(crate) empty_to: T,
}

impl<T: Timestamp + Lattice + TotalOrder + Codec64 + Sync> DataSnapshot<T> {
    /// Unblocks reading a snapshot at `self.as_of` by waiting for the latest
    /// write before that time and then running an empty CaA if necessary.
    #[instrument(level = "debug", fields(shard = %self.data_id, ts = ?self.as_of, empty_to = ?self.empty_to))]
    pub(crate) async fn unblock_read<K, V, D>(&self, mut data_write: WriteHandle<K, V, T, D>)
    where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
    {
        debug!(
            "unblock_read latest_write={:?} as_of={:?} for {:.9}",
            self.latest_write,
            self.as_of,
            self.data_id.to_string()
        );
        // First block until the latest write has been applied.
        if let Some(latest_write) = self.latest_write.as_ref() {
            let () = data_write
                .wait_for_upper_past(&Antichain::from_elem(latest_write.clone()))
                .await;
        }

        // Now fill `(latest_write, as_of]` with empty updates, so we can read
        // the shard at as_of normally. In practice, because CaA takes an
        // exclusive upper, we actually fill `(latest_write, empty_to)`.
        //
        // It's quite counter-intuitive for reads to involve writes, but I think
        // this is fine. In particular, because writing empty updates to a
        // persist shard is a metadata-only operation. It might result in things
        // like GC maintenance or a CRDB write, but this is also true for
        // registering a reader. On the balance, I think this is a _much_ better
        // set of tradeoffs than the original plan of trying to translate read
        // timestamps to the most recent write and reading that.
        let Some(mut data_upper) = data_write.shared_upper().into_option() else {
            // If the upper is the empty antichain, then we've unblocked all
            // possible reads. Return early.
            debug!(
                "CaA data snapshot {:.9} shard finalized",
                self.data_id.to_string(),
            );
            return;
        };

        // TODO(jkosh44) We should not be writing to unregistered shards, but
        // we haven't checked to see if this was registered at `self.empty_to`.
        // See https://github.com/MaterializeInc/database-issues/issues/8022.
        while data_upper < self.empty_to {
            // It would be very bad if we accidentally filled any times <=
            // latest_write with empty updates, so defensively assert on each
            // iteration through the loop.
            if let Some(latest_write) = self.latest_write.as_ref() {
                assert!(latest_write < &data_upper);
            }
            assert!(self.as_of < self.empty_to);
            let res = crate::small_caa(
                || format!("data {:.9} unblock reads", self.data_id.to_string()),
                &mut data_write,
                &[],
                data_upper.clone(),
                self.empty_to.clone(),
            )
            .await;
            match res {
                Ok(()) => {
                    // Persist registers writers on the first write, so politely
                    // expire the writer we just created, but (as a performance
                    // optimization) only if we actually wrote something.
                    data_write.expire().await;
                    break;
                }
                Err(new_data_upper) => {
                    data_upper = new_data_upper;
                    continue;
                }
            }
        }
    }

    /// See [ReadHandle::snapshot_and_fetch].
    pub async fn snapshot_and_fetch<K, V, D>(
        &self,
        data_read: &mut ReadHandle<K, V, T, D>,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, Since<T>>
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
    {
        let data_write = WriteHandle::from_read(data_read, "unblock_read");
        self.unblock_read(data_write).await;
        data_read
            .snapshot_and_fetch(Antichain::from_elem(self.as_of.clone()))
            .await
    }

    /// See [ReadHandle::snapshot_cursor].
    pub async fn snapshot_cursor<K, V, D>(
        &self,
        data_read: &mut ReadHandle<K, V, T, D>,
        should_fetch_part: impl for<'a> Fn(Option<&'a LazyPartStats>) -> bool,
    ) -> Result<Cursor<K, V, T, D>, Since<T>>
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
    {
        let data_write = WriteHandle::from_read(data_read, "unblock_read");
        self.unblock_read(data_write).await;
        data_read
            .snapshot_cursor(Antichain::from_elem(self.as_of.clone()), should_fetch_part)
            .await
    }

    /// See [ReadHandle::snapshot_and_stream].
    pub async fn snapshot_and_stream<K, V, D>(
        &self,
        data_read: &mut ReadHandle<K, V, T, D>,
    ) -> Result<
        impl Stream<Item = ((Result<K, String>, Result<V, String>), T, D)> + use<K, V, T, D>,
        Since<T>,
    >
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
    {
        let data_write = WriteHandle::from_read(data_read, "unblock_read");
        self.unblock_read(data_write).await;
        data_read
            .snapshot_and_stream(Antichain::from_elem(self.as_of.clone()))
            .await
    }

    /// See [SinceHandle::snapshot_stats].
    pub fn snapshot_stats_from_critical<K, V, D, O>(
        &self,
        data_since: &SinceHandle<K, V, T, D, O>,
    ) -> impl Future<Output = Result<SnapshotStats, Since<T>>> + Send + 'static
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Semigroup + Codec64 + Send + Sync,
        O: Opaque + Codec64,
    {
        // This is used by the optimizer in planning to get cost statistics, so
        // it can't use `unblock_read`. Instead use the "translated as_of"
        // trick we invented but didn't end up using for read of the shard
        // contents. The reason we didn't use it for that was because we'd have
        // to deal with advancing timestamps of the updates we read, but the
        // stats we return here don't have that issue.
        //
        // TODO: If we don't have a `latest_write`, then the `None` option to
        // `snapshot_stats` is not quite correct because of pubsub races
        // (probably marginal) and historical `as_of`s (probably less marginal
        // but not common in mz right now). Fixing this more precisely in a
        // performant way (i.e. no crdb queries involved) seems to require that
        // txn-wal always keep track of the latest write, even when it's
        // known to have been applied. `snapshot_stats` is an estimate anyway,
        // it doesn't even attempt to account for things like consolidation, so
        // this seems fine for now.
        let as_of = self.latest_write.clone().map(Antichain::from_elem);
        data_since.snapshot_stats(as_of)
    }

    /// See [ReadHandle::snapshot_stats].
    pub fn snapshot_stats_from_leased<K, V, D>(
        &self,
        data_since: &ReadHandle<K, V, T, D>,
    ) -> impl Future<Output = Result<SnapshotStats, Since<T>>> + Send + 'static
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Ord + Semigroup + Codec64 + Send + Sync,
    {
        // This is used by the optimizer in planning to get cost statistics, so
        // it can't use `unblock_read`. Instead use the "translated as_of"
        // trick we invented but didn't end up using for read of the shard
        // contents. The reason we didn't use it for that was because we'd have
        // to deal with advancing timestamps of the updates we read, but the
        // stats we return here don't have that issue.
        //
        // TODO: If we don't have a `latest_write`, then the `None` option to
        // `snapshot_stats` is not quite correct because of pubsub races
        // (probably marginal) and historical `as_of`s (probably less marginal
        // but not common in mz right now). Fixing this more precisely in a
        // performant way (i.e. no crdb queries involved) seems to require that
        // txn-wal always keep track of the latest write, even when it's
        // known to have been applied. `snapshot_stats` is an estimate anyway,
        // it doesn't even attempt to account for things like consolidation, so
        // this seems fine for now.
        let as_of = self.latest_write.clone().map(Antichain::from_elem);
        data_since.snapshot_stats(as_of)
    }

    /// See [ReadHandle::snapshot_parts_stats].
    pub async fn snapshot_parts_stats<K, V, D>(
        &self,
        data_read: &ReadHandle<K, V, T, D>,
    ) -> Result<SnapshotPartsStats, Since<T>>
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
    {
        let data_write = WriteHandle::from_read(data_read, "unblock_read");
        self.unblock_read(data_write).await;
        data_read
            .snapshot_parts_stats(Antichain::from_elem(self.as_of.clone()))
            .await
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if let Some(latest_write) = self.latest_write.as_ref() {
            if !(latest_write <= &self.as_of) {
                return Err(format!(
                    "latest_write {:?} not <= as_of {:?}",
                    self.latest_write, self.as_of
                ));
            }
        }
        if !(self.as_of < self.empty_to) {
            return Err(format!(
                "as_of {:?} not < empty_to {:?}",
                self.as_of, self.empty_to
            ));
        }
        Ok(())
    }
}

/// The next action to take in a data shard `Listen`.
///
/// See [crate::txn_cache::TxnsCacheState::data_listen_next].
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub enum DataListenNext<T> {
    /// Read the data shard normally, until this timestamp is less_equal what
    /// has been read.
    ReadDataTo(T),
    /// It is known that there are no writes between the progress given to the
    /// `data_listen_next` call and this timestamp. Advance the data shard
    /// listen progress to this (exclusive) frontier.
    EmitLogicalProgress(T),
    /// The data shard listen has caught up to what has been written to the txns
    /// shard. Wait for it to progress with `update_gt` and call
    /// `data_listen_next` again.
    WaitForTxnsProgress,
}

/// A mapping between the physical upper of a data shard and the largest upper
/// which is known to logically have the same contents.
///
/// Said another way, `[physical_upper,logical_upper)` is known to be empty (in
/// the "definite" sense).
///
/// Invariant: physical_upper <= logical_upper
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataRemapEntry<T> {
    /// The physical upper of a data shard.
    pub physical_upper: T,
    /// An upper bound on the times known to be empty of writes via txns since
    /// `physical_upper`.
    pub logical_upper: T,
}

/// Keeps track of a [`DataRemapEntry`] for shard `data_id`.
#[derive(Debug)]
pub(crate) struct DataSubscribe<T> {
    /// The id of the data shard.
    pub(crate) data_id: ShardId,
    /// The initial snapshot, used to unblock the reading of the initial
    /// snapshot.
    pub(crate) snapshot: Option<DataSnapshot<T>>,
    /// The physical and logical upper of `data_id`.
    pub(crate) remap: DataRemapEntry<T>,
}

/// An active subscription of [`DataRemapEntry`]s for a data shard.
#[derive(Debug)]
pub struct DataSubscription<T> {
    /// Metadata and current [`DataRemapEntry`] for the data shard.
    subscribe: DataSubscribe<T>,
    /// Channel to send [`DataRemapEntry`]s.
    tx: mpsc::UnboundedSender<DataRemapEntry<T>>,
}

/// A shared [TxnsCache] running in a task and communicated with over a channel.
#[derive(Debug, Clone)]
pub struct TxnsRead<T> {
    txns_id: ShardId,
    tx: mpsc::UnboundedSender<TxnsReadCmd<T>>,
    _read_task: Arc<AbortOnDropHandle<()>>,
    _subscribe_task: Arc<AbortOnDropHandle<()>>,
}

impl<T: Timestamp + Lattice + Codec64 + Sync> TxnsRead<T> {
    /// Starts the task worker and returns a handle for communicating with it.
    pub async fn start<C>(client: PersistClient, txns_id: ShardId) -> Self
    where
        T: TotalOrder + StepForward,
        C: TxnsCodec + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();

        let (mut subscribe_task, cache) =
            TxnsSubscribeTask::<T, C>::open(&client, txns_id, None, tx.clone()).await;

        let mut task = TxnsReadTask {
            rx,
            cache,
            pending_waits_by_ts: BTreeSet::new(),
            pending_waits_by_id: BTreeMap::new(),
            data_subscriptions: Vec::new(),
        };

        let read_task =
            mz_ore::task::spawn(|| "txn-wal::read_task", async move { task.run().await });

        let subscribe_task = mz_ore::task::spawn(|| "txn-wal::subscribe_task", async move {
            subscribe_task.run().await
        });

        TxnsRead {
            txns_id,
            tx,
            _read_task: Arc::new(read_task.abort_on_drop()),
            _subscribe_task: Arc::new(subscribe_task.abort_on_drop()),
        }
    }

    /// Returns the [ShardId] of the txns shard.
    pub fn txns_id(&self) -> &ShardId {
        &self.txns_id
    }

    /// See [crate::txn_cache::TxnsCacheState::data_snapshot].
    pub async fn data_snapshot(&self, data_id: ShardId, as_of: T) -> DataSnapshot<T> {
        self.send(|tx| TxnsReadCmd::DataSnapshot { data_id, as_of, tx })
            .await
    }

    /// Initiate a subscription to `data_id`.
    ///
    /// Returns a channel that [`DataRemapEntry`]s are sent over.
    pub(crate) async fn data_subscribe<K, V, D>(
        &self,
        data_id: ShardId,
        as_of: T,
        unblock: WriteHandle<K, V, T, D>,
    ) -> mpsc::UnboundedReceiver<DataRemapEntry<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
    {
        let (snapshot, rest) = self
            .send(|tx| TxnsReadCmd::DataSubscribe { data_id, as_of, tx })
            .await;
        if let Some(snapshot) = snapshot {
            snapshot.unblock_read(unblock).await;
        }
        rest
    }

    /// See [TxnsCache::update_ge].
    pub async fn update_ge(&self, ts: T) {
        let wait = WaitTs::GreaterEqual(ts);
        self.update(wait).await
    }

    /// See [TxnsCache::update_gt].
    pub async fn update_gt(&self, ts: T) {
        let wait = WaitTs::GreaterThan(ts);
        self.update(wait).await
    }

    async fn update(&self, wait: WaitTs<T>) {
        let id = Uuid::new_v4();
        let res = self.send(|tx| TxnsReadCmd::Wait {
            id: id.clone(),
            ts: wait,
            tx,
        });

        // We install a drop guard so that we can cancel the wait in case the
        // future is cancelled/dropped.
        let mut cancel_guard = CancelWaitOnDrop {
            id,
            tx: Some(self.tx.clone()),
        };

        let res = res.await;

        // We don't have to cancel the wait on drop anymore.
        cancel_guard.complete();

        res
    }

    async fn send<R: std::fmt::Debug>(
        &self,
        cmd: impl FnOnce(oneshot::Sender<R>) -> TxnsReadCmd<T>,
    ) -> R {
        let (tx, rx) = oneshot::channel();
        let req = cmd(tx);
        let () = self.tx.send(req).expect("task unexpectedly shut down");
        rx.await.expect("task unexpectedly shut down")
    }
}

/// Cancels an in-flight wait command when dropped, unless the given `tx` is
/// yanked before that.
struct CancelWaitOnDrop<T> {
    id: Uuid,
    tx: Option<mpsc::UnboundedSender<TxnsReadCmd<T>>>,
}

impl<T> CancelWaitOnDrop<T> {
    /// Marks the wait command as complete. This guard will no longer send a
    /// cancel command when dropped.
    pub fn complete(&mut self) {
        self.tx.take();
    }
}

impl<T> Drop for CancelWaitOnDrop<T> {
    fn drop(&mut self) {
        let tx = match self.tx.take() {
            Some(tx) => tx,
            None => {
                // No need to cancel anymore!
                return;
            }
        };

        let _ = tx.send(TxnsReadCmd::CancelWait {
            id: self.id.clone(),
        });
    }
}

#[derive(Debug)]
enum TxnsReadCmd<T> {
    Updates {
        entries: Vec<(TxnsEntry, T, i64)>,
        frontier: T,
    },
    DataSnapshot {
        data_id: ShardId,
        as_of: T,
        tx: oneshot::Sender<DataSnapshot<T>>,
    },
    DataSubscribe {
        data_id: ShardId,
        as_of: T,
        tx: oneshot::Sender<(
            Option<DataSnapshot<T>>,
            mpsc::UnboundedReceiver<DataRemapEntry<T>>,
        )>,
    },
    Wait {
        id: Uuid,
        ts: WaitTs<T>,
        tx: oneshot::Sender<()>,
    },
    CancelWait {
        id: Uuid,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum WaitTs<T> {
    GreaterEqual(T),
    GreaterThan(T),
}

// Specially made for keeping `WaitTs` in a `BTreeSet` and peeling them off in
// the order in which they would be retired.
//
// [`WaitTs`] with different timestamps are ordered according to their
// timestamps. For [`WaitTs`] with the same timestamp, we have to order
// `GreaterEqual` before `GreaterThan`, because those can be retired
// earlier/they are less "strict" in how far they need the frontier to advance.
impl<T: Ord> Ord for WaitTs<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_ts = match self {
            WaitTs::GreaterEqual(ts) => ts,
            WaitTs::GreaterThan(ts) => ts,
        };
        let other_ts = match other {
            WaitTs::GreaterEqual(ts) => ts,
            WaitTs::GreaterThan(ts) => ts,
        };

        if self_ts < other_ts {
            Ordering::Less
        } else if *self_ts > *other_ts {
            Ordering::Greater
        } else if matches!(self, WaitTs::GreaterEqual(_)) && matches!(other, WaitTs::GreaterThan(_))
        {
            Ordering::Less
        } else if matches!(self, WaitTs::GreaterThan(_)) && matches!(other, WaitTs::GreaterEqual(_))
        {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl<T: Ord> PartialOrd for WaitTs<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Timestamp + Lattice> WaitTs<T> {
    /// Returns `true` iff (sic) this [WaitTs] is ready.
    fn is_ready(&self, frontier: &T) -> bool {
        match &self {
            WaitTs::GreaterEqual(ts) => {
                if frontier >= ts {
                    return true;
                }
            }
            WaitTs::GreaterThan(ts) => {
                if frontier > ts {
                    return true;
                }
            }
        };

        false
    }
}

#[derive(Debug)]
struct TxnsReadTask<T> {
    rx: mpsc::UnboundedReceiver<TxnsReadCmd<T>>,
    cache: TxnsCacheState<T>,
    pending_waits_by_ts: BTreeSet<(WaitTs<T>, Uuid)>,
    pending_waits_by_id: BTreeMap<Uuid, PendingWait<T>>,
    data_subscriptions: Vec<DataSubscription<T>>,
}

/// A pending "wait" notification that we will complete once the frontier
/// advances far enough.
#[derive(Debug)]
struct PendingWait<T> {
    ts: WaitTs<T>,
    tx: Option<oneshot::Sender<()>>,
}

impl<T: Timestamp + Lattice + Codec64> PendingWait<T> {
    /// Returns `true` if this [PendingWait] is completed.
    ///
    /// A pending wait is completed when the frontier advances far enough or
    /// when the receiver side hangs up.
    fn maybe_complete(&mut self, frontier: &T) -> bool {
        if self.tx.is_none() {
            // Already completed!
            return true;
        }

        if self.ts.is_ready(frontier) {
            let _ = self.tx.take().expect("known to exist").send(());
            return true;
        }

        if let Some(tx) = self.tx.as_ref() {
            if tx.is_closed() {
                // Receiver dropped, so also complete.
                self.tx.take();
                return true;
            }
        }

        false
    }
}

impl<T> TxnsReadTask<T>
where
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
{
    async fn run(&mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                TxnsReadCmd::Updates { entries, frontier } => {
                    tracing::trace!(
                        "updates from subscribe task at ({:?}): {:?}",
                        frontier,
                        entries
                    );

                    self.cache.push_entries(entries.clone(), frontier.clone());

                    self.data_subscriptions
                        .retain(|subscription| !subscription.tx.is_closed());
                    for subscription in &mut self.data_subscriptions {
                        Self::update_subscription(subscription, &self.cache);
                    }

                    // The frontier has advanced, so respond to waits and retain
                    // those that still have to wait.

                    loop {
                        let first_wait = self.pending_waits_by_ts.first();

                        let (wait_ts, id) = match first_wait {
                            Some(wait) => wait,
                            None => break,
                        };

                        let completed = wait_ts.is_ready(&frontier);

                        if completed {
                            let mut wait = self
                                .pending_waits_by_id
                                .remove(id)
                                .expect("wait must be in map");

                            let really_completed = wait.maybe_complete(&frontier);
                            assert!(really_completed);

                            self.pending_waits_by_ts.pop_first();
                        } else {
                            // All further wait's timestamps are higher. We're
                            // using a `BTreeSet`, which is ordered!
                            break;
                        }
                    }
                }
                TxnsReadCmd::DataSnapshot { data_id, as_of, tx } => {
                    let res = self.cache.data_snapshot(data_id, as_of.clone());
                    let _ = tx.send(res);
                }
                TxnsReadCmd::DataSubscribe { data_id, as_of, tx } => {
                    let mut subscribe = self.cache.data_subscribe(data_id, as_of.clone());
                    let snapshot = subscribe.snapshot.take();
                    let (sub_tx, sub_rx) = mpsc::unbounded_channel();
                    // Send the initial remap entry.
                    sub_tx
                        .send(subscribe.remap.clone())
                        .expect("receiver still in scope");
                    let mut subscription = DataSubscription {
                        subscribe,
                        tx: sub_tx,
                    };
                    // Fill the subscriber in on the updates from as_of to the current progress.
                    Self::update_subscription(&mut subscription, &self.cache);
                    self.data_subscriptions.push(subscription);
                    let _ = tx.send((snapshot, sub_rx));
                }
                TxnsReadCmd::Wait { id, ts, tx } => {
                    let mut pending_wait = PendingWait { ts, tx: Some(tx) };
                    let completed = pending_wait.maybe_complete(&self.cache.progress_exclusive);
                    if !completed {
                        let wait_ts = pending_wait.ts.clone();
                        self.pending_waits_by_ts.insert((wait_ts, id.clone()));
                        self.pending_waits_by_id.insert(id, pending_wait);
                    }
                }
                TxnsReadCmd::CancelWait { id } => {
                    // A waiter may have been dropped after a wait completed,
                    // but before hearing about the completion. In that case
                    // they will have tried to cancel an already cleaned up
                    // wait.
                    if let Some(pending_wait) = self.pending_waits_by_id.remove(&id) {
                        self.pending_waits_by_ts.remove(&(pending_wait.ts, id));
                    }
                }
            }
        }
        warn!("TxnsReadTask shutting down");
    }

    fn update_subscription(subscription: &mut DataSubscription<T>, cache: &TxnsCacheState<T>) {
        loop {
            match cache.data_listen_next(
                &subscription.subscribe.data_id,
                &subscription.subscribe.remap.logical_upper,
            ) {
                // The data shard got a write!
                DataListenNext::ReadDataTo(new_upper) => {
                    // A write means both the physical and logical upper advance.
                    subscription.subscribe.remap.physical_upper = new_upper.clone();
                    subscription.subscribe.remap.logical_upper = new_upper.clone();
                }
                // We know there are no writes in `[logical_upper,
                // new_progress)`, so advance our output frontier.
                DataListenNext::EmitLogicalProgress(new_progress) => {
                    assert!(subscription.subscribe.remap.physical_upper < new_progress);
                    assert!(subscription.subscribe.remap.logical_upper < new_progress);

                    subscription.subscribe.remap.logical_upper = new_progress.clone();
                }
                // We've caught up to the txns upper, and we have to wait for
                // more before updates before sending more pairs.
                DataListenNext::WaitForTxnsProgress => break,
            };
            // Not an error if the receiver hung up, they just need be cleaned up at some point.
            let _ = subscription.tx.send(subscription.subscribe.remap.clone());
        }
        assert_eq!(
            cache.progress_exclusive, subscription.subscribe.remap.logical_upper,
            "we should update the subscription up to the current progress_exclusive"
        );
    }
}

/// Reads txn updates from a [Subscribe] and forwards them to a [TxnsReadTask]
/// when receiving a progress update.
#[derive(Debug)]
struct TxnsSubscribeTask<T, C: TxnsCodec = TxnsCodecDefault> {
    txns_subscribe: Subscribe<C::Key, C::Val, T, i64>,

    /// Staged update that we will consume and forward to the [TxnsReadTask]
    /// when we receive a progress update.
    buf: Vec<(TxnsEntry, T, i64)>,

    /// For sending updates to the main [TxnsReadTask].
    tx: mpsc::UnboundedSender<TxnsReadCmd<T>>,

    /// If Some, this cache only tracks the indicated data shard as a
    /// performance optimization. When used, only some methods (in particular,
    /// the ones necessary for the txns_progress operator) are supported.
    ///
    /// TODO: It'd be nice to make this a compile time thing. I have some ideas,
    /// but they're decently invasive, so leave it for a followup.
    only_data_id: Option<ShardId>,
}

impl<T, C> TxnsSubscribeTask<T, C>
where
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    C: TxnsCodec,
{
    /// Creates a [TxnsSubscribeTask] reading from the given txn shard that
    /// forwards updates (entries and progress) to the given `tx`.
    ///
    /// This returns both the created task and a [TxnsCacheState] that can be
    /// used to interact with the txn system and into which the updates should
    /// be funneled.
    ///
    /// NOTE: We create both the [TxnsSubscribeTask] and the [TxnsCacheState] at
    /// the same time because the cache is initialized with a `since_ts`, which
    /// we get from the same [ReadHandle] that we use to initialize the
    /// [Subscribe].
    pub async fn open(
        client: &PersistClient,
        txns_id: ShardId,
        only_data_id: Option<ShardId>,
        tx: mpsc::UnboundedSender<TxnsReadCmd<T>>,
    ) -> (Self, TxnsCacheState<T>) {
        let (txns_key_schema, txns_val_schema) = C::schemas();
        let txns_read: ReadHandle<<C as TxnsCodec>::Key, <C as TxnsCodec>::Val, T, i64> = client
            .open_leased_reader(
                txns_id,
                Arc::new(txns_key_schema),
                Arc::new(txns_val_schema),
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "read txns".to_owned(),
                },
                USE_CRITICAL_SINCE_TXN.get(client.dyncfgs()),
            )
            .await
            .expect("txns schema shouldn't change");
        let (state, txns_subscribe) = TxnsCacheState::init::<C>(only_data_id, txns_read).await;
        let subscribe_task = TxnsSubscribeTask {
            txns_subscribe,
            buf: Vec::new(),
            tx,
            only_data_id,
        };

        (subscribe_task, state)
    }

    async fn run(&mut self) {
        loop {
            let events = self.txns_subscribe.next(None).await;
            for event in events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        let frontier_ts = frontier
                            .into_option()
                            .expect("nothing should close the txns shard");
                        let entries = std::mem::take(&mut self.buf);
                        let res = self.tx.send(TxnsReadCmd::Updates {
                            entries,
                            frontier: frontier_ts,
                        });
                        if let Err(e) = res {
                            warn!("TxnsSubscribeTask shutting down: {}", e);
                            return;
                        }
                    }
                    ListenEvent::Updates(parts) => {
                        TxnsCache::<T, C>::fetch_parts(
                            self.only_data_id.clone(),
                            &mut self.txns_subscribe,
                            parts,
                            &mut self.buf,
                        )
                        .await;
                    }
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WaitTs;

    #[mz_ore::test]
    fn wait_ts_ord() {
        let mut waits = vec![
            WaitTs::GreaterThan(3),
            WaitTs::GreaterThan(2),
            WaitTs::GreaterEqual(2),
            WaitTs::GreaterThan(1),
        ];

        waits.sort();

        let expected = vec![
            WaitTs::GreaterThan(1),
            WaitTs::GreaterEqual(2),
            WaitTs::GreaterThan(2),
            WaitTs::GreaterThan(3),
        ];

        assert_eq!(waits, expected);
    }
}
