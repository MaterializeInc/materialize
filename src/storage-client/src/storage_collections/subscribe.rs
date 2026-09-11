// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Snapshot-then-listen streams over a collection's persist shard, see
//! [`StorageCollections::subscribe`](super::StorageCollections::subscribe).
//!
//! Registering a persist reader is a compare-and-set on the shard's state, so
//! many subscribers each opening their own reader serialize on that state. A
//! [`SharedTail`] per collection instead opens one listen and one snapshot
//! reader and fans the decoded listen events out to every subscriber. It
//! retains a short window of past events so a subscriber whose `as_of` lies
//! slightly behind the shared listen still gets a complete stream: its own
//! snapshot at `as_of` through the shared snapshot reader, the retained events
//! after `as_of`, then the live events. A subscriber whose `as_of` is older
//! than the retained window gets a private reader instead.
//!
//! A subscriber's snapshot is read through a consolidating cursor and handed
//! out in chunks as the stream is polled, so a large snapshot is never held in
//! memory at once. Listen events are pushed into a per-subscriber queue as the
//! shard advances and pulled when the subscriber's stream is polled, so a slow
//! subscriber costs only its queue. A queue that exceeds the subscriber's byte budget is
//! dropped and the stream ends with [`SubscribeEvent::Detached`]. The
//! subscriber then resumes from its own frontier with a new stream, which
//! reads from the retained window or from the shard itself, so the shard's
//! retained history, not memory, is what bounds how far behind a subscriber
//! may fall.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use mz_persist_client::ShardId;
use mz_persist_client::fetch::LeasedBatchPart;
use mz_persist_client::read::{Cursor, Listen, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::StorageError;
use mz_storage_types::sources::SourceData;
use mz_txn_wal::txn_read::{DataRemapEntry, TxnsRead};
use timely::PartialOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::debug;

use super::CollectionState;

/// An update of a collection, as stored in its shard.
pub type Update = (SourceData, Timestamp, StorageDiff);

/// An event of a [`StorageCollections::subscribe`](super::StorageCollections::subscribe)
/// stream.
#[derive(Debug, Clone)]
pub enum SubscribeEvent {
    /// Updates, possibly shared with the other subscribers of the collection.
    Updates(Arc<Vec<Update>>),
    /// A consolidated piece of the subscriber's snapshot, all at its `as_of`.
    /// Chunks arrive in order, before any other event, and each is complete in
    /// itself: no update in a later chunk consolidates with one already
    /// delivered, so a chunk may be emitted without waiting for the rest of
    /// the snapshot.
    ///
    /// Persist consolidates the encoded updates. Two encodings of one row
    /// therefore reach different chunks, which the subscriber is left to
    /// consolidate or, as here, to emit as they are.
    SnapshotChunk(Arc<Vec<Update>>),
    /// Every update at a time before this frontier has been emitted. The stream
    /// ends after an empty frontier.
    Progress(Antichain<Timestamp>),
    /// The subscriber's queue of events it had not yet pulled exceeded its
    /// budget, so the queue was dropped and the stream ends. Every update below
    /// the last `Progress` frontier was emitted, so the subscriber can resume
    /// from there with a new stream.
    Detached {
        buffered_bytes: usize,
        max_buffered_bytes: usize,
    },
}

/// What a subscriber's memory is bounded by, from the dyncfgs at the time it
/// joins.
#[derive(Debug, Clone, Copy)]
pub struct SubscribeLimits {
    /// Bytes of queued events after which the subscriber is detached.
    pub max_buffered_bytes: usize,
    /// Updates of the snapshot delivered in one event, see
    /// `SUBSCRIBE_SNAPSHOT_CHUNK_SIZE`. The cursor yields larger pieces than
    /// this, so it bounds what a single poll decodes, not what the cursor
    /// holds.
    pub snapshot_chunk: usize,
}

/// Bytes of decoded listen updates a shared tail retains for late joiners.
const RETAINED_BYTES_BUDGET: usize = 64 * 1024 * 1024;

/// How far behind its frontier a shared tail retains listen updates, in
/// milliseconds of collection time. Covers a subscriber whose `as_of`, chosen
/// from the timestamp oracle, trails the shard's upper.
const RETAINED_WINDOW_MS: u64 = 30_000;

/// How often the tail task lets the snapshot reader's since follow the
/// collection's since, which is what lets the shard compact.
const SINCE_DOWNGRADE_INTERVAL: Duration = Duration::from_secs(10);

/// Charged to every queued event on top of its updates' bytes, so a queue of
/// progress-only events still counts against a subscriber's budget.
const QUEUED_EVENT_OVERHEAD_BYTES: usize = 1024;

/// The persist handles a [`SharedTail`] runs on.
pub(super) struct TailHandles {
    pub listen: Listen<SourceData, (), Timestamp, StorageDiff>,
    pub snapshot_handle: ReadHandle<SourceData, (), Timestamp, StorageDiff>,
    /// `Some` for txn-wal backed collections, see [`raw_stream`].
    pub remap_rx: Option<mpsc::UnboundedReceiver<DataRemapEntry<Timestamp>>>,
    /// `Some` for txn-wal backed collections, to unblock a subscriber's snapshot
    /// at its `as_of`.
    pub txns: Option<(TxnsRead<Timestamp>, ShardId)>,
}

/// The shared tails of all collections, keyed by collection.
#[derive(Debug, Default)]
pub(super) struct SharedTails {
    /// An async lock, since creating a tail opens persist handles. Joining an
    /// existing tail holds it only briefly.
    tails: tokio::sync::Mutex<BTreeMap<GlobalId, Arc<SharedTail>>>,
}

impl SharedTails {
    /// Joins the shared tail of `id` at `as_of`, creating the tail with `open`
    /// if there is none. A subscriber whose `as_of` is older than what the
    /// shared tail retains gets a tail of its own instead, opened the same way
    /// but not offered to others.
    pub(super) async fn join(
        self: &Arc<Self>,
        id: GlobalId,
        as_of: Timestamp,
        with_snapshot: bool,
        limits: SubscribeLimits,
        collections: Arc<Mutex<BTreeMap<GlobalId, CollectionState>>>,
        open: impl FnOnce(Timestamp) -> BoxFuture<'static, Result<TailHandles, StorageError>>,
    ) -> Result<BoxStream<'static, SubscribeEvent>, StorageError> {
        let SubscribeLimits {
            max_buffered_bytes,
            snapshot_chunk,
        } = limits;
        // Called at most once: every path that opens breaks out of the loop.
        let mut open = Some(open);
        let started = Instant::now();
        let (tail, joined) = loop {
            let mut tails = self.tails.lock().await;
            debug!(%id, %as_of, lock_wait_ms = started.elapsed().as_millis(), "joining shared tail");
            match tails.get(&id).map(Arc::clone) {
                Some(tail) => match tail.try_join(as_of, max_buffered_bytes) {
                    Ok(joined) => break (tail, joined),
                    Err(JoinRefused::Closed) => {
                        // A tail closes when its last subscriber leaves. Replace it.
                        if tails.get(&id).is_some_and(|t| Arc::ptr_eq(t, &tail)) {
                            tails.remove(&id);
                        }
                    }
                    Err(JoinRefused::TooOld) => {
                        drop(tails);
                        let open = open.take().expect("opens at most once");
                        let handles = open(as_of).await?;
                        debug!(%id, %as_of, "as_of older than the shared tail retains, tailing privately");
                        break SharedTail::start(
                            id,
                            as_of,
                            handles,
                            None,
                            collections,
                            max_buffered_bytes,
                        );
                    }
                },
                None => {
                    let open = open.take().expect("opens at most once");
                    let handles = open(as_of).await?;
                    let (tail, joined) = SharedTail::start(
                        id,
                        as_of,
                        handles,
                        Some(Arc::clone(self)),
                        collections,
                        max_buffered_bytes,
                    );
                    tails.insert(id, Arc::clone(&tail));
                    break (tail, joined);
                }
            }
        };
        tail.joined_stream(as_of, with_snapshot, snapshot_chunk, joined)
            .await
    }

    async fn remove(&self, id: GlobalId, tail: &Arc<SharedTail>) {
        let mut tails = self.tails.lock().await;
        if tails.get(&id).is_some_and(|t| Arc::ptr_eq(t, tail)) {
            tails.remove(&id);
        }
    }
}

/// One listen over a collection's shard, fanned out to all its subscribers.
#[derive(Debug)]
pub(super) struct SharedTail {
    id: GlobalId,
    /// Requests to the snapshot task, which owns the snapshot reader and
    /// serves each subscriber its snapshot at its own `as_of`, see
    /// [`run_snapshotter`]. One task draining a queue beats a lock: with
    /// hundreds of subscribers arriving at once, a lock hands the reader from
    /// waiter to waiter through the scheduler, and each hop costs more than
    /// leasing a snapshot does.
    snapshot_tx: mpsc::UnboundedSender<SnapshotRequest>,
    state: Mutex<TailState>,
    /// The listen frontier, for subscribers to await without the state lock.
    frontier: watch::Sender<Antichain<Timestamp>>,
}

/// A consolidating read of a collection's shard at one `as_of`, which yields
/// the snapshot in pieces rather than all at once.
type SnapshotCursor = Cursor<SourceData, (), Timestamp, StorageDiff>;

enum SnapshotRequest {
    Snapshot {
        as_of: Timestamp,
        reply: oneshot::Sender<Result<SnapshotCursor, StorageError>>,
    },
    /// Let the snapshot reader's since follow the collection's since, which is
    /// what lets the shard compact.
    DowngradeSince(Antichain<Timestamp>),
}

#[derive(Debug)]
struct TailState {
    /// The listen frontier: the `upper` of the last published event.
    frontier: Antichain<Timestamp>,
    /// Updates at times at or past this frontier are in `retained`. Earlier
    /// ones have been evicted, so a subscriber needing them cannot join.
    retained_from: Antichain<Timestamp>,
    retained: VecDeque<Arc<TailEvent>>,
    retained_bytes: usize,
    subscribers: Vec<TailSubscriber>,
    /// Set once the last subscriber left or the collection closed. A closed
    /// tail accepts no subscribers and gets replaced.
    closed: bool,
}

/// A subscriber's queue of published events. The tail pushes without waiting
/// for the subscriber, and the subscriber pulls when its stream is polled, so
/// the queue is what a slow subscriber costs.
#[derive(Debug)]
struct TailSubscriber {
    tx: mpsc::UnboundedSender<TailItem>,
    /// Bytes of queued events, including overhead. Added to by the tail on
    /// push, subtracted from by the subscriber on pull.
    queued_bytes: Arc<AtomicUsize>,
    queued_events: Arc<AtomicUsize>,
    max_buffered_bytes: usize,
}

#[derive(Debug)]
enum TailItem {
    Event(Arc<TailEvent>),
    /// The queue exceeded its budget. The tail has dropped the subscriber, so
    /// this is the last item.
    Detached {
        buffered_bytes: usize,
        max_buffered_bytes: usize,
    },
}

/// What a subscriber gets back from joining a tail.
struct Joined {
    rx: mpsc::UnboundedReceiver<TailItem>,
    queued_bytes: Arc<AtomicUsize>,
    queued_events: Arc<AtomicUsize>,
    /// Retained events to replay before the queue.
    replay: Vec<Arc<TailEvent>>,
}

/// One published listen event: the decoded updates at times in
/// `[lower, upper)`, after which the tail's frontier is `upper`.
#[derive(Debug)]
struct TailEvent {
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
    updates: Arc<Vec<Update>>,
    bytes: usize,
}

enum JoinRefused {
    Closed,
    TooOld,
}

impl SharedTail {
    /// Creates the tail at `as_of` with one subscriber already joined, so the
    /// task cannot observe an empty subscriber list before the creator is in.
    /// A tail registered in `tails` is shared with later subscribers and
    /// removes itself when it stops; a private tail has no registry.
    fn start(
        id: GlobalId,
        as_of: Timestamp,
        handles: TailHandles,
        tails: Option<Arc<SharedTails>>,
        collections: Arc<Mutex<BTreeMap<GlobalId, CollectionState>>>,
        max_buffered_bytes: usize,
    ) -> (Arc<Self>, Joined) {
        let TailHandles {
            listen,
            snapshot_handle,
            remap_rx,
            txns,
        } = handles;
        let (subscriber, joined) = TailSubscriber::new(max_buffered_bytes);
        let (snapshot_tx, snapshot_rx) = mpsc::unbounded_channel();
        mz_ore::task::spawn(|| format!("shared-tail-snapshots-{id}"), async move {
            run_snapshotter(id, snapshot_handle, txns, snapshot_rx).await;
        });
        // The listen emits times past `as_of` only, so coverage starts there.
        let start = Antichain::from_elem(as_of.step_forward());
        let tail = Arc::new(SharedTail {
            id,
            snapshot_tx,
            state: Mutex::new(TailState {
                frontier: start.clone(),
                retained_from: start.clone(),
                retained: VecDeque::new(),
                retained_bytes: 0,
                subscribers: vec![subscriber],
                closed: false,
            }),
            frontier: watch::Sender::new(start),
        });
        debug!(%id, %as_of, shared = tails.is_some(), "starting subscribe tail");
        let task_tail = Arc::clone(&tail);
        mz_ore::task::spawn(|| format!("shared-tail-{id}"), async move {
            task_tail.run(listen, remap_rx, collections).await;
            if let Some(tails) = tails {
                tails.remove(id, &task_tail).await;
            }
            debug!(%id, "subscribe tail stopped");
        });
        (tail, joined)
    }

    /// Registers a subscriber at `as_of`.
    fn try_join(&self, as_of: Timestamp, max_buffered_bytes: usize) -> Result<Joined, JoinRefused> {
        let mut state = self.state.lock().expect("shared tail state poisoned");
        if state.closed {
            return Err(JoinRefused::Closed);
        }
        // The subscriber needs every update past `as_of`.
        let needs_from = Antichain::from_elem(as_of.step_forward());
        if !PartialOrder::less_equal(&state.retained_from, &needs_from) {
            return Err(JoinRefused::TooOld);
        }
        let (subscriber, mut joined) = TailSubscriber::new(max_buffered_bytes);
        joined.replay = state.retained.iter().cloned().collect();
        state.subscribers.push(subscriber);
        Ok(joined)
    }

    /// The tail task: publishes every listen event to the subscribers, keeps
    /// the retained window, and stops once the last subscriber left or the
    /// collection closed.
    async fn run(
        self: &Arc<Self>,
        listen: Listen<SourceData, (), Timestamp, StorageDiff>,
        remap_rx: Option<mpsc::UnboundedReceiver<DataRemapEntry<Timestamp>>>,
        collections: Arc<Mutex<BTreeMap<GlobalId, CollectionState>>>,
    ) {
        let mut events = Box::pin(raw_stream(listen, remap_rx));
        let mut pending = Vec::new();
        let mut pending_bytes = 0;
        let mut last_downgrade = Instant::now();
        while let Some(event) = events.next().await {
            match event {
                RawEvent::Updates(updates) => {
                    pending_bytes += updates.iter().map(update_bytes).sum::<usize>();
                    pending.extend(updates);
                }
                RawEvent::Progress(upper) => {
                    let closed = upper.is_empty();
                    let updates = std::mem::take(&mut pending);
                    let bytes = std::mem::take(&mut pending_bytes);
                    if !self.publish(updates, bytes, upper) || closed {
                        break;
                    }
                }
            }
            if last_downgrade.elapsed() >= SINCE_DOWNGRADE_INTERVAL {
                self.downgrade_snapshot_since(&collections);
                last_downgrade = Instant::now();
            }
        }
        let mut state = self.state.lock().expect("shared tail state poisoned");
        state.closed = true;
        state.subscribers.clear();
    }

    /// Publishes one event. Returns whether any subscriber is left, and closes
    /// the tail when none is.
    fn publish(&self, updates: Vec<Update>, bytes: usize, upper: Antichain<Timestamp>) -> bool {
        let mut state = self.state.lock().expect("shared tail state poisoned");
        let event = Arc::new(TailEvent {
            lower: std::mem::replace(&mut state.frontier, upper.clone()),
            upper,
            updates: Arc::new(updates),
            bytes,
        });

        state.retained_bytes += event.bytes;
        state.retained.push_back(Arc::clone(&event));
        let horizon = state
            .frontier
            .as_option()
            .and_then(|t| u64::from(*t).checked_sub(RETAINED_WINDOW_MS))
            .map(Timestamp::from);
        while let Some(oldest) = state.retained.front() {
            let over_budget = state.retained_bytes > RETAINED_BYTES_BUDGET;
            let too_old = match (&horizon, oldest.upper.as_option()) {
                (Some(horizon), Some(upper)) => upper < horizon,
                _ => false,
            };
            if !(over_budget || too_old) || state.retained.len() == 1 {
                break;
            }
            let evicted = state.retained.pop_front().expect("checked non-empty");
            state.retained_bytes -= evicted.bytes;
            state.retained_from = evicted.upper.clone();
        }

        let footprint = event.bytes + QUEUED_EVENT_OVERHEAD_BYTES;
        state.subscribers.retain(|subscriber| {
            let queued_bytes = subscriber
                .queued_bytes
                .fetch_add(footprint, Ordering::Relaxed)
                + footprint;
            let queued_events = subscriber.queued_events.fetch_add(1, Ordering::Relaxed) + 1;
            // A single event is queued whatever its size, like the message a
            // client is draining is tolerated today. Only a queue that builds
            // up behind one detaches the subscriber.
            if queued_events > 1 && queued_bytes > subscriber.max_buffered_bytes {
                let _ = subscriber.tx.send(TailItem::Detached {
                    buffered_bytes: queued_bytes,
                    max_buffered_bytes: subscriber.max_buffered_bytes,
                });
                return false;
            }
            subscriber
                .tx
                .send(TailItem::Event(Arc::clone(&event)))
                .is_ok()
        });
        self.frontier.send_replace(event.upper.clone());
        if state.subscribers.is_empty() {
            state.closed = true;
            return false;
        }
        true
    }

    /// Lets the snapshot reader's since follow the collection's since. Every
    /// subscriber holds a read hold on the collection until its snapshot is
    /// taken, so the collection's since is always early enough for them.
    fn downgrade_snapshot_since(&self, collections: &Mutex<BTreeMap<GlobalId, CollectionState>>) {
        let since = {
            let collections = collections.lock().expect("lock poisoned");
            collections
                .get(&self.id)
                .map(|c| c.read_capabilities.frontier().to_owned())
        };
        if let Some(since) = since {
            let _ = self
                .snapshot_tx
                .send(SnapshotRequest::DowngradeSince(since));
        }
    }

    /// The subscriber's snapshot at `as_of` through the shared snapshot reader.
    ///
    /// Waiting for the shard's upper to pass `as_of`, which a subscriber whose
    /// `as_of` came from the timestamp oracle usually has to, happens here
    /// rather than in the snapshot task, where it would hold up every other
    /// subscriber's snapshot behind it.
    async fn snapshot(&self, as_of: Timestamp) -> Result<SnapshotCursor, StorageError> {
        let started = Instant::now();
        let as_of_frontier = Antichain::from_elem(as_of);
        let mut frontier = self.frontier.subscribe();
        frontier
            .wait_for(|frontier| !PartialOrder::less_equal(frontier, &as_of_frontier))
            .await
            .map_err(|_| StorageError::ReadBeforeSince(self.id))?;
        let frontier_wait_ms = started.elapsed().as_millis();

        let queued = Instant::now();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.snapshot_tx
            .send(SnapshotRequest::Snapshot {
                as_of,
                reply: reply_tx,
            })
            .map_err(|_| StorageError::ReadBeforeSince(self.id))?;
        let cursor = reply_rx
            .await
            .map_err(|_| StorageError::ReadBeforeSince(self.id))??;
        debug!(
            id = %self.id,
            %as_of,
            frontier_wait_ms,
            queue_wait_ms = queued.elapsed().as_millis(),
            "shared tail snapshot leased"
        );
        Ok(cursor)
    }

    /// The stream for a subscriber that joined at `as_of`: its snapshot, the
    /// retained events, then the live events, each filtered to what lies past
    /// `as_of`.
    ///
    /// The snapshot's cursor is leased before this returns, so a subscriber
    /// that never polls still cannot be served an `as_of` the shard has
    /// compacted away. Its updates are fetched and decoded as the stream is
    /// polled, which is what keeps a large snapshot out of memory, and its
    /// lease holds the parts it has yet to read until then.
    async fn joined_stream(
        &self,
        as_of: Timestamp,
        with_snapshot: bool,
        snapshot_chunk: usize,
        joined: Joined,
    ) -> Result<BoxStream<'static, SubscribeEvent>, StorageError> {
        let Joined {
            mut rx,
            queued_bytes,
            queued_events,
            replay,
        } = joined;
        let mut cursor = match with_snapshot {
            true => Some(self.snapshot(as_of).await?),
            false => None,
        };
        // The first piece is fetched here rather than on the first poll, so a
        // snapshot that fits in one piece is in hand when the subscriber
        // first fetches, as a dataflow's first batch would be. Persist bounds
        // a piece, which is what keeps this from reading a whole large
        // snapshot.
        let primed = next_piece(cursor.as_mut()).await;
        let stream = async_stream::stream! {
            let mut piece = primed;
            while let Some(updates) = piece {
                let mut updates = updates.into_iter();
                loop {
                    let chunk: Vec<Update> = updates.by_ref().take(snapshot_chunk).collect();
                    if chunk.is_empty() {
                        break;
                    }
                    yield SubscribeEvent::SnapshotChunk(Arc::new(chunk));
                }
                piece = next_piece(cursor.as_mut()).await;
            }
            // Give up the lease as soon as the snapshot is delivered.
            drop(cursor);

            for event in replay {
                for out in past_as_of(&event, as_of) {
                    yield out;
                }
            }
            while let Some(item) = rx.recv().await {
                let event = match item {
                    TailItem::Event(event) => event,
                    TailItem::Detached {
                        buffered_bytes,
                        max_buffered_bytes,
                    } => {
                        yield SubscribeEvent::Detached {
                            buffered_bytes,
                            max_buffered_bytes,
                        };
                        return;
                    }
                };
                let footprint = event.bytes + QUEUED_EVENT_OVERHEAD_BYTES;
                queued_bytes.fetch_sub(footprint, Ordering::Relaxed);
                queued_events.fetch_sub(1, Ordering::Relaxed);
                let closed = event.upper.is_empty();
                for out in past_as_of(&event, as_of) {
                    yield out;
                }
                if closed {
                    return;
                }
            }
        };
        Ok(stream.boxed())
    }
}

impl TailSubscriber {
    fn new(max_buffered_bytes: usize) -> (Self, Joined) {
        let (tx, rx) = mpsc::unbounded_channel();
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let queued_events = Arc::new(AtomicUsize::new(0));
        let subscriber = TailSubscriber {
            tx,
            queued_bytes: Arc::clone(&queued_bytes),
            queued_events: Arc::clone(&queued_events),
            max_buffered_bytes,
        };
        let joined = Joined {
            rx,
            queued_bytes,
            queued_events,
            replay: Vec::new(),
        };
        (subscriber, joined)
    }
}

/// Serves the snapshot requests of one shared tail from its snapshot reader.
///
/// Leasing a snapshot's parts is quick once the shard's upper is past the
/// `as_of`, which the requester has waited for. The parts are fetched by the
/// returned stream, off this task. Exits once the tail is gone, which expires
/// the reader.
/// The cursor's next piece of the snapshot, `None` once it is exhausted or
/// when there is no snapshot to read. A piece is consolidated and in key
/// order, and persist bounds its size.
async fn next_piece(cursor: Option<&mut SnapshotCursor>) -> Option<Vec<Update>> {
    let updates = cursor?.next().await?;
    Some(updates.map(|((data, ()), t, d)| (data, t, d)).collect())
}

async fn run_snapshotter(
    id: GlobalId,
    mut handle: ReadHandle<SourceData, (), Timestamp, StorageDiff>,
    txns: Option<(TxnsRead<Timestamp>, ShardId)>,
    mut requests: mpsc::UnboundedReceiver<SnapshotRequest>,
) {
    while let Some(request) = requests.recv().await {
        match request {
            SnapshotRequest::Snapshot { as_of, reply } => {
                if let Some((txns_read, data_id)) = &txns {
                    // The shard's physical upper only moves when a write is
                    // applied, so the snapshot has to be unblocked first.
                    txns_read.update_gt(as_of).await;
                    let data_snapshot = txns_read.data_snapshot(*data_id, as_of).await;
                    let unblock = WriteHandle::from_read(&handle, "subscribe unblock");
                    data_snapshot.unblock_read(unblock).await;
                }
                // The cursor's lease keeps the parts it will read alive, so
                // the reader's since may move on while a subscriber is still
                // working through its snapshot.
                let cursor = handle
                    .snapshot_cursor(Antichain::from_elem(as_of), |_| true)
                    .await
                    .map_err(|_| StorageError::ReadBeforeSince(id));
                // The requester may have given up, which is fine.
                let _ = reply.send(cursor);
            }
            SnapshotRequest::DowngradeSince(since) => {
                if PartialOrder::less_than(handle.since(), &since) {
                    handle.downgrade_since(&since).await;
                }
            }
        }
    }
}

/// The parts of `event` a subscriber at `as_of` has not seen through its
/// snapshot: updates at times past `as_of`, and the frontier if it is past
/// `as_of`.
fn past_as_of(event: &TailEvent, as_of: Timestamp) -> Vec<SubscribeEvent> {
    let as_of_frontier = Antichain::from_elem(as_of);
    let mut out = Vec::with_capacity(2);
    if !event.updates.is_empty() {
        // Times in the event lie in `[lower, upper)`. A lower past `as_of`
        // means every update passes, so the decoded updates are shared as is.
        if !event.lower.less_equal(&as_of) {
            out.push(SubscribeEvent::Updates(Arc::clone(&event.updates)));
        } else if !event.upper.less_equal(&as_of.step_forward()) {
            let updates: Vec<_> = event
                .updates
                .iter()
                .filter(|(_, t, _)| *t > as_of)
                .cloned()
                .collect();
            if !updates.is_empty() {
                out.push(SubscribeEvent::Updates(Arc::new(updates)));
            }
        }
    }
    if !PartialOrder::less_equal(&event.upper, &as_of_frontier) {
        out.push(SubscribeEvent::Progress(event.upper.clone()));
    }
    out
}

fn update_bytes((data, _, _): &Update) -> usize {
    match &data.0 {
        Ok(row) => row.byte_len(),
        Err(_) => 0,
    }
}

enum RawEvent {
    Updates(Vec<Update>),
    Progress(Antichain<Timestamp>),
}

/// Turns a persist listen of a collection's shard into a stream of updates and
/// strictly advancing progress, ending after an empty frontier.
///
/// `remap_rx` is `Some` for txn-wal backed collections. Their shard's upper
/// only moves when a write is applied, so progress between writes comes from
/// the txns shard: once the listen has read everything below an entry's
/// `physical_upper`, the interval up to its `logical_upper` is known to be
/// empty and the frontier can jump there. This mirrors `txns_progress`.
fn raw_stream(
    listen: Listen<SourceData, (), Timestamp, StorageDiff>,
    remap_rx: Option<mpsc::UnboundedReceiver<DataRemapEntry<Timestamp>>>,
) -> impl Stream<Item = RawEvent> + Send + 'static {
    enum Event {
        Listen(Vec<Update>, Antichain<Timestamp>),
        Remap(DataRemapEntry<Timestamp>),
    }

    async fn fetch(
        listen: &mut Listen<SourceData, (), Timestamp, StorageDiff>,
        part: LeasedBatchPart<Timestamp>,
    ) -> Vec<Update> {
        listen
            .fetch_batch_part(part)
            .await
            .map(|((data, ()), t, d)| (data, t, d))
            .collect()
    }

    async_stream::stream! {
        // `Listen::next` is not cancel safe, so drive it through an `unfold`
        // that keeps its future across polls rather than a `select!` that would
        // drop it whenever a remap entry arrives.
        let listen_stream = futures::stream::unfold(Some(listen), |listen| async move {
            let mut listen = listen?;
            let (parts, progress) = listen.next(None).await;
            let mut updates = Vec::new();
            for part in parts {
                updates.extend(fetch(&mut listen, part).await);
            }
            // Nothing follows the empty frontier, and calling `next` again
            // would wait forever.
            let listen = (!progress.is_empty()).then_some(listen);
            Some((Event::Listen(updates, progress), listen))
        });
        let remap_stream = match remap_rx {
            Some(rx) => futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|entry| (Event::Remap(entry), rx))
            })
            .boxed(),
            None => futures::stream::empty().boxed(),
        };
        let mut events = futures::stream::select(listen_stream.boxed(), remap_stream);

        let mut physical = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut remap: Option<DataRemapEntry<Timestamp>> = None;
        let mut emitted = Antichain::from_elem(TimelyTimestamp::minimum());
        while let Some(event) = events.next().await {
            match event {
                Event::Listen(updates, progress) => {
                    if !updates.is_empty() {
                        yield RawEvent::Updates(updates);
                    }
                    physical = progress;
                }
                Event::Remap(entry) => {
                    // Entries are not assumed to arrive in order. Keep the one
                    // with the largest logical upper.
                    if remap.as_ref().is_none_or(|r| r.logical_upper < entry.logical_upper) {
                        remap = Some(entry);
                    }
                }
            }

            let mut progress = physical.clone();
            if let (Some(remap), Some(physical)) = (&remap, physical.as_option()) {
                if remap.physical_upper <= *physical && *physical < remap.logical_upper {
                    progress = Antichain::from_elem(remap.logical_upper);
                }
            }
            if PartialOrder::less_than(&emitted, &progress) {
                emitted = progress.clone();
                let closed = progress.is_empty();
                yield RawEvent::Progress(progress);
                if closed {
                    return;
                }
            }
        }
    }
}
