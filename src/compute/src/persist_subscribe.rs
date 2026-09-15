// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serving `SUBSCRIBE` on a replica by tailing a persist shard, without a
//! dataflow, see [`ComputeCommand::Subscribe`].
//!
//! Like a persist peek, a subscribe runs as a task rather than a dataflow.
//! Unlike a peek it is long-lived: it tails the shard through a
//! [`SharedTails`] registry that one listen and one snapshot reader per
//! collection are shared through, cuts the events into the batches a subscribe
//! sink would produce, and pushes them into the worker's response channel until
//! the controller ends it with compaction to the empty frontier.
//!
//! [`ComputeCommand::Subscribe`]: mz_compute_client::protocol::command::ComputeCommand::Subscribe

use std::collections::BTreeMap;
use std::sync::Arc;

use differential_dataflow::Hashable;
use futures::{FutureExt, StreamExt};
use mz_compute_client::persist_subscribe::PersistTailBatcher;
use mz_compute_client::protocol::command::PersistSubscribe;
use mz_compute_client::protocol::response::{ComputeResponse, SubscribeBatch, SubscribeResponse};
use mz_ore::cast::CastFrom;
use mz_ore::task::JoinHandle;
use mz_persist_client::ShardId;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::storage_collections::{
    SharedTails, SubscribeEvent, SubscribeLimits, open_tail_handles,
};
use mz_storage_types::controller::TxnsCodecRow;
use mz_txn_wal::txn_read::TxnsRead;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use tokio::sync::{Mutex, oneshot};
use tracing::debug;

use crate::server::ResponseSender;

/// The persist-backed subscribes a worker knows about.
///
/// Every worker records every subscribe, so the compaction command that ends
/// it finds it wherever it lands, but only the worker chosen for the target
/// collection runs a task. Choosing by collection rather than by subscribe
/// puts all subscribes of one collection on one worker, where they share its
/// tail.
///
/// The controller merges a subscribe's responses expecting one stream per
/// process, as a subscribe sink emits from one worker. So exactly one worker
/// per process speaks for a subscribe: the chosen worker in its process, and
/// the first worker of every other process, which reports the empty frontier
/// at once so the merge is driven by the chosen worker alone.
#[derive(Default)]
pub(crate) struct PersistSubscribes {
    tails: Arc<SharedTails>,
    /// One txns reader per txns shard, shared by every txn-wal backed
    /// collection subscribed on this worker.
    txns_reads: Arc<Mutex<BTreeMap<ShardId, TxnsRead<Timestamp>>>>,
    running: BTreeMap<GlobalId, Running>,
}

enum Running {
    /// Served by another worker of this replica.
    Elsewhere,
    /// Served by a task on this worker. Dropping `cancel` asks the task to end
    /// with a `DroppedAt`; aborting `task` ends it silently.
    Here {
        cancel: Option<oneshot::Sender<()>>,
        task: JoinHandle<()>,
    },
}

impl PersistSubscribes {
    /// Starts serving `subscribe`, on this worker if it is the one chosen for
    /// the target collection.
    pub(crate) fn start(
        &mut self,
        subscribe: PersistSubscribe,
        persist_clients: Arc<PersistClientCache>,
        response_tx: ResponseSender,
        worker_index: usize,
        worker_peers: usize,
        max_result_size: usize,
        snapshot_chunk: usize,
    ) {
        let id = subscribe.id;
        let chosen = usize::cast_from(subscribe.target.hashed()) % worker_peers;
        if chosen != worker_index {
            let done = SubscribeBatch {
                lower: Antichain::from_elem(Timestamp::minimum()),
                upper: Antichain::new(),
                updates: Ok(Vec::new()),
            };
            let _ = response_tx.send(ComputeResponse::SubscribeResponse(
                id,
                SubscribeResponse::Batch(done),
            ));
            self.running.insert(id, Running::Elsewhere);
            return;
        }

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let task = mz_ore::task::spawn(
            || format!("persist-subscribe-{id}"),
            run(
                subscribe,
                Arc::clone(&self.tails),
                Arc::clone(&self.txns_reads),
                persist_clients,
                response_tx,
                max_result_size,
                snapshot_chunk,
                cancel_rx,
            ),
        );
        self.running.insert(
            id,
            Running::Here {
                cancel: Some(cancel_tx),
                task,
            },
        );
    }

    /// Whether `id` is a subscribe this worker knows about.
    pub(crate) fn contains(&self, id: GlobalId) -> bool {
        self.running.contains_key(&id)
    }

    /// Ends the subscribe `id`, if this worker knows about it, and says so.
    /// The task answers with a `DroppedAt` unless it has already produced its
    /// final batch, which is what the protocol asks for.
    pub(crate) fn end(&mut self, id: GlobalId) -> bool {
        match self.running.remove(&id) {
            None => false,
            Some(Running::Elsewhere) => true,
            Some(Running::Here { cancel, task: _ }) => {
                drop(cancel);
                true
            }
        }
    }

    /// Aborts every subscribe without a word, for reconciliation: the response
    /// stream is reformed and the controller reissues what it still wants.
    pub(crate) fn abort_all(&mut self) {
        for (_, running) in std::mem::take(&mut self.running) {
            if let Running::Here { task, .. } = running {
                task.into_tokio_handle().abort();
            }
        }
    }
}

/// The task behind one subscribe: joins the target's tail, batches its events,
/// and pushes them into the response channel until the stream ends, the
/// controller cancels, or the client falls behind for good.
async fn run(
    subscribe: PersistSubscribe,
    tails: Arc<SharedTails>,
    txns_reads: Arc<Mutex<BTreeMap<ShardId, TxnsRead<Timestamp>>>>,
    persist_clients: Arc<PersistClientCache>,
    response_tx: ResponseSender,
    max_result_size: usize,
    snapshot_chunk: usize,
    mut cancel_rx: oneshot::Receiver<()>,
) {
    let PersistSubscribe {
        id,
        target,
        metadata,
        as_of,
        up_to,
        with_snapshot,
        order,
        chunk_snapshot,
        max_buffered_bytes,
    } = subscribe;
    let respond = |response: SubscribeResponse| {
        let _ = response_tx.send(ComputeResponse::SubscribeResponse(id, response));
    };

    // The shard's upper only moves when a write is applied, so a txn-wal
    // backed collection is read through the txns shard, one reader per worker.
    let txns_read = match metadata.txns_shard {
        None => None,
        Some(txns_id) => {
            let mut reads = txns_reads.lock().await;
            match reads.get(&txns_id) {
                Some(read) => Some(read.clone()),
                None => {
                    let client = persist_clients
                        .open(metadata.persist_location.clone())
                        .await
                        .expect("invalid persist usage");
                    let read = TxnsRead::start::<TxnsCodecRow>(client, txns_id).await;
                    reads.insert(txns_id, read.clone());
                    Some(read)
                }
            }
        }
    };

    let limits = SubscribeLimits {
        max_buffered_bytes,
        snapshot_chunk,
    };
    let attach = |as_of: Timestamp, with_snapshot: bool| {
        let tails = Arc::clone(&tails);
        let persist = Arc::clone(&persist_clients);
        let metadata = metadata.clone();
        let txns_read = txns_read.clone();
        async move {
            let open =
                move |as_of| open_tail_handles(persist, metadata, target, txns_read, as_of).boxed();
            tails.join(target, as_of, with_snapshot, limits, open).await
        }
    };

    let mut batcher = PersistTailBatcher::new(
        as_of,
        up_to,
        with_snapshot,
        order,
        max_result_size,
        chunk_snapshot,
    );
    let mut stream = match attach(as_of, with_snapshot).await {
        Ok(stream) => stream,
        Err(error) => {
            respond(error_batch(&batcher, error.to_string()));
            return;
        }
    };
    debug!(%id, %target, %as_of, "persist subscribe attached");

    loop {
        let event = tokio::select! {
            _ = &mut cancel_rx => {
                respond(SubscribeResponse::DroppedAt(batcher.frontier().clone()));
                return;
            }
            event = stream.next() => event,
        };
        match event {
            Some(SubscribeEvent::Updates(updates)) => batcher.push(&updates),
            Some(SubscribeEvent::SnapshotChunk(updates)) => {
                if let Some(batch) = batcher.push_snapshot_chunk(&updates) {
                    respond(SubscribeResponse::Batch(batch));
                }
            }
            Some(SubscribeEvent::Progress(upper)) => {
                for batch in batcher.progress(upper) {
                    let finished = batch.upper.is_empty();
                    respond(SubscribeResponse::Batch(batch));
                    if finished {
                        return;
                    }
                }
            }
            Some(SubscribeEvent::Detached {
                buffered_bytes,
                max_buffered_bytes,
            }) => {
                // The same message a client that falls behind gets today.
                let fell_behind = format!(
                    "SUBSCRIBE fell behind: the client did not read results fast enough, \
                     so its backlog reached {buffered_bytes} bytes, exceeding the \
                     {max_buffered_bytes} byte budget"
                );
                let Some((as_of, with_snapshot)) = batcher.resume_point() else {
                    respond(error_batch(&batcher, fell_behind));
                    return;
                };
                debug!(%id, %target, %as_of, "persist subscribe detached, resuming");
                match attach(as_of, with_snapshot).await {
                    Ok(resumed) => stream = resumed,
                    Err(_) => {
                        respond(error_batch(&batcher, fell_behind));
                        return;
                    }
                }
            }
            None => {
                if let Some(batch) = batcher.close() {
                    respond(SubscribeResponse::Batch(batch));
                }
                return;
            }
        }
    }
}

/// A batch that ends the subscribe with `error`. It advances to the empty
/// frontier, since the partitioned response merge only forwards a batch that
/// moves the frontier and the controller only forwards one past its own.
fn error_batch(batcher: &PersistTailBatcher, error: String) -> SubscribeResponse {
    SubscribeResponse::Batch(SubscribeBatch {
        lower: batcher.frontier().clone(),
        upper: Antichain::new(),
        updates: Err(error),
    })
}
