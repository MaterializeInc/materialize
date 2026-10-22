// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Forwarding of batched subscribe batches to the client.
//!
//! The coordinator hands each accepted batch to the subscribe's [`Forwarder`], whose task turns
//! the batch into client rows. Stashed shares are read back from persist in that task, so their
//! size never passes through the coordinator. The task applies backpressure by waiting on the
//! client's backlog, which the coordinator loop cannot do.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use mz_compute_client::controller::{AcceptedBatch, AcceptedShare};
use mz_compute_client::stash::{OwnedStash, StashBatch};
use mz_expr::row::{RowCollection, RowCollectionBuilder};
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::read::Cursor;
use mz_persist_client::{PersistClient, Schemas};
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, IntoRowIterator, RelationDesc, Row, RowRef, Timestamp};
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::AdapterError;
use crate::active_compute_sink::{
    SubscribeBacklogAccounting, SubscribeRowFormat, send_last_to_client, send_to_client,
};
use crate::coord::peek::PeekResponseUnary;

/// How a forwarder ended the client's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwarderEvent {
    /// The subscribe reached the empty frontier and every update was sent.
    Finished(GlobalId),
    /// The subscribe's error was sent to the client, and nothing follows it.
    Errored(GlobalId),
}

/// The handle to the task that forwards the batches of one batched subscribe to its client.
///
/// Dropping the handle aborts the task. Every stashed share it was sent and has not finished
/// reading is then deleted.
///
/// NOTE: An error update in a stashed share is found only while the share is read. Rows of the
/// same batch can reach the client before that error, and no progress row follows it.
pub(crate) struct Forwarder {
    batches: mpsc::UnboundedSender<QueuedBatch>,
    unread_stash_bytes: Arc<AtomicUsize>,
    unread_inline: Arc<Mutex<UnreadInline>>,
    _task: AbortOnDropHandle<()>,
}

impl fmt::Debug for Forwarder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Forwarder")
            .field("unread_stash_bytes", &self.unread_stash_bytes())
            .field("unread_inline_bytes", &self.unread_inline_bytes())
            .finish_non_exhaustive()
    }
}

impl Forwarder {
    /// Spawns the forwarding task of sink `sink_id`.
    ///
    /// Stashed shares are read with `output_desc` as their key schema. Client rows are sent in
    /// messages of about a chunk size, the smaller of `read_batch_bytes` and a quarter of
    /// `max_buffered_bytes`, and each one waits until `backlog` holds at most a chunk. The task
    /// reports to `events` once it has ended the stream.
    ///
    /// Must be called from within a tokio runtime.
    pub(crate) fn spawn<E: From<ForwarderEvent> + Send + 'static>(
        sink_id: GlobalId,
        format: SubscribeRowFormat,
        output_desc: RelationDesc,
        persist_client: PersistClient,
        channel: mpsc::UnboundedSender<PeekResponseUnary>,
        backlog: Arc<Mutex<SubscribeBacklogAccounting>>,
        read_batch_bytes: usize,
        read_memory_budget_bytes: usize,
        max_buffered_bytes: usize,
        events: mpsc::UnboundedSender<E>,
    ) -> Forwarder {
        // A client that keeps up holds about one chunk in its backlog, the message queued behind
        // the one it drains. The coordinator retires the subscribe once that backlog plus the
        // queued inline shares exceeds `max_buffered_bytes`, so a chunk the size of the budget
        // would retire a client that keeps up as soon as any inline share queues. A quarter
        // leaves the rest of the budget to queued inline shares.
        let chunk_bytes = std::cmp::min(read_batch_bytes, max_buffered_bytes / 4);
        let (batches_tx, batches_rx) = mpsc::unbounded_channel();
        let task = ForwarderTask {
            format,
            read_schemas: Schemas {
                id: None,
                key: Arc::new(output_desc),
                val: Arc::new(UnitSchema),
            },
            persist_client,
            channel,
            backlog,
            chunk_bytes,
            read_memory_budget_bytes,
        };
        let handle = mz_ore::task::spawn(
            || format!("batched_subscribe_forwarder({sink_id})"),
            async move {
                if let Some(event) = task.run(sink_id, batches_rx).await {
                    let _ = events.send(event.into());
                }
            },
        );
        Forwarder {
            batches: batches_tx,
            unread_stash_bytes: Arc::new(AtomicUsize::new(0)),
            unread_inline: Arc::new(Mutex::new(UnreadInline::default())),
            _task: handle.abort_on_drop(),
        }
    }

    /// Queues `batch` for the client. Batches are forwarded in the order they are sent.
    ///
    /// A batch sent after the stream ended is dropped, which deletes its stashed shares.
    pub(crate) fn send(&self, batch: AcceptedBatch) {
        let AcceptedBatch {
            lower: _,
            upper,
            shares,
            error,
        } = batch;
        let inline_bytes = shares
            .iter()
            .map(|share| match share {
                AcceptedShare::Inline(updates) => updates.byte_len(),
                AcceptedShare::Stashed { .. } => 0,
            })
            .sum();
        let inline = UnreadInlineBatch::new(inline_bytes, Arc::clone(&self.unread_inline));
        let shares = shares
            .into_iter()
            .map(|share| match share {
                AcceptedShare::Inline(updates) => QueuedShare::Inline { updates },
                AcceptedShare::Stashed {
                    shard_id,
                    stash,
                    from,
                } => {
                    let unread = UnreadBytes::new(
                        stash.encoded_size_bytes(),
                        Arc::clone(&self.unread_stash_bytes),
                    );
                    QueuedShare::Stashed {
                        shard_id,
                        stash,
                        from,
                        unread,
                    }
                }
            })
            .collect();
        let _ = self.batches.send(QueuedBatch {
            upper,
            shares,
            error,
            inline,
        });
    }

    /// The encoded size of the stashed shares sent to the forwarder and not yet fully read.
    ///
    /// These are held in blob storage. The caller retires the subscribe with
    /// `SubscribeFellBehind` once they exceed `batched_subscribe_max_unread_stash_bytes`.
    pub(crate) fn unread_stash_bytes(&self) -> usize {
        self.unread_stash_bytes.load(Ordering::SeqCst)
    }

    /// The size of the inline shares of the batches queued behind the one the forwarder is
    /// formatting.
    ///
    /// These are held in environmentd memory. The caller retires the subscribe with
    /// `SubscribeFellBehind` once `backlog_size + unread_inline_bytes` exceeds
    /// `subscribe_max_buffered_bytes`. The batch being formatted is tolerated whatever its size,
    /// as `backlog_size` tolerates the message the client drains. The batches behind it count,
    /// because nothing backpressures the replicas that produce them.
    pub(crate) fn unread_inline_bytes(&self) -> usize {
        self.unread_inline
            .lock()
            .expect("unread inline accounting poisoned")
            .queued_bytes()
    }
}

/// An [`AcceptedBatch`] in the forwarder's queue.
struct QueuedBatch {
    upper: Antichain<Timestamp>,
    shares: Vec<QueuedShare>,
    error: Option<String>,
    /// Dropped once the batch is forwarded or discarded.
    inline: UnreadInlineBatch,
}

enum QueuedShare {
    Inline {
        updates: mz_repr::UpdateCollection,
    },
    Stashed {
        shard_id: ShardId,
        stash: OwnedStash,
        from: Antichain<Timestamp>,
        /// Dropped once the share is fully read or discarded.
        unread: UnreadBytes,
    },
}

/// The inline bytes of every batch sent to the forwarder and not yet forwarded or discarded.
#[derive(Default)]
struct UnreadInline {
    /// Inline bytes by batch, keyed by send order.
    batches: BTreeMap<u64, usize>,
    /// The key of the next batch sent.
    next: u64,
    /// Sum of `batches`.
    total: usize,
}

impl UnreadInline {
    /// The inline bytes of every batch but the oldest, which is the one being formatted.
    fn queued_bytes(&self) -> usize {
        let head = self.batches.values().next().copied().unwrap_or(0);
        self.total - head
    }
}

/// A batch's entry in [`UnreadInline`], removed on drop.
struct UnreadInlineBatch {
    key: u64,
    unread: Arc<Mutex<UnreadInline>>,
}

impl UnreadInlineBatch {
    fn new(bytes: usize, unread: Arc<Mutex<UnreadInline>>) -> Self {
        let key = {
            let mut guard = unread.lock().expect("unread inline accounting poisoned");
            let key = guard.next;
            guard.next += 1;
            guard.batches.insert(key, bytes);
            guard.total += bytes;
            key
        };
        UnreadInlineBatch { key, unread }
    }
}

impl Drop for UnreadInlineBatch {
    fn drop(&mut self) {
        let mut guard = self
            .unread
            .lock()
            .expect("unread inline accounting poisoned");
        if let Some(bytes) = guard.batches.remove(&self.key) {
            guard.total -= bytes;
        }
    }
}

/// A share's contribution to [`Forwarder::unread_stash_bytes`], withdrawn on drop.
struct UnreadBytes {
    bytes: usize,
    total: Arc<AtomicUsize>,
}

impl UnreadBytes {
    fn new(bytes: usize, total: Arc<AtomicUsize>) -> Self {
        total.fetch_add(bytes, Ordering::SeqCst);
        UnreadBytes { bytes, total }
    }
}

impl Drop for UnreadBytes {
    fn drop(&mut self) {
        self.total.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

type StashCursor = Cursor<SourceData, (), Timestamp, i64, Vec<StashBatch>>;

/// A read of stashed batches that deletes them when dropped, wherever the read stopped.
///
/// The cursor owns the batches while it reads, and persist's `Drop for Batch` only logs, so an
/// aborted forwarder would leak them without this.
struct StashRead {
    cursor: Option<StashCursor>,
    runtime: Handle,
}

impl StashRead {
    fn cursor(&mut self) -> &mut StashCursor {
        self.cursor.as_mut().expect("present until dropped")
    }
}

impl Drop for StashRead {
    fn drop(&mut self) {
        if let Some(cursor) = self.cursor.take() {
            drop(OwnedStash::from_batches(
                cursor.into_lease(),
                self.runtime.clone(),
            ));
        }
    }
}

/// Why a forwarder stopped forwarding.
enum Stop {
    /// The stream ended with this event.
    Ended(ForwarderEvent),
    /// The client went away.
    ClientGone,
}

struct ForwarderTask {
    format: SubscribeRowFormat,
    read_schemas: Schemas<SourceData, ()>,
    persist_client: PersistClient,
    channel: mpsc::UnboundedSender<PeekResponseUnary>,
    backlog: Arc<Mutex<SubscribeBacklogAccounting>>,
    /// The target size of a client message, and the backlog a send waits for.
    chunk_bytes: usize,
    read_memory_budget_bytes: usize,
}

impl ForwarderTask {
    /// Forwards batches until the stream ends, and returns how it ended.
    ///
    /// Returns `None` if the client or the [`Forwarder`] went away first.
    async fn run(
        mut self,
        sink_id: GlobalId,
        mut batches: mpsc::UnboundedReceiver<QueuedBatch>,
    ) -> Option<ForwarderEvent> {
        while let Some(batch) = batches.recv().await {
            match self.forward(sink_id, batch).await {
                Ok(()) => {}
                Err(Stop::Ended(event)) => return Some(event),
                Err(Stop::ClientGone) => return None,
            }
        }
        None
    }

    async fn forward(&mut self, sink_id: GlobalId, batch: QueuedBatch) -> Result<(), Stop> {
        let QueuedBatch {
            upper,
            shares,
            error,
            inline: _inline,
        } = batch;
        if let Some(error) = error {
            return Err(self.error(sink_id, error));
        }

        let mut chunk = Chunk::default();
        for share in shares {
            match share {
                QueuedShare::Inline { updates } => {
                    for (row, time, diff) in updates.iter() {
                        if chunk.push(&self.format, row, *time, diff) > self.chunk_bytes {
                            self.flush(&mut chunk).await?;
                        }
                    }
                }
                QueuedShare::Stashed {
                    shard_id,
                    stash,
                    from,
                    unread,
                } => {
                    self.forward_stashed(sink_id, &mut chunk, shard_id, stash, from)
                        .await?;
                    drop(unread);
                }
            }
        }
        self.flush(&mut chunk).await?;

        if let Some(progress) = self.format.progress_row(&upper) {
            self.send_rows(progress).await?;
        }
        if upper.is_empty() {
            return Err(Stop::Ended(ForwarderEvent::Finished(sink_id)));
        }
        Ok(())
    }

    /// Reads one stashed share into `chunk`, flushing as it fills.
    async fn forward_stashed(
        &mut self,
        sink_id: GlobalId,
        chunk: &mut Chunk,
        shard_id: ShardId,
        stash: OwnedStash,
        from: Antichain<Timestamp>,
    ) -> Result<(), Stop> {
        let runtime = Handle::current();
        let cursor = self
            .persist_client
            .read_batches_consolidated_from::<_, _, _, i64>(
                shard_id,
                from,
                self.read_schemas.clone(),
                stash.take(),
                |_stats| true,
                self.read_memory_budget_bytes,
            )
            .await
            .expect("a range read has no since to violate");
        let mut read = StashRead {
            cursor: Some(cursor),
            runtime,
        };

        while let Some(updates) = read.cursor().next().await {
            for ((source_data, ()), time, diff) in updates {
                let row = match source_data.0 {
                    Ok(row) => row,
                    Err(error) => return Err(self.error(sink_id, error.to_string())),
                };
                if chunk.push(&self.format, &row, time, Diff::from(diff)) > self.chunk_bytes {
                    self.flush(chunk).await?;
                }
            }
        }
        // Dropping `read` deletes the batches.
        Ok(())
    }

    /// Sends the subscribe's error, which ends the stream.
    fn error(&self, sink_id: GlobalId, error: String) -> Stop {
        send_last_to_client(
            &self.channel,
            &self.backlog,
            PeekResponseUnary::Error(AdapterError::Unstructured(anyhow::Error::msg(error))),
        );
        Stop::Ended(ForwarderEvent::Errored(sink_id))
    }

    /// Sends the rows in `chunk`, if any.
    async fn flush(&self, chunk: &mut Chunk) -> Result<(), Stop> {
        if chunk.bytes == 0 {
            return Ok(());
        }
        let rows = std::mem::take(chunk).builder.build();
        self.send_rows(rows).await
    }

    /// Sends `rows` once the client's backlog has room for them.
    async fn send_rows(&self, rows: RowCollection) -> Result<(), Stop> {
        tokio::select! {
            () = SubscribeBacklogAccounting::wait_until_at_most(
                &self.backlog,
                self.chunk_bytes,
            ) => {}
            // A client that went away never drains its backlog.
            () = self.channel.closed() => return Err(Stop::ClientGone),
        }
        let bytes = rows.byte_len();
        send_to_client(
            &self.channel,
            &self.backlog,
            PeekResponseUnary::Rows(Box::new(rows.into_row_iter())),
            bytes,
        );
        Ok(())
    }
}

/// Client rows collected for one message.
struct Chunk {
    builder: RowCollectionBuilder,
    buf: Row,
    bytes: usize,
}

impl Default for Chunk {
    fn default() -> Self {
        Chunk {
            builder: RowCollection::builder(0, 0),
            buf: Row::default(),
            bytes: 0,
        }
    }
}

impl Chunk {
    /// Adds the client row of an update, and returns the chunk's size afterwards.
    fn push(
        &mut self,
        format: &SubscribeRowFormat,
        row: &RowRef,
        time: Timestamp,
        diff: Diff,
    ) -> usize {
        format.push_diff_row(&mut self.builder, &mut self.buf, row, time, diff);
        self.bytes = self.bytes.saturating_add(self.buf.byte_len());
        self.bytes
    }
}

#[cfg(test)]
mod tests;
