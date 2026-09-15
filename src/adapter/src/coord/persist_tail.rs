// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serving `SUBSCRIBE` by tailing a persist shard from `environmentd`.
//!
//! `SUBSCRIBE <collection>` on a table, source, or materialized view computes
//! nothing: the collection's persist shard already holds the snapshot and
//! receives every later update. A dataflow for it only re-reads that shard on
//! a cluster and forwards batches through the compute controller and the
//! coordinator loop. Instead, this module reads the shard directly through
//! [`StorageCollections::subscribe`] and formats rows in the session as the
//! client fetches them, so the client sees what a dataflow-backed subscribe
//! produces while no dataflow, cluster round trip, or coordinator loop turn is
//! on the data path.
//!
//! Fetching drives the reads. A [`PersistTailStream`] holds a frontier below
//! which everything has been formatted and handed to the session, and formats
//! more only when polled. A client that stops fetching costs only the queue of
//! decoded events the shared tail keeps for it. When that queue exceeds its
//! budget the tail drops it and the stream resumes from its frontier with a
//! new read of the shard, without a snapshot, so the collection's retained
//! history rather than memory bounds how far behind a client may fall. Only
//! once the collection has compacted past the frontier does the client get an
//! error.
//!
//! [`StorageCollections::subscribe`]: mz_storage_client::storage_collections::StorageCollections::subscribe

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mz_compute_client::persist_subscribe::PersistTailBatcher;
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::plan::{SubscribeFrom, SubscribePlan};
use mz_storage_client::storage_collections::{StorageCollections, SubscribeEvent};
use mz_storage_types::controller::StorageError;
use tokio::sync::mpsc;

use crate::AdapterError;
use crate::active_compute_sink::{ActiveComputeSinkRetireReason, SubscribeFormatter};
use crate::catalog::Catalog;
use crate::coord::Message;
use crate::coord::peek::PeekResponseUnary;

/// The collection a subscribe tails from persist, see [`persist_tail_source`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct PersistTailSource {
    pub from_id: GlobalId,
    /// The arity of the collection, which is what the subscribe exports.
    pub arity: usize,
}

/// Decides whether a subscribe takes the persist fast path: the flag is on and
/// the plan is `SUBSCRIBE <collection>` on an existing storage collection. A
/// query, a view, or a log source get a dataflow.
///
/// Both sequencing paths, the session task and the coordinator, use this so
/// they agree on what the fast path serves.
pub(crate) fn persist_tail_source(
    catalog: &Catalog,
    storage_collections: &dyn StorageCollections,
    plan: &SubscribePlan,
) -> Option<PersistTailSource> {
    if !catalog.system_config().enable_subscribe_persist_fast_path() {
        return None;
    }
    let SubscribeFrom::Id(from_id) = &plan.from else {
        return None;
    };
    storage_collections.check_exists(*from_id).ok()?;
    let arity = catalog
        .get_entry_by_global_id(from_id)
        .relation_desc()
        .expect("storage collections have a relation desc")
        .arity();
    Some(PersistTailSource {
        from_id: *from_id,
        arity,
    })
}

/// Opens a read of the subscribed collection, from `(as_of, with_snapshot)` to
/// a stream, see [`StorageCollections::subscribe`].
pub(crate) type AttachFn = Arc<
    dyn Fn(
            Timestamp,
            bool,
        ) -> BoxFuture<'static, Result<BoxStream<'static, SubscribeEvent>, StorageError>>
        + Send
        + Sync,
>;

/// The rows of a persist-tail subscribe, formatted as the session polls.
///
/// The first read is opened before the stream exists, off the coordinator
/// loop, and handed in through `attached`, so the subscribe holds its read
/// holds only until the collection is being read no matter when the client
/// first fetches. Later reads, after a detach, open on the poll that needs
/// them.
pub(crate) struct PersistTailStream {
    // `RowBatchStream` requires `Sync`. The storage stream and the futures
    // inside are not, and `poll_next` is the only access.
    inner: Mutex<Inner>,
}

struct Inner {
    sink_id: GlobalId,
    attach: AttachFn,
    batcher: PersistTailBatcher,
    formatter: SubscribeFormatter,
    /// Terminal messages from the coordinator: cancellation, dependency
    /// drops. The sender is the coordinator's `SubscribeEmitter`, so the
    /// channel closes when the sink is retired.
    control: mpsc::UnboundedReceiver<PeekResponseUnary>,
    /// Formatted messages not yet handed to the session.
    pending: VecDeque<PeekResponseUnary>,
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    source: Source,
    /// Nothing follows what is in `pending`.
    done: bool,
}

enum Source {
    Attaching {
        future: BoxFuture<'static, Result<BoxStream<'static, SubscribeEvent>, StorageError>>,
        /// `Some` when this resumes a cut-off stream, with the queue bytes and
        /// budget at the cut. A read before the collection's since then means
        /// the client fell behind for good.
        resuming: Option<(usize, usize)>,
    },
    Attached(BoxStream<'static, SubscribeEvent>),
    Closed,
}

impl PersistTailStream {
    /// `attached` resolves to the first read of the collection, opened by the
    /// caller. `attach` opens the later ones.
    pub fn new(
        sink_id: GlobalId,
        attached: BoxFuture<'static, Result<BoxStream<'static, SubscribeEvent>, StorageError>>,
        attach: AttachFn,
        batcher: PersistTailBatcher,
        formatter: SubscribeFormatter,
        control: mpsc::UnboundedReceiver<PeekResponseUnary>,
        internal_cmd_tx: mpsc::UnboundedSender<Message>,
    ) -> Self {
        let mut pending = VecDeque::new();
        if let Some((message, _bytes)) = formatter.initial_progress() {
            pending.push_back(message);
        }
        Self {
            inner: Mutex::new(Inner {
                sink_id,
                attach,
                batcher,
                formatter,
                control,
                pending,
                internal_cmd_tx,
                source: Source::Attaching {
                    future: attached,
                    resuming: None,
                },
                done: false,
            }),
        }
    }
}

impl Stream for PersistTailStream {
    type Item = PeekResponseUnary;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock().expect("persist tail stream poisoned");
        inner.poll_next(cx)
    }
}

impl Inner {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<PeekResponseUnary>> {
        loop {
            if let Some(message) = self.pending.pop_front() {
                return Poll::Ready(Some(message));
            }
            if self.done {
                return Poll::Ready(None);
            }
            // The coordinator's terminal message ends the stream whatever the
            // collection is doing.
            match self.control.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    self.finish();
                    return Poll::Ready(Some(message));
                }
                Poll::Ready(None) => {
                    self.finish();
                    continue;
                }
                Poll::Pending => {}
            }
            match &mut self.source {
                Source::Attaching { future, resuming } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => self.source = Source::Attached(stream),
                    Poll::Ready(Err(StorageError::ReadBeforeSince(_))) if resuming.is_some() => {
                        let (buffered_bytes, max_buffered_bytes) = resuming.expect("checked above");
                        tracing::debug!(
                            sink_id = %self.sink_id,
                            frontier = ?self.batcher.frontier(),
                            "persist tail cannot resume, history compacted"
                        );
                        self.pending.push_back(PeekResponseUnary::Error(
                            AdapterError::SubscribeHistoryCompacted {
                                buffered_bytes,
                                max_buffered_bytes,
                            },
                        ));
                        self.retire();
                    }
                    Poll::Ready(Err(error)) => {
                        self.pending
                            .push_back(PeekResponseUnary::Error(AdapterError::from(error)));
                        self.retire();
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Source::Attached(stream) => match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(SubscribeEvent::Updates(updates))) => {
                        self.batcher.push(&updates);
                    }
                    Poll::Ready(Some(SubscribeEvent::SnapshotChunk(updates))) => {
                        if let Some(batch) = self.batcher.push_snapshot_chunk(&updates) {
                            self.format(batch);
                        }
                    }
                    Poll::Ready(Some(SubscribeEvent::Progress(upper))) => {
                        for batch in self.batcher.progress(upper) {
                            self.format(batch);
                        }
                    }
                    Poll::Ready(Some(SubscribeEvent::Detached {
                        buffered_bytes,
                        max_buffered_bytes,
                    })) => {
                        match self.batcher.resume_point() {
                            Some((as_of, with_snapshot)) => {
                                tracing::debug!(
                                    sink_id = %self.sink_id,
                                    buffered_bytes,
                                    max_buffered_bytes,
                                    %as_of,
                                    "persist tail detached, resuming"
                                );
                                self.source = Source::Attaching {
                                    future: (self.attach)(as_of, with_snapshot),
                                    resuming: Some((buffered_bytes, max_buffered_bytes)),
                                };
                            }
                            // Part of the snapshot is out and its timestamp
                            // never completed, so there is no point to resume
                            // from without repeating rows.
                            None => {
                                tracing::debug!(
                                    sink_id = %self.sink_id,
                                    buffered_bytes,
                                    max_buffered_bytes,
                                    "persist tail detached mid-snapshot, cannot resume"
                                );
                                self.pending.push_back(PeekResponseUnary::Error(
                                    AdapterError::SubscribeFellBehind {
                                        buffered_bytes,
                                        max_buffered_bytes,
                                    },
                                ));
                                self.retire();
                            }
                        }
                    }
                    Poll::Ready(None) => {
                        if let Some(batch) = self.batcher.close() {
                            self.format(batch);
                        }
                        self.retire();
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Source::Closed => self.done = true,
            }
        }
    }

    /// Formats one batch into pending messages, retiring the sink after the
    /// last one.
    fn format(&mut self, batch: SubscribeBatch) {
        let formatted = self.formatter.format_batch(batch);
        for (message, _bytes) in formatted.messages {
            // A batch without rows still yields a message today, which is
            // nothing for a fetching client to wake up for.
            if let PeekResponseUnary::Rows(rows) = &message
                && rows.count() == 0
            {
                continue;
            }
            self.pending.push_back(message);
        }
        if formatted.finished {
            self.retire();
        }
    }

    /// Stops reading and asks the coordinator to retire the sink. Pending
    /// messages are still delivered, so the coordinator's own terminal message
    /// for `Finished`, which is none, is the right one.
    fn retire(&mut self) {
        if !matches!(self.source, Source::Closed) {
            // The coordinator ignores this if it already retired the sink.
            let _ = self.internal_cmd_tx.send(Message::RetireComputeSink {
                sink_id: self.sink_id,
                reason: ActiveComputeSinkRetireReason::Finished,
            });
        }
        self.finish();
    }

    fn finish(&mut self) {
        self.source = Source::Closed;
        self.done = true;
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use futures::stream;
    use mz_repr::{Datum, Row};
    use mz_sql::plan::SubscribeOutput;
    use mz_storage_types::sources::SourceData;
    use timely::progress::Antichain;

    use super::*;

    fn row(i: i64) -> SourceData {
        SourceData(Ok(Row::pack_slice(&[Datum::Int64(i)])))
    }

    fn formatter(as_of: u64) -> SubscribeFormatter {
        SubscribeFormatter {
            emit_progress: false,
            as_of: Timestamp::new(as_of),
            arity: 1,
            output: SubscribeOutput::Diffs,
        }
    }

    /// Collects the first column of every row message, in order.
    async fn drain(stream: &mut PersistTailStream) -> Vec<Result<Vec<i64>, String>> {
        let mut out = Vec::new();
        while let Some(message) = stream.next().await {
            match message {
                PeekResponseUnary::Rows(mut rows) => {
                    let mut ids = Vec::new();
                    while let Some(row) = rows.next() {
                        // Columns are mz_timestamp, mz_diff, then the row.
                        ids.push(
                            row.iter()
                                .nth(2)
                                .expect("row has the data column")
                                .unwrap_int64(),
                        );
                    }
                    out.push(Ok(ids));
                }
                PeekResponseUnary::Error(err) => out.push(Err(err.to_string())),
                other => panic!("unexpected message {other:?}"),
            }
        }
        out
    }

    /// A detached stream resumes below its shipped frontier and delivers the
    /// updates it had not shipped exactly once, then retires the sink when the
    /// collection closes.
    #[mz_ore::test(tokio::test)]
    async fn detach_resumes_from_frontier() {
        let as_of = Timestamp::new(10);
        let first = stream::iter(vec![
            SubscribeEvent::Updates(Arc::new(vec![(row(1), as_of, 1)])),
            SubscribeEvent::Progress(Antichain::from_elem(Timestamp::new(11))),
            // Pushed but not shipped when the tail cuts the queue.
            SubscribeEvent::Updates(Arc::new(vec![(row(2), Timestamp::new(11), 1)])),
            SubscribeEvent::Detached {
                buffered_bytes: 2,
                max_buffered_bytes: 1,
            },
        ]);
        let resumed_at = Arc::new(Mutex::new(Vec::new()));
        let attach: AttachFn = {
            let resumed_at = Arc::clone(&resumed_at);
            Arc::new(move |as_of, with_snapshot| {
                resumed_at
                    .lock()
                    .expect("lock poisoned")
                    .push((as_of, with_snapshot));
                let events = vec![
                    SubscribeEvent::Updates(Arc::new(vec![(row(2), Timestamp::new(11), 1)])),
                    SubscribeEvent::Progress(Antichain::from_elem(Timestamp::new(12))),
                    SubscribeEvent::Progress(Antichain::new()),
                ];
                Box::pin(async move { Ok(stream::iter(events).boxed()) })
            })
        };
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let mut stream = PersistTailStream::new(
            GlobalId::Transient(1),
            Box::pin(async move { Ok(first.boxed()) }),
            attach,
            PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, false),
            formatter(10),
            control_rx,
            cmd_tx,
        );

        assert_eq!(drain(&mut stream).await, vec![Ok(vec![1]), Ok(vec![2])]);
        assert_eq!(
            *resumed_at.lock().expect("lock poisoned"),
            vec![(Timestamp::new(10), false)]
        );
        assert!(matches!(
            cmd_rx.try_recv(),
            Ok(Message::RetireComputeSink {
                reason: ActiveComputeSinkRetireReason::Finished,
                ..
            })
        ));
    }

    /// Resuming below a frontier the collection has compacted past is the one
    /// case where a slow client gets an error.
    #[mz_ore::test(tokio::test)]
    async fn resume_past_since_errors() {
        let as_of = Timestamp::new(10);
        let first = stream::iter(vec![
            SubscribeEvent::Updates(Arc::new(vec![(row(1), as_of, 1)])),
            SubscribeEvent::Progress(Antichain::from_elem(Timestamp::new(11))),
            SubscribeEvent::Detached {
                buffered_bytes: 2,
                max_buffered_bytes: 1,
            },
        ]);
        let attach: AttachFn = Arc::new(|_, _| {
            Box::pin(async { Err(StorageError::ReadBeforeSince(GlobalId::Transient(1))) })
        });
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let mut stream = PersistTailStream::new(
            GlobalId::Transient(1),
            Box::pin(async move { Ok(first.boxed()) }),
            attach,
            PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, false),
            formatter(10),
            control_rx,
            cmd_tx,
        );

        let out = drain(&mut stream).await;
        assert_eq!(out[0], Ok(vec![1]));
        assert!(
            out[1]
                .as_ref()
                .expect_err("the resume fails")
                .contains("fell behind")
        );
        assert_eq!(out.len(), 2);
    }

    /// A client cut off partway through a chunked snapshot cannot be resumed,
    /// so it gets the error a client that falls behind gets today.
    #[mz_ore::test(tokio::test)]
    async fn detach_mid_snapshot_errors() {
        let as_of = Timestamp::new(10);
        let first = stream::iter(vec![
            SubscribeEvent::SnapshotChunk(Arc::new(vec![(row(1), as_of, 1)])),
            SubscribeEvent::SnapshotChunk(Arc::new(vec![(row(2), as_of, 1)])),
            SubscribeEvent::Detached {
                buffered_bytes: 2,
                max_buffered_bytes: 1,
            },
        ]);
        let attached = Arc::new(Mutex::new(0));
        let attach: AttachFn = {
            let attached = Arc::clone(&attached);
            Arc::new(move |_, _| {
                *attached.lock().expect("lock poisoned") += 1;
                Box::pin(async { Ok(stream::pending().boxed()) })
            })
        };
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let mut stream = PersistTailStream::new(
            GlobalId::Transient(1),
            Box::pin(async move { Ok(first.boxed()) }),
            attach,
            PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, true),
            formatter(10),
            control_rx,
            cmd_tx,
        );

        let out = drain(&mut stream).await;
        assert_eq!(out[0], Ok(vec![1]));
        assert_eq!(out[1], Ok(vec![2]));
        assert!(
            out[2]
                .as_ref()
                .expect_err("the resume fails")
                .contains("fell behind")
        );
        assert_eq!(out.len(), 3);
        assert_eq!(
            *attached.lock().expect("lock poisoned"),
            0,
            "no resume was attempted"
        );
    }

    /// A terminal message from the coordinator ends the stream ahead of data.
    #[mz_ore::test(tokio::test)]
    async fn coordinator_terminates() {
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let attach: AttachFn = Arc::new(|_, _| Box::pin(async { Ok(stream::pending().boxed()) }));
        let mut stream = PersistTailStream::new(
            GlobalId::Transient(1),
            Box::pin(async { Ok(stream::pending().boxed()) }),
            attach,
            PersistTailBatcher::new(Timestamp::new(10), None, true, vec![], usize::MAX, false),
            formatter(10),
            control_rx,
            cmd_tx,
        );
        control_tx
            .send(PeekResponseUnary::Canceled)
            .expect("stream is alive");
        assert!(matches!(
            stream.next().await,
            Some(PeekResponseUnary::Canceled)
        ));
        assert!(stream.next().await.is_none());
    }
}
