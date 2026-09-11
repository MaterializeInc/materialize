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
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_expr::{ColumnOrder, compare_columns};
use mz_ore::iter::consolidate_update_iter;
use mz_repr::{DatumVec, Diff, GlobalId, Row, Timestamp, UpdateCollection};
use mz_sql::plan::{SubscribeFrom, SubscribePlan};
use mz_storage_client::storage_collections::{StorageCollections, SubscribeEvent, Update};
use mz_storage_types::controller::StorageError;
use timely::PartialOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
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

/// Cuts a collection's snapshot-then-listen stream into [`SubscribeBatch`]es
/// with the contents and boundaries the compute subscribe sink produces
/// (`mz_compute::sink::subscribe`): one batch per frontier advance at or past
/// `as_of`, holding the consolidated updates below the new frontier in the
/// subscribe's row order, and a closing batch at the empty frontier once
/// `up_to` is reached.
#[derive(Debug)]
pub(crate) struct PersistTailBatcher {
    as_of: Antichain<Timestamp>,
    up_to: Antichain<Timestamp>,
    with_snapshot: bool,
    /// The subscribe's row order within a timestamp, see
    /// `SubscribeOutput::row_order`.
    order: Vec<ColumnOrder>,
    /// A batch whose rows exceed this many bytes is replaced by an error.
    max_result_size: usize,
    /// Upper of the last batch produced, so the lower of the next one.
    prev_upper: Antichain<Timestamp>,
    /// Updates at or beyond `prev_upper`, not yet shipped.
    rows: Vec<(Row, Timestamp, Diff)>,
    errors: Vec<(String, Timestamp, Diff)>,
    /// Once an error is reported every later batch repeats it. Like the
    /// compute sink, the subscribe protocol cannot retract an error
    /// (database-issues#5182).
    poison: Option<String>,
    /// Whether the snapshot may be shipped in pieces, see
    /// [`Self::push_snapshot_chunk`].
    chunk_snapshot: bool,
    /// Set once a piece of the snapshot has been shipped. The frontier does
    /// not record this, so it is what makes a resume impossible.
    snapshot_shipped: bool,
    finished: bool,
}

impl PersistTailBatcher {
    /// `chunk_snapshot` lets the snapshot ship in pieces, which the caller
    /// allows only for an output whose rows are independent of one another
    /// within a timestamp, see [`Self::push_snapshot_chunk`].
    pub fn new(
        as_of: Timestamp,
        up_to: Option<Timestamp>,
        with_snapshot: bool,
        order: Vec<ColumnOrder>,
        max_result_size: usize,
        chunk_snapshot: bool,
    ) -> Self {
        Self {
            as_of: Antichain::from_elem(as_of),
            up_to: up_to.map(Antichain::from_elem).unwrap_or_default(),
            with_snapshot,
            order,
            max_result_size,
            prev_upper: Antichain::from_elem(TimelyTimestamp::minimum()),
            rows: Vec::new(),
            errors: Vec::new(),
            poison: None,
            chunk_snapshot,
            snapshot_shipped: false,
            finished: false,
        }
    }

    /// Buffers updates from the stream. The snapshot arrives at `as_of` and is
    /// dropped without `with_snapshot`. Nothing at or beyond `up_to` is ever
    /// emitted.
    pub fn push(&mut self, updates: &[Update]) {
        for (data, time, diff) in updates {
            if !self.should_emit(time) {
                continue;
            }
            let diff = Diff::from(*diff);
            match &data.0 {
                Ok(row) => self.rows.push((row.clone(), *time, diff)),
                Err(error) => self.errors.push((error.to_string(), *time, diff)),
            }
        }
    }

    fn should_emit(&self, time: &Timestamp) -> bool {
        let beyond_as_of = if self.with_snapshot {
            self.as_of.less_equal(time)
        } else {
            self.as_of.less_than(time)
        };
        beyond_as_of && !self.up_to.less_equal(time)
    }

    /// Buffers a consolidated piece of the snapshot and returns the batch that
    /// ships it, which is what keeps a large snapshot from being held whole.
    ///
    /// The batch claims no frontier advance: its bounds are both the `as_of`,
    /// so the formatter emits its rows and holds the progress message until
    /// the timestamp completes. The client therefore sees the same rows in the
    /// same places as it would from one batch, but earlier and in pieces.
    ///
    /// Shipping early gives up the frontier's account of what the subscriber
    /// has seen, see [`Self::resume_point`], and it costs the check against
    /// `max_result_size` across the whole snapshot, which now bounds a chunk.
    /// Without `chunk_snapshot` the updates are only buffered, like
    /// [`Self::push`].
    pub fn push_snapshot_chunk(&mut self, updates: &[Update]) -> Option<SubscribeBatch> {
        self.push(updates);
        if !self.chunk_snapshot || self.finished {
            return None;
        }
        // Only the snapshot's own timestamp is complete in a chunk. Nothing
        // later is expected here, since chunks arrive before any other event,
        // but a later update would have to wait for its frontier.
        let as_of = *self.as_of.as_option().expect("as_of is never empty");
        self.sort_rows();
        self.sort_errors();
        let split = self.rows.partition_point(|(_, t, _)| *t <= as_of);
        let rows = self.take_rows(split);
        let split = self.errors.partition_point(|(_, t, _)| *t <= as_of);
        let errors = self.take_errors(split);
        if rows.len() == 0 && errors.is_empty() && self.poison.is_none() {
            return None;
        }
        self.snapshot_shipped = true;
        let mut batch = SubscribeBatch {
            lower: self.as_of.clone(),
            upper: self.as_of.clone(),
            updates: self.updates_or_error(rows, errors),
        };
        batch.to_error_if_exceeds(self.max_result_size);
        Some(batch)
    }

    /// Records that every update below `upper` has been pushed and returns the
    /// batches this completes. Empty once the batcher is finished.
    pub fn progress(&mut self, upper: Antichain<Timestamp>) -> Vec<SubscribeBatch> {
        if self.finished {
            return Vec::new();
        }
        let mut batches = Vec::new();
        batches.extend(self.batch(upper.clone()));
        if PartialOrder::less_equal(&self.up_to, &upper) {
            self.finished = true;
            batches.extend(self.batch(Antichain::new()));
        }
        batches
    }

    /// Ends the subscribe early, so a client sees an end instead of a stall
    /// when the stream stops before the collection closes.
    pub fn close(&mut self) -> Option<SubscribeBatch> {
        if self.finished {
            return None;
        }
        self.finished = true;
        self.batch(Antichain::new())
    }

    /// Where to resume reading after the stream was cut off: the `as_of` and
    /// whether a snapshot is needed. Everything below `prev_upper` has been
    /// shipped, so a new stream at `prev_upper - 1` without a snapshot emits
    /// exactly the updates from `prev_upper` on. Before the first batch the
    /// subscribe starts over, snapshot included.
    ///
    /// `None` once part of the snapshot has shipped but its timestamp has not
    /// completed: the frontier does not say how much of it went out, and
    /// reading it again would repeat rows the client already has.
    ///
    /// Updates pushed since the last batch are dropped: the new stream emits
    /// them again.
    pub fn resume_point(&mut self) -> Option<(Timestamp, bool)> {
        self.rows.clear();
        self.errors.clear();
        match self.prev_upper.as_option() {
            Some(upper) if *upper != Timestamp::minimum() => Some((
                upper
                    .checked_sub(1)
                    .expect("a shipped upper is past the minimum"),
                false,
            )),
            _ if self.snapshot_shipped => None,
            _ => Some((
                *self.as_of.as_option().expect("as_of is never empty"),
                self.with_snapshot,
            )),
        }
    }

    /// The frontier everything shipped so far lies below.
    pub fn frontier(&self) -> &Antichain<Timestamp> {
        &self.prev_upper
    }

    fn batch(&mut self, upper: Antichain<Timestamp>) -> Option<SubscribeBatch> {
        // Like the compute sink: no batch before the frontier reaches `as_of`,
        // and none when the frontier did not move.
        if !PartialOrder::less_equal(&self.as_of, &upper) || upper == self.prev_upper {
            return None;
        }

        let rows = self.take_rows_below(&upper);
        let errors = self.take_errors_below(&upper);
        let updates = self.updates_or_error(rows, errors);

        let mut batch = SubscribeBatch {
            lower: std::mem::replace(&mut self.prev_upper, upper.clone()),
            upper,
            updates,
        };
        batch.to_error_if_exceeds(self.max_result_size);
        Some(batch)
    }

    /// A batch's updates, or the error that replaces them. The first error a
    /// subscribe sees poisons every later batch.
    fn updates_or_error(
        &mut self,
        rows: UpdateCollection,
        errors: Vec<(String, Timestamp, Diff)>,
    ) -> Result<Vec<UpdateCollection>, String> {
        match (&self.poison, errors.first()) {
            (Some(error), _) => Err(error.clone()),
            (None, Some((error, _, _))) => {
                self.poison = Some(error.clone());
                Err(error.clone())
            }
            (None, None) => Ok(vec![rows]),
        }
    }

    /// Removes and returns the rows below `upper`, sorted by time and row order
    /// and consolidated, as `SubscribeFormatter::format_batch` expects.
    fn take_rows_below(&mut self, upper: &Antichain<Timestamp>) -> UpdateCollection {
        self.sort_rows();
        let split = self.rows.partition_point(|(_, t, _)| !upper.less_equal(t));
        self.take_rows(split)
    }

    /// Removes and returns the first `split` rows. The caller sorts first with
    /// [`Self::sort_rows`], which both places the split and puts the rows in
    /// the order the formatter expects.
    fn take_rows(&mut self, split: usize) -> UpdateCollection {
        let shipped = self.rows.drain(..split).collect::<Vec<_>>();
        let byte_len = shipped.iter().map(|(row, _, _)| row.byte_len()).sum();
        let mut builder = UpdateCollection::builder(byte_len, shipped.len());
        let updates = shipped.iter().map(|(row, t, d)| (row.as_row_ref(), *t, *d));
        for (row, time, diff) in consolidate_update_iter(updates) {
            builder.push((row, &time, diff));
        }
        builder.build()
    }

    fn sort_rows(&mut self) {
        let order = self.order.as_slice();
        let mut left_datums = DatumVec::new();
        let mut right_datums = DatumVec::new();
        self.rows.sort_unstable_by(|(r0, t0, _), (r1, t1, _)| {
            t0.cmp(t1).then_with(|| {
                let left = left_datums.borrow_with(r0);
                let right = right_datums.borrow_with(r1);
                compare_columns(order, &left, &right, || r0.cmp(r1))
            })
        });
    }

    fn take_errors_below(
        &mut self,
        upper: &Antichain<Timestamp>,
    ) -> Vec<(String, Timestamp, Diff)> {
        self.sort_errors();
        let split = self
            .errors
            .partition_point(|(_, t, _)| !upper.less_equal(t));
        self.take_errors(split)
    }

    /// Removes and returns the first `split` errors, consolidated. The caller
    /// sorts first with [`Self::sort_errors`].
    fn take_errors(&mut self, split: usize) -> Vec<(String, Timestamp, Diff)> {
        consolidate_update_iter(self.errors.drain(..split)).collect()
    }

    fn sort_errors(&mut self) {
        self.errors
            .sort_unstable_by(|(e0, t0, _), (e1, t1, _)| t0.cmp(t1).then_with(|| e0.cmp(e1)));
    }
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
    use mz_repr::Datum;
    use mz_sql::plan::SubscribeOutput;
    use mz_storage_types::sources::SourceData;

    use super::*;

    fn row(i: i64) -> SourceData {
        SourceData(Ok(Row::pack_slice(&[Datum::Int64(i)])))
    }

    fn rows(batch: &SubscribeBatch) -> Vec<(i64, u64, i64)> {
        batch
            .updates
            .as_ref()
            .expect("no error")
            .iter()
            .flat_map(|updates| {
                updates
                    .iter()
                    .map(|(row, time, diff)| {
                        (
                            row.unpack_first().unwrap_int64(),
                            u64::from(*time),
                            diff.into_inner(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The snapshot and later updates come out consolidated, at the frontier
    /// advances a compute subscribe sink would report them at, and the
    /// batches chain through their lower and upper frontiers.
    #[mz_ore::test]
    fn batches_follow_progress() {
        let mut batcher =
            PersistTailBatcher::new(Timestamp::new(10), None, true, vec![], usize::MAX, false);
        // Snapshot at 10, with a duplicate to consolidate and a listen update
        // at 12 that is not yet complete at frontier 12.
        batcher.push(&[
            (row(2), Timestamp::new(10), 1),
            (row(1), Timestamp::new(10), 1),
            (row(2), Timestamp::new(10), 1),
            (row(3), Timestamp::new(12), 1),
        ]);

        assert!(
            batcher
                .progress(Antichain::from_elem(Timestamp::new(9)))
                .is_empty()
        );

        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].lower, Antichain::from_elem(Timestamp::minimum()));
        assert_eq!(batches[0].upper, Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(rows(&batches[0]), vec![(1, 10, 1), (2, 10, 2)]);

        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(13)));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].lower, Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(rows(&batches[0]), vec![(3, 12, 1)]);
    }

    /// Without a snapshot the updates at `as_of` are dropped, and reaching
    /// `up_to` closes the stream with a batch at the empty frontier that
    /// excludes updates at or beyond `up_to`.
    #[mz_ore::test]
    fn no_snapshot_and_up_to() {
        let mut batcher = PersistTailBatcher::new(
            Timestamp::new(10),
            Some(Timestamp::new(12)),
            false,
            vec![],
            usize::MAX,
            false,
        );
        batcher.push(&[
            (row(1), Timestamp::new(10), 1),
            (row(2), Timestamp::new(11), 1),
            (row(3), Timestamp::new(12), 1),
        ]);
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(13)));
        assert_eq!(batches.len(), 2);
        assert_eq!(rows(&batches[0]), vec![(2, 11, 1)]);
        assert!(batches[1].upper.is_empty());
        assert!(rows(&batches[1]).is_empty());
        assert!(
            batcher
                .progress(Antichain::from_elem(Timestamp::new(14)))
                .is_empty()
        );
    }

    /// An error in the collection poisons every batch from the one that
    /// reports it onwards.
    #[mz_ore::test]
    fn errors_poison() {
        let mut batcher =
            PersistTailBatcher::new(Timestamp::new(10), None, true, vec![], usize::MAX, false);
        let error = SourceData(Err(mz_storage_types::errors::DataflowError::from(
            mz_expr::EvalError::DivisionByZero,
        )));
        batcher.push(&[
            (row(1), Timestamp::new(10), 1),
            (error, Timestamp::new(11), 1),
        ]);

        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(11)));
        assert_eq!(rows(&batches[0]), vec![(1, 10, 1)]);

        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(12)));
        assert!(batches[0].updates.is_err());
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(13)));
        assert!(batches[0].updates.is_err());
    }

    /// Before any batch shipped, resuming starts over with the snapshot. After
    /// one, it resumes one below the shipped frontier without a snapshot, and
    /// unshipped updates are dropped so the new stream can deliver them.
    #[mz_ore::test]
    fn resume_point() {
        let mut batcher =
            PersistTailBatcher::new(Timestamp::new(10), None, true, vec![], usize::MAX, false);
        batcher.push(&[(row(1), Timestamp::new(10), 1)]);
        assert_eq!(batcher.resume_point(), Some((Timestamp::new(10), true)));
        assert!(batcher.rows.is_empty());

        batcher.push(&[(row(1), Timestamp::new(10), 1)]);
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(rows(&batches[0]), vec![(1, 10, 1)]);
        batcher.push(&[(row(2), Timestamp::new(12), 1)]);
        assert_eq!(batcher.resume_point(), Some((Timestamp::new(11), false)));
        assert!(batcher.rows.is_empty());
    }

    /// Chunks of the snapshot ship as they arrive, each as its own batch at
    /// the `as_of`, and the timestamp's progress waits for the frontier.
    #[mz_ore::test]
    fn snapshot_ships_in_chunks() {
        let as_of = Timestamp::new(10);
        let mut batcher = PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, true);
        let first = batcher
            .push_snapshot_chunk(&[(row(1), as_of, 1), (row(2), as_of, 1)])
            .expect("chunk ships");
        assert_eq!(rows(&first), vec![(1, 10, 1), (2, 10, 1)]);
        assert_eq!(first.lower, Antichain::from_elem(as_of));
        assert_eq!(first.upper, Antichain::from_elem(as_of));

        let second = batcher
            .push_snapshot_chunk(&[(row(3), as_of, 1)])
            .expect("chunk ships");
        assert_eq!(rows(&second), vec![(3, 10, 1)]);

        // The listen updates that follow still wait for the frontier.
        batcher.push(&[(row(4), Timestamp::new(11), 1)]);
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(11)));
        assert_eq!(batches.len(), 1);
        assert!(rows(&batches[0]).is_empty());
        assert_eq!(batches[0].upper, Antichain::from_elem(Timestamp::new(11)));
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(rows(&batches[0]), vec![(4, 11, 1)]);
    }

    /// An output that orders or groups a timestamp needs all of it, so its
    /// chunks are only buffered and ship with the timestamp.
    #[mz_ore::test]
    fn ordered_output_holds_the_snapshot() {
        let as_of = Timestamp::new(10);
        let mut batcher = PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, false);
        assert!(batcher.push_snapshot_chunk(&[(row(2), as_of, 1)]).is_none());
        assert!(batcher.push_snapshot_chunk(&[(row(1), as_of, 1)]).is_none());
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(11)));
        assert_eq!(rows(&batches[0]), vec![(1, 10, 1), (2, 10, 1)]);
    }

    /// Once part of the snapshot is out there is nothing to resume from: the
    /// frontier never advanced, and reading again would repeat those rows.
    #[mz_ore::test]
    fn resume_point_after_a_shipped_chunk() {
        let as_of = Timestamp::new(10);
        let mut batcher = PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, true);
        assert!(batcher.push_snapshot_chunk(&[(row(1), as_of, 1)]).is_some());
        assert_eq!(batcher.resume_point(), None);

        // Once the snapshot's timestamp completes the frontier covers it again.
        let _ = batcher.progress(Antichain::from_elem(Timestamp::new(11)));
        assert_eq!(batcher.resume_point(), Some((Timestamp::new(10), false)));
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
