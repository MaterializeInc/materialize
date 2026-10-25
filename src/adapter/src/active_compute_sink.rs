// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator bookkeeping for active compute sinks.

use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::controller::AcceptedBatch;
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::ClusterId;
use mz_expr::row::{RowCollection, RowCollectionBuilder};
use mz_expr::{RowComparator, compare_columns};
use mz_ore::cast::CastFrom;
use mz_ore::now::EpochMillis;
use mz_repr::adt::numeric;
use mz_repr::{CatalogItemId, Datum, Diff, GlobalId, IntoRowIterator, Row, RowRef, Timestamp};
use mz_sql::plan::SubscribeOutput;
use mz_storage_types::instances::StorageInstanceId;
use timely::progress::Antichain;
use tokio::sync::{Notify, mpsc, oneshot};
use uuid::Uuid;

use crate::batched_subscribe::Forwarder;
use crate::coord::peek::{DroppedDependency, PeekResponseUnary};
use crate::error::SubscribeBudget;
use crate::{AdapterError, ExecuteContext, ExecuteResponse};

#[derive(Debug)]
/// A description of an active compute sink from the coordinator's perspective.
pub enum ActiveComputeSink {
    /// An active subscribe sink.
    Subscribe(ActiveSubscribe),
    /// An active `SUBSCRIBE ... WITH (BATCHED)` sink.
    BatchedSubscribe(ActiveBatchedSubscribe),
    /// An active copy to sink.
    CopyTo(ActiveCopyTo),
}

impl ActiveComputeSink {
    /// Reports the ID of the cluster on which the sink is running.
    pub fn cluster_id(&self) -> ClusterId {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => subscribe.cluster_id,
            ActiveComputeSink::BatchedSubscribe(subscribe) => subscribe.cluster_id,
            ActiveComputeSink::CopyTo(copy_to) => copy_to.cluster_id,
        }
    }

    /// Reports the ID of the connection which created the sink.
    pub fn connection_id(&self) -> Option<&ConnectionId> {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => subscribe.connection_id(),
            ActiveComputeSink::BatchedSubscribe(subscribe) => Some(&subscribe.conn_id),
            ActiveComputeSink::CopyTo(copy_to) => Some(&copy_to.conn_id),
        }
    }

    /// Reports the IDs of the objects on which the sink depends.
    pub fn depends_on(&self) -> &BTreeSet<GlobalId> {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => &subscribe.depends_on,
            ActiveComputeSink::BatchedSubscribe(subscribe) => &subscribe.depends_on,
            ActiveComputeSink::CopyTo(copy_to) => &copy_to.depends_on,
        }
    }

    /// Retires the sink with the specified reason.
    ///
    /// This method must be called on every sink before it is dropped. It
    /// informs the end client that the sink is finished for the specified
    /// reason.
    pub fn retire(self, reason: ActiveComputeSinkRetireReason) {
        match self {
            ActiveComputeSink::Subscribe(subscribe) => subscribe.retire(reason),
            ActiveComputeSink::BatchedSubscribe(subscribe) => subscribe.retire(reason),
            ActiveComputeSink::CopyTo(copy_to) => copy_to.retire(reason),
        }
    }
}

/// The reason for removing an [`ActiveComputeSink`].
#[derive(Debug, Clone)]
pub enum ActiveComputeSinkRetireReason {
    /// The compute sink completed successfully.
    Finished,
    /// The compute sink was canceled due to a user request.
    Canceled,
    /// The compute sink was forcibly terminated because an object it depended on
    /// was dropped.
    DependencyDropped(DroppedDependency),
    /// The compute sink was retired because its coordinator-side buffer exceeded
    /// its budget while the client was not reading fast enough. Carries the
    /// buffered and budget byte counts for the terminal error.
    ///
    /// A batched subscribe also retires with this reason when its unread stashed
    /// shares exceed `batched_subscribe_max_unread_stash_bytes`.
    BufferExceeded {
        buffered_bytes: usize,
        max_buffered_bytes: usize,
        budget: SubscribeBudget,
    },
}

/// Overhead charged to every queued message on top of its payload.
///
/// A frontier-only advance carries no rows, so its payload is zero bytes. It
/// still costs real memory: a node in the unbounded channel and an entry in
/// `footprints`. Charging payload alone would leave the backlog flat while
/// those two grow without bound, so the retire check would never trip on a
/// stalled client that only receives progress messages. The fixed charge makes
/// the budget bound message *count* as well as payload bytes.
pub(crate) const SUBSCRIBE_MESSAGE_OVERHEAD_BYTES: usize = 1024;

/// Footprints of the subscribe messages queued to the client but not yet
/// drained, oldest first. The producer pushes one per message sent, the
/// client-writer task pops as it drains. FIFO delivery keeps the queue aligned
/// with the channel.
///
/// The message at the front is the one the client is currently draining. It is
/// always tolerated, however large, so a client working through one big batch is
/// not retired. Only what is queued behind it counts as backlog.
#[derive(Debug, Default)]
pub struct SubscribeBacklogAccounting {
    /// Per-message footprints, in send order.
    footprints: VecDeque<usize>,
    /// Sum of `footprints`.
    total: usize,
    /// Signalled whenever a message is popped.
    drained: Arc<Notify>,
    /// Set once the terminal message was sent. Later sends are dropped.
    closed: bool,
}

impl SubscribeBacklogAccounting {
    /// Records a queued message of the given footprint.
    pub fn push(&mut self, footprint: usize) {
        self.footprints.push_back(footprint);
        self.total = self.total.saturating_add(footprint);
    }

    /// Records the oldest queued message being drained by the client writer.
    pub fn pop(&mut self) {
        if let Some(footprint) = self.footprints.pop_front() {
            self.total = self.total.saturating_sub(footprint);
        }
        self.drained.notify_waiters();
    }

    /// Bytes queued behind the message the client is currently draining. That
    /// message is tolerated whatever its size, so only this counts against the
    /// budget.
    pub fn backlog_size(&self) -> usize {
        self.total
            .saturating_sub(self.footprints.front().copied().unwrap_or(0))
    }

    /// Waits until the [`backlog_size`](Self::backlog_size) of the shared
    /// accounting is at most `bytes`.
    pub async fn wait_until_at_most(this: &Mutex<Self>, bytes: usize) {
        let drained = Arc::clone(&lock(this).drained);
        loop {
            let notified = drained.notified();
            tokio::pin!(notified);
            // Registers for `notify_waiters` before checking, so a pop between
            // the check and the await is not missed.
            notified.as_mut().enable();
            if lock(this).backlog_size() <= bytes {
                return;
            }
            notified.await;
        }
    }
}

fn lock(backlog: &Mutex<SubscribeBacklogAccounting>) -> MutexGuard<'_, SubscribeBacklogAccounting> {
    backlog
        .lock()
        .expect("subscribe backlog accounting poisoned")
}

/// Sends a message to a subscribe client, unless the client has gone away.
///
/// `bytes` is the message's payload size. Its footprint (payload plus a fixed
/// per-message overhead) is recorded in `backlog` here and released by the
/// receiver side when the message is drained.
///
/// Nothing is sent after [`send_last_to_client`].
pub(crate) fn send_to_client(
    channel: &mpsc::UnboundedSender<PeekResponseUnary>,
    backlog: &Mutex<SubscribeBacklogAccounting>,
    response: PeekResponseUnary,
    bytes: usize,
) {
    let footprint = bytes.saturating_add(SUBSCRIBE_MESSAGE_OVERHEAD_BYTES);
    // The send happens under the lock, so that it is ordered with `send_last_to_client`.
    let mut backlog = lock(backlog);
    if backlog.closed {
        return;
    }
    backlog.push(footprint);
    let _ = channel.send(response);
}

/// Sends the terminal message of a subscribe, after which [`send_to_client`] sends nothing to
/// the client.
pub(crate) fn send_last_to_client(
    channel: &mpsc::UnboundedSender<PeekResponseUnary>,
    backlog: &Mutex<SubscribeBacklogAccounting>,
    response: PeekResponseUnary,
) {
    let mut backlog = lock(backlog);
    if backlog.closed {
        return;
    }
    backlog.push(SUBSCRIBE_MESSAGE_OVERHEAD_BYTES);
    let _ = channel.send(response);
    backlog.closed = true;
}

/// Ownership and cleanup scope of an active subscribe.
#[derive(Debug)]
pub enum ActiveSubscribeOwner {
    /// The subscribe belongs to a SQL session.
    Session {
        conn_id: ConnectionId,
        session_uuid: Uuid,
    },
    /// The subscribe belongs to a coordinator background task.
    ///
    /// Always `internal`, since there is no session to attribute a
    /// `mz_subscriptions` row to.
    Background,
}

/// The layout of the rows a subscribe sends to its client.
///
/// Every producer of subscribe output must pack rows through this type so
/// that the bytes on the wire do not depend on the producer.
#[derive(Debug)]
pub struct SubscribeRowFormat {
    /// Whether progress information should be emitted.
    pub(crate) emit_progress: bool,
    /// The number of columns in the relation that was subscribed to.
    pub(crate) arity: usize,
    /// How to present the subscribe's output.
    pub(crate) output: SubscribeOutput,
}

impl SubscribeRowFormat {
    /// Packs one update into `builder`, using `buf` as scratch space.
    ///
    /// The envelope outputs have no `mz_diff` column, so they ignore `diff`.
    pub(crate) fn push_diff_row(
        &self,
        builder: &mut RowCollectionBuilder,
        buf: &mut Row,
        row: &RowRef,
        time: Timestamp,
        diff: Diff,
    ) {
        let mut packer = buf.packer();
        // TODO: Change to MzTimestamp.
        packer.push(Datum::from(numeric::Numeric::from(time)));
        if self.emit_progress {
            // When sinking with PROGRESS, the output includes an
            // additional column that indicates whether a timestamp is
            // complete. For regular "data" updates this is always
            // `false`.
            packer.push(Datum::False);
        }

        match &self.output {
            SubscribeOutput::EnvelopeUpsert { .. } | SubscribeOutput::EnvelopeDebezium { .. } => {}
            SubscribeOutput::Diffs | SubscribeOutput::WithinTimestampOrderBy { .. } => {
                packer.push(Datum::Int64(diff.into_inner()));
            }
        }

        packer.extend_by_row_ref(row);

        builder.push(buf.as_row_ref(), NonZeroUsize::MIN);
    }

    /// Builds the row that announces `upper` as complete.
    ///
    /// Returns `None` if progress is not requested or `upper` is empty.
    pub(crate) fn progress_row(&self, upper: &Antichain<Timestamp>) -> Option<RowCollection> {
        if !self.emit_progress {
            return None;
        }
        let upper = upper.as_option()?;
        let mut row_buf = Row::default();
        let mut packer = row_buf.packer();
        packer.push(Datum::from(numeric::Numeric::from(*upper)));
        packer.push(Datum::True);

        // Fill in the mz_diff or mz_state column
        packer.push(Datum::Null);

        // Fill all table columns with NULL.
        for _ in 0..self.arity {
            packer.push(Datum::Null);
        }

        if let SubscribeOutput::EnvelopeDebezium { order_by_keys } = &self.output {
            for _ in 0..(self.arity - order_by_keys.len()) {
                packer.push(Datum::Null);
            }
        }

        let mut builder = RowCollection::builder(0, 0);
        builder.push(row_buf.as_row_ref(), NonZeroUsize::MIN);
        Some(builder.build())
    }
}

/// A description of an active subscribe from coord's perspective
#[derive(Debug)]
pub struct ActiveSubscribe {
    /// The owner responsible for retiring the subscribe.
    pub owner: ActiveSubscribeOwner,
    /// The ID of the cluster on which the subscribe is running.
    pub cluster_id: ClusterId,
    /// The IDs of the objects on which the subscribe depends.
    pub depends_on: BTreeSet<GlobalId>,
    /// Channel on which to send responses to the client.
    // The responses have the form `PeekResponseUnary` but should perhaps
    // become `SubscribeResponse`.
    pub channel: mpsc::UnboundedSender<PeekResponseUnary>,
    /// Footprints of the messages queued in `channel` but not yet drained by the
    /// client writer. Shared with the receiver side, which pops as it drains.
    ///
    /// The producer runs on the non-blockable coordinator loop and cannot block
    /// on a slow client, so instead of applying backpressure the coordinator
    /// watches `backlog_size` against `max_buffered_bytes` and retires the
    /// subscribe once the backlog exceeds it.
    pub backlog_accounting: Arc<Mutex<SubscribeBacklogAccounting>>,
    /// Budget for the buffered backlog. A snapshot of `subscribe_max_buffered_bytes`
    /// taken when the subscribe was created.
    pub max_buffered_bytes: usize,
    /// How the subscribe's output rows and progress rows are laid out.
    pub format: SubscribeRowFormat,
    /// The logical timestamp at which the subscribe began execution.
    pub as_of: Timestamp,
    /// The time when the subscribe started.
    pub start_time: EpochMillis,
    /// If true, this is an internal subscribe that should not appear in
    /// introspection tables like mz_subscriptions.
    pub internal: bool,
}

impl ActiveSubscribe {
    /// The session uuid for this subscribe's `mz_subscriptions` row, or `None`
    /// if it does not appear there.
    pub fn introspection_session_uuid(&self) -> Option<Uuid> {
        match &self.owner {
            ActiveSubscribeOwner::Session { session_uuid, .. } if !self.internal => {
                Some(*session_uuid)
            }
            _ => None,
        }
    }

    /// Returns the owning connection, if this is a session subscribe.
    pub fn connection_id(&self) -> Option<&ConnectionId> {
        match &self.owner {
            ActiveSubscribeOwner::Session { conn_id, .. } => Some(conn_id),
            ActiveSubscribeOwner::Background => None,
        }
    }

    /// Initializes the subscription.
    ///
    /// This method must be called exactly once, after constructing an
    /// `ActiveSubscribe` and before calling `process_response`.
    pub fn initialize(&self) {
        // Always emit progress message indicating snapshot timestamp.
        self.send_progress_message(&Antichain::from_elem(self.as_of));
    }

    fn send_progress_message(&self, upper: &Antichain<Timestamp>) {
        if let Some(rows) = self.format.progress_row(upper) {
            let bytes = rows.byte_len();
            self.send(
                PeekResponseUnary::Rows(Box::new(rows.into_row_iter())),
                bytes,
            );
        }
    }

    /// Processes a subscribe response from the controller.
    ///
    /// Returns `true` if the subscribe is finished.
    pub fn process_response(&self, batch: SubscribeBatch) -> bool {
        let comparator = RowComparator::new(self.format.output.row_order());
        let rows = match batch.updates {
            Ok(ref rows) => {
                let iters = rows.iter().map(|r| r.iter());
                let merged = mz_ore::iter::merge_iters_by(
                    iters,
                    |(left_row, left_time, _), (right_row, right_time, _)| {
                        left_time.cmp(right_time).then_with(|| {
                            comparator.compare_rows(left_row, right_row, || left_row.cmp(right_row))
                        })
                    },
                );
                mz_ore::iter::consolidate_update_iter(merged)
            }
            Err(s) => {
                self.send(
                    PeekResponseUnary::Error(AdapterError::Unstructured(anyhow::Error::msg(s))),
                    0,
                );
                return true;
            }
        };

        // Sort results by time. We use stable sort here because it will produce
        // deterministic results since the cursor will always produce rows in
        // the same order. Compute doesn't guarantee that the results are sorted
        // (materialize#18936)
        let mut output_buf = Row::default();
        let mut output_builder = RowCollection::builder(0, 0);
        let mut left_datum_vec = mz_repr::DatumVec::new();
        let mut right_datum_vec = mz_repr::DatumVec::new();
        let mut push_row = |row: &RowRef, time: Timestamp, diff: Diff| {
            assert!(self.as_of <= time);
            self.format
                .push_diff_row(&mut output_builder, &mut output_buf, row, time, diff);
        };

        match &self.format.output {
            SubscribeOutput::WithinTimestampOrderBy { order_by } => {
                let mut rows: Vec<_> = rows.collect();
                // Since the diff is inserted as the first column, we can't take advantage of the
                // known ordering. (Aside from timestamp, I suppose.)
                rows.sort_by(
                    |(left_row, left_time, left_diff), (right_row, right_time, right_diff)| {
                        left_time.cmp(right_time).then_with(|| {
                            let mut left_datums = left_datum_vec.borrow();
                            left_datums.extend(&[Datum::Int64(left_diff.into_inner())]);
                            left_datums.extend(left_row.iter());
                            let mut right_datums = right_datum_vec.borrow();
                            right_datums.extend(&[Datum::Int64(right_diff.into_inner())]);
                            right_datums.extend(right_row.iter());
                            compare_columns(order_by, &left_datums, &right_datums, || {
                                left_row.cmp(right_row).then(left_diff.cmp(right_diff))
                            })
                        })
                    },
                );
                for (row, time, diff) in rows {
                    push_row(row, *time, diff);
                }
            }
            SubscribeOutput::EnvelopeUpsert { order_by_keys }
            | SubscribeOutput::EnvelopeDebezium { order_by_keys } => {
                let debezium =
                    matches!(self.format.output, SubscribeOutput::EnvelopeDebezium { .. });
                let mut it = rows.peekable();
                let mut datum_vec = mz_repr::DatumVec::new();
                let mut old_datum_vec = mz_repr::DatumVec::new();
                let comparator = RowComparator::new(order_by_keys.as_slice());
                let mut group = Vec::with_capacity(2);
                let mut row_buf = Row::default();
                // The iterator is sorted by time and key, so elements in the same group should be
                // adjacent already.
                while let Some(start) = it.next() {
                    group.clear();
                    group.push(start);
                    while let Some(row) = it.peek()
                        && start.1 == row.1
                        && {
                            comparator
                                .compare_rows(start.0, row.0, || Ordering::Equal)
                                .is_eq()
                        }
                    {
                        group.extend(it.next());
                    }
                    group.sort_by_key(|(_, _, d)| *d);

                    // Four cases:
                    // [(key, value, +1)] => ("insert", key, NULL, value)
                    // [(key, v1, -1), (key, v2, +1)] => ("upsert", key, v1, v2)
                    // [(key, value, -1)] => ("delete", key, value, NULL)
                    // everything else => ("key_violation", key, NULL, NULL)
                    // Defense in depth: the planner ensures that KEY columns are
                    // distinct columns of the underlying relation, so this
                    // subtraction must never underflow. If it does, we'd OOM
                    // the coordinator with a giant loop, so check it here.
                    mz_ore::soft_assert_or_log!(
                        order_by_keys.len() <= self.format.arity,
                        "SUBSCRIBE ENVELOPE has more KEY columns ({}) than \
                         relation arity ({}); planner should have rejected this",
                        order_by_keys.len(),
                        self.format.arity,
                    );
                    let value_columns = self.format.arity.saturating_sub(order_by_keys.len());
                    let mut packer = row_buf.packer();
                    match &group[..] {
                        [(row, _, Diff::ONE)] => {
                            packer.push(if debezium {
                                Datum::String("insert")
                            } else {
                                Datum::String("upsert")
                            });
                            let datums = datum_vec.borrow_with(row);
                            for column_order in order_by_keys {
                                packer.push(datums[column_order.column]);
                            }
                            if debezium {
                                for _ in 0..value_columns {
                                    packer.push(Datum::Null);
                                }
                            }
                            for idx in 0..self.format.arity {
                                if !order_by_keys.iter().any(|co| co.column == idx) {
                                    packer.push(datums[idx]);
                                }
                            }
                            push_row(row_buf.as_row_ref(), *start.1, Diff::ZERO)
                        }
                        [(_, _, Diff::MINUS_ONE)] => {
                            packer.push(Datum::String("delete"));
                            let datums = datum_vec.borrow_with(start.0);
                            for column_order in order_by_keys {
                                packer.push(datums[column_order.column]);
                            }
                            if debezium {
                                for idx in 0..self.format.arity {
                                    if !order_by_keys.iter().any(|co| co.column == idx) {
                                        packer.push(datums[idx]);
                                    }
                                }
                            }
                            for _ in 0..value_columns {
                                packer.push(Datum::Null);
                            }
                            push_row(row_buf.as_row_ref(), *start.1, Diff::ZERO)
                        }
                        [(old_row, _, Diff::MINUS_ONE), (row, _, Diff::ONE)] => {
                            packer.push(Datum::String("upsert"));
                            let datums = datum_vec.borrow_with(row);
                            let old_datums = old_datum_vec.borrow_with(old_row);

                            for column_order in order_by_keys {
                                packer.push(datums[column_order.column]);
                            }
                            if debezium {
                                for idx in 0..self.format.arity {
                                    if !order_by_keys.iter().any(|co| co.column == idx) {
                                        packer.push(old_datums[idx]);
                                    }
                                }
                            }
                            for idx in 0..self.format.arity {
                                if !order_by_keys.iter().any(|co| co.column == idx) {
                                    packer.push(datums[idx]);
                                }
                            }
                            push_row(row_buf.as_row_ref(), *start.1, Diff::ZERO)
                        }
                        _ => {
                            packer.push(Datum::String("key_violation"));
                            let datums = datum_vec.borrow_with(start.0);
                            for column_order in order_by_keys {
                                packer.push(datums[column_order.column]);
                            }
                            if debezium {
                                for _ in 0..value_columns {
                                    packer.push(Datum::Null);
                                }
                            }
                            for _ in 0..value_columns {
                                packer.push(Datum::Null);
                            }
                            push_row(row_buf.as_row_ref(), *start.1, Diff::ZERO)
                        }
                    };
                }
            }
            SubscribeOutput::Diffs => {
                // Diffs output is sorted by time and row, so it can be pushed directly.
                for (row, time, diff) in rows {
                    push_row(row, *time, diff)
                }
            }
        };

        let rows = output_builder.build();
        let bytes = rows.byte_len();
        let rows = Box::new(rows.into_row_iter());
        self.send(PeekResponseUnary::Rows(rows), bytes);

        // Emit progress message if requested. Don't emit progress for the first
        // batch if the upper is exactly `as_of` (we're guaranteed it is not
        // less than `as_of`, but it might be exactly `as_of`) as we've already
        // emitted that progress message in `initialize`.
        if !batch.upper.less_equal(&self.as_of) {
            self.send_progress_message(&batch.upper);
        }

        batch.upper.is_empty()
    }

    /// Retires the subscribe with the specified reason.
    ///
    /// This method must be called on every subscribe before it is dropped. It
    /// informs the end client that the subscribe is finished for the specified
    /// reason.
    pub fn retire(self, reason: ActiveComputeSinkRetireReason) {
        let message = match reason {
            ActiveComputeSinkRetireReason::Finished => return,
            ActiveComputeSinkRetireReason::Canceled => PeekResponseUnary::Canceled,
            ActiveComputeSinkRetireReason::DependencyDropped(d) => {
                PeekResponseUnary::DependencyDropped(d)
            }
            ActiveComputeSinkRetireReason::BufferExceeded {
                buffered_bytes,
                max_buffered_bytes,
                budget,
            } => PeekResponseUnary::Error(AdapterError::SubscribeFellBehind {
                buffered_bytes,
                max_buffered_bytes,
                budget,
            }),
        };
        self.send(message, 0);
    }

    /// Sends a message to the client if the subscribe has not already completed
    /// and if the client has not already gone away.
    ///
    /// See [`send_to_client`] for the accounting of `bytes`. Overflow of the
    /// budget is detected by the coordinator after `process_response` returns,
    /// not here, because this method cannot retire the sink.
    fn send(&self, response: PeekResponseUnary, bytes: usize) {
        send_to_client(&self.channel, &self.backlog_accounting, response, bytes);
    }
}

/// A description of an active `SUBSCRIBE ... WITH (BATCHED)` from the coordinator's perspective.
///
/// Its `Forwarder` sends the subscribe's rows to the client. The coordinator hands it every
/// accepted batch and retires the subscribe once the forwarder reports a `ForwarderEvent`.
#[derive(Debug)]
pub struct ActiveBatchedSubscribe {
    /// The ID of the connection that created the subscribe.
    pub conn_id: ConnectionId,
    /// The UUID of the session that created the subscribe.
    pub session_uuid: Uuid,
    /// The ID of the cluster on which the subscribe is running.
    pub cluster_id: ClusterId,
    /// The IDs of the objects on which the subscribe depends.
    pub depends_on: BTreeSet<GlobalId>,
    /// The logical timestamp at which the subscribe began execution.
    pub as_of: Timestamp,
    /// The time when the subscribe started.
    pub start_time: EpochMillis,
    /// The channel to the client, shared with the forwarder.
    pub channel: mpsc::UnboundedSender<PeekResponseUnary>,
    /// The footprints of the messages queued in `channel`, shared with the forwarder and the
    /// receiver side.
    pub backlog_accounting: Arc<Mutex<SubscribeBacklogAccounting>>,
    /// The bound on the client backlog plus the forwarder's unread inline shares. A snapshot of
    /// `subscribe_max_buffered_bytes`.
    pub max_buffered_bytes: usize,
    /// The bound on the forwarder's unread stashed shares. A snapshot of
    /// `batched_subscribe_max_unread_stash_bytes`.
    pub max_unread_stash_bytes: usize,
    /// The task that sends the subscribe's batches to the client.
    pub(crate) forwarder: Forwarder,
}

impl ActiveBatchedSubscribe {
    /// Hands an accepted batch to the forwarder.
    ///
    /// Returns the reason to retire the subscribe with, if the batch pushed it past one of its
    /// bounds.
    pub(crate) fn send(&self, batch: AcceptedBatch) -> Option<ActiveComputeSinkRetireReason> {
        self.forwarder.send(batch);

        let backlog = lock(&self.backlog_accounting).backlog_size();
        let buffered_bytes = backlog.saturating_add(self.forwarder.unread_inline_bytes());
        if buffered_bytes > self.max_buffered_bytes {
            return Some(ActiveComputeSinkRetireReason::BufferExceeded {
                buffered_bytes,
                max_buffered_bytes: self.max_buffered_bytes,
                budget: SubscribeBudget::BufferedBytes,
            });
        }
        let unread_stash_bytes = self.forwarder.unread_stash_bytes();
        if unread_stash_bytes > self.max_unread_stash_bytes {
            return Some(ActiveComputeSinkRetireReason::BufferExceeded {
                buffered_bytes: unread_stash_bytes,
                max_buffered_bytes: self.max_unread_stash_bytes,
                budget: SubscribeBudget::UnreadStashBytes,
            });
        }
        None
    }

    /// Retires the subscribe with the specified reason.
    ///
    /// A `Finished` subscribe was ended by its forwarder, which already sent the last message.
    /// For any other reason the client receives the matching terminal message. Dropping the
    /// forwarder deletes the stashed shares it still holds.
    pub fn retire(self, reason: ActiveComputeSinkRetireReason) {
        let message = match reason {
            ActiveComputeSinkRetireReason::Finished => None,
            ActiveComputeSinkRetireReason::Canceled => Some(PeekResponseUnary::Canceled),
            ActiveComputeSinkRetireReason::DependencyDropped(d) => {
                Some(PeekResponseUnary::DependencyDropped(d))
            }
            ActiveComputeSinkRetireReason::BufferExceeded {
                buffered_bytes,
                max_buffered_bytes,
                budget,
            } => Some(PeekResponseUnary::Error(
                AdapterError::SubscribeFellBehind {
                    buffered_bytes,
                    max_buffered_bytes,
                    budget,
                },
            )),
        };
        // Dropping the forwarder aborts its task, but the task can still be in a send on
        // another thread. `send_last_to_client` closes the channel to every later send, so the
        // terminal message is the last one the client receives.
        drop(self.forwarder);
        if let Some(message) = message {
            send_last_to_client(&self.channel, &self.backlog_accounting, message);
        }
    }
}

/// A description of an active copy to sink from the coordinator's perspective.
#[derive(Debug)]
pub struct ActiveCopyTo {
    /// The ID of the connection which created the subscribe.
    pub conn_id: ConnectionId,
    /// The result channel for the `COPY ... TO` statement that created the copy to sink.
    pub tx: oneshot::Sender<Result<ExecuteResponse, AdapterError>>,
    /// The ID of the cluster on which the copy to is running.
    pub cluster_id: ClusterId,
    /// The IDs of the objects on which the copy to depends.
    pub depends_on: BTreeSet<GlobalId>,
}

impl ActiveCopyTo {
    /// Retires the copy to with a response from the controller.
    ///
    /// Unlike subscribes, copy tos only expect a single response from the
    /// controller, so `process_response` and `retire` are unified into a single
    /// operation.
    ///
    /// Either this method or `retire` must be called on every copy to before it
    /// is dropped.
    pub fn retire_with_response(self, response: Result<u64, anyhow::Error>) {
        let response = match response {
            Ok(n) => Ok(ExecuteResponse::Copied(usize::cast_from(n))),
            Err(error) => Err(AdapterError::Unstructured(error)),
        };
        let _ = self.tx.send(response);
    }

    /// Retires the copy to with the specified reason.
    ///
    /// Either this method or `retire_with_response` must be called on every
    /// copy to before it is dropped.
    pub fn retire(self, reason: ActiveComputeSinkRetireReason) {
        let message = match reason {
            ActiveComputeSinkRetireReason::Finished => return,
            ActiveComputeSinkRetireReason::Canceled => Err(AdapterError::Canceled),
            ActiveComputeSinkRetireReason::DependencyDropped(dep) => {
                Err(dep.to_concurrent_dependency_drop())
            }
            ActiveComputeSinkRetireReason::BufferExceeded {
                buffered_bytes,
                max_buffered_bytes,
                budget,
            } => Err(AdapterError::SubscribeFellBehind {
                buffered_bytes,
                max_buffered_bytes,
                budget,
            }),
        };
        let _ = self.tx.send(message);
    }
}

/// State we keep in the `Coordinator` to track active `COPY FROM` statements.
#[derive(Debug)]
pub(crate) struct ActiveCopyFrom {
    /// ID of the ingestion running in clusterd.
    pub ingestion_id: uuid::Uuid,
    /// The cluster this is currently running on.
    pub cluster_id: StorageInstanceId,
    /// The table we're currently copying into.
    pub table_id: CatalogItemId,
    /// Context of the SQL session that ran the statement.
    pub ctx: ExecuteContext,
}

#[cfg(test)]
mod tests;
