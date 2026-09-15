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
use std::sync::{Arc, Mutex};

use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::ClusterId;
use mz_expr::row::RowCollection;
use mz_expr::{RowComparator, compare_columns};
use mz_ore::cast::CastFrom;
use mz_ore::now::EpochMillis;
use mz_repr::adt::numeric;
use mz_repr::{CatalogItemId, Datum, Diff, GlobalId, IntoRowIterator, Row, RowRef, Timestamp};
use mz_sql::plan::SubscribeOutput;
use mz_storage_types::instances::StorageInstanceId;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::coord::peek::{DroppedDependency, PeekResponseUnary};
use crate::{AdapterError, ExecuteContext, ExecuteResponse};

#[derive(Debug)]
/// A description of an active compute sink from the coordinator's perspective.
pub enum ActiveComputeSink {
    /// An active subscribe sink.
    Subscribe(ActiveSubscribe),
    /// An active copy to sink.
    CopyTo(ActiveCopyTo),
}

impl ActiveComputeSink {
    /// Reports the ID of the cluster on which the sink is running.
    pub fn cluster_id(&self) -> ClusterId {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => subscribe.cluster_id,
            ActiveComputeSink::CopyTo(copy_to) => copy_to.cluster_id,
        }
    }

    /// Reports the cluster holding a compute collection under the sink's ID,
    /// if any. A persist-tail subscribe has none, so it needs no controller
    /// cleanup and does not depend on its cluster.
    pub fn compute_collection_cluster(&self) -> Option<ClusterId> {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => match subscribe.execution {
                SubscribeExecution::Dataflow => Some(subscribe.cluster_id),
                SubscribeExecution::PersistTail => None,
            },
            ActiveComputeSink::CopyTo(copy_to) => Some(copy_to.cluster_id),
        }
    }

    /// Reports the ID of the connection which created the sink.
    pub fn connection_id(&self) -> Option<&ConnectionId> {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => subscribe.connection_id(),
            ActiveComputeSink::CopyTo(copy_to) => Some(&copy_to.conn_id),
        }
    }

    /// Reports the IDs of the objects on which the sink depends.
    pub fn depends_on(&self) -> &BTreeSet<GlobalId> {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => &subscribe.depends_on,
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
    BufferExceeded {
        buffered_bytes: usize,
        max_buffered_bytes: usize,
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
const SUBSCRIBE_MESSAGE_OVERHEAD_BYTES: usize = 1024;

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
    }

    /// Bytes queued behind the message the client is currently draining. That
    /// message is tolerated whatever its size, so only this counts against the
    /// budget.
    pub fn backlog_size(&self) -> usize {
        self.total
            .saturating_sub(self.footprints.front().copied().unwrap_or(0))
    }
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

/// A description of an active subscribe from coord's perspective
#[derive(Debug)]
pub struct ActiveSubscribe {
    /// The owner responsible for retiring the subscribe.
    pub owner: ActiveSubscribeOwner,
    /// The ID of the cluster the subscribe was issued on. A dataflow-executed
    /// subscribe runs there. A persist tail uses no cluster and only reports
    /// it in `mz_subscriptions`.
    pub cluster_id: ClusterId,
    /// The IDs of the objects on which the subscribe depends.
    pub depends_on: BTreeSet<GlobalId>,
    /// Formats the subscribe's batches and queues them for the client.
    pub emitter: SubscribeEmitter,
    /// What produces the subscribe's batches.
    pub execution: SubscribeExecution,
    /// The time when the subscribe started.
    pub start_time: EpochMillis,
    /// If true, this is an internal subscribe that should not appear in
    /// introspection tables like mz_subscriptions.
    pub internal: bool,
}

/// What produces an [`ActiveSubscribe`]'s batches.
#[derive(Debug)]
pub enum SubscribeExecution {
    /// A dataflow on the subscribe's cluster exporting a subscribe sink under
    /// the sink ID. Its batches arrive through the compute controller.
    Dataflow,
    /// A stream in the session that pulls from the persist shard of the
    /// subscribed collection as the client fetches, see
    /// `crate::coord::persist_tail`. No compute collection exists under the
    /// sink ID, and the coordinator reaches the client only through the
    /// emitter's channel.
    PersistTail,
}

/// Turns subscribe batches into client rows and queues them for delivery.
///
/// Clones share the client channel and the backlog accounting, so a producer
/// running off the coordinator loop can deliver batches while the coordinator
/// keeps its own copy for the terminal message.
#[derive(Debug, Clone)]
pub struct SubscribeEmitter {
    /// Channel on which to send responses to the client.
    // The responses have the form `PeekResponseUnary` but should perhaps
    // become `SubscribeResponse`.
    pub channel: mpsc::UnboundedSender<PeekResponseUnary>,
    /// Footprints of the messages queued in `channel` but not yet drained by the
    /// client writer. Shared with the receiver side, which pops as it drains.
    ///
    /// The producer cannot block on a slow client, so instead of applying
    /// backpressure it watches `backlog_bytes` against `max_buffered_bytes`
    /// and retires the subscribe once the backlog exceeds it.
    pub backlog_accounting: Arc<Mutex<SubscribeBacklogAccounting>>,
    /// Budget for the buffered backlog. A snapshot of `subscribe_max_buffered_bytes`
    /// taken when the subscribe was created.
    pub max_buffered_bytes: usize,
    pub formatter: SubscribeFormatter,
}

/// Turns subscribe batches into the rows a client sees.
#[derive(Debug, Clone)]
pub struct SubscribeFormatter {
    /// Whether progress information should be emitted.
    pub emit_progress: bool,
    /// The logical timestamp at which the subscribe began execution.
    pub as_of: Timestamp,
    /// The number of columns in the relation that was subscribed to.
    pub arity: usize,
    /// How to present the subscribe's output.
    pub output: SubscribeOutput,
}

/// The client messages for one formatted batch, each with its payload size.
pub struct FormattedBatch {
    pub messages: Vec<(PeekResponseUnary, usize)>,
    /// Set for the batch at the empty frontier, after which nothing follows.
    pub finished: bool,
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
            } => PeekResponseUnary::Error(AdapterError::SubscribeFellBehind {
                buffered_bytes,
                max_buffered_bytes,
            }),
        };
        self.emitter.send(message, 0);
    }
}

impl SubscribeEmitter {
    /// Initializes the subscription.
    ///
    /// This method must be called exactly once, after constructing a
    /// `SubscribeEmitter` and before calling `process_response`.
    pub fn initialize(&self) {
        if let Some((message, bytes)) = self.formatter.initial_progress() {
            self.send(message, bytes);
        }
    }

    /// Processes a subscribe response from the controller.
    ///
    /// Returns `true` if the subscribe is finished.
    pub fn process_response(&self, batch: SubscribeBatch) -> bool {
        let formatted = self.formatter.format_batch(batch);
        for (message, bytes) in formatted.messages {
            self.send(message, bytes);
        }
        formatted.finished
    }
    /// Bytes queued behind the message the client is currently draining, see
    /// [`SubscribeBacklogAccounting::backlog_size`].
    pub fn backlog_bytes(&self) -> usize {
        self.backlog_accounting
            .lock()
            .expect("subscribe backlog accounting poisoned")
            .backlog_size()
    }

    /// The reason to retire the subscribe with if its backlog exceeds the
    /// budget. Checked after every delivered batch, since `send` itself cannot
    /// retire the subscribe.
    pub fn backlog_exceeded(&self) -> Option<ActiveComputeSinkRetireReason> {
        let buffered_bytes = self.backlog_bytes();
        (buffered_bytes > self.max_buffered_bytes).then_some(
            ActiveComputeSinkRetireReason::BufferExceeded {
                buffered_bytes,
                max_buffered_bytes: self.max_buffered_bytes,
            },
        )
    }

    /// Reports an error to the client. The subscribe must be retired
    /// afterwards, the client stops reading at the error.
    pub fn send_error(&self, error: AdapterError) {
        self.send(PeekResponseUnary::Error(error), 0);
    }

    /// Sends a message to the client if the subscribe has not already completed
    /// and if the client has not already gone away.
    ///
    /// `bytes` is the message's payload size. Its footprint (payload plus a fixed
    /// per-message overhead) is recorded in `backlog_accounting` here and
    /// released by the receiver side when the message is drained. Overflow of
    /// the budget is detected by the producer after `process_response`
    /// returns, see `backlog_exceeded`, because this method cannot retire the
    /// sink.
    fn send(&self, response: PeekResponseUnary, bytes: usize) {
        let footprint = bytes.saturating_add(SUBSCRIBE_MESSAGE_OVERHEAD_BYTES);
        self.backlog_accounting
            .lock()
            .expect("subscribe backlog accounting poisoned")
            .push(footprint);
        let _ = self.channel.send(response);
    }
}

impl SubscribeFormatter {
    /// The progress message announcing the snapshot timestamp, which every
    /// subscribe emits first, if it emits progress at all.
    pub fn initial_progress(&self) -> Option<(PeekResponseUnary, usize)> {
        self.progress_message(&Antichain::from_elem(self.as_of))
    }

    fn progress_message(&self, upper: &Antichain<Timestamp>) -> Option<(PeekResponseUnary, usize)> {
        if !self.emit_progress {
            return None;
        }
        if let Some(upper) = upper.as_option() {
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

            let bytes = row_buf.byte_len();
            let row_iter = Box::new(row_buf.into_row_iter());
            Some((PeekResponseUnary::Rows(row_iter), bytes))
        } else {
            None
        }
    }

    /// Formats one batch into client messages, in delivery order.
    pub fn format_batch(&self, batch: SubscribeBatch) -> FormattedBatch {
        let mut messages = Vec::with_capacity(2);
        let comparator = RowComparator::new(self.output.row_order());
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
                messages.push((
                    PeekResponseUnary::Error(AdapterError::Unstructured(anyhow::Error::msg(s))),
                    0,
                ));
                return FormattedBatch {
                    messages,
                    finished: true,
                };
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
            let mut packer = output_buf.packer();
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
                SubscribeOutput::EnvelopeUpsert { .. }
                | SubscribeOutput::EnvelopeDebezium { .. } => {}
                SubscribeOutput::Diffs | SubscribeOutput::WithinTimestampOrderBy { .. } => {
                    packer.push(Datum::Int64(diff.into_inner()));
                }
            }

            packer.extend_by_row_ref(row);

            output_builder.push(output_buf.as_row_ref(), NonZeroUsize::MIN);
        };

        match &self.output {
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
                let debezium = matches!(self.output, SubscribeOutput::EnvelopeDebezium { .. });
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
                        order_by_keys.len() <= self.arity,
                        "SUBSCRIBE ENVELOPE has more KEY columns ({}) than \
                         relation arity ({}); planner should have rejected this",
                        order_by_keys.len(),
                        self.arity,
                    );
                    let value_columns = self.arity.saturating_sub(order_by_keys.len());
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
                            for idx in 0..self.arity {
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
                                for idx in 0..self.arity {
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
                                for idx in 0..self.arity {
                                    if !order_by_keys.iter().any(|co| co.column == idx) {
                                        packer.push(old_datums[idx]);
                                    }
                                }
                            }
                            for idx in 0..self.arity {
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
        messages.push((PeekResponseUnary::Rows(rows), bytes));

        // Emit progress message if requested. Don't emit progress for the first
        // batch if the upper is exactly `as_of` (we're guaranteed it is not
        // less than `as_of`, but it might be exactly `as_of`) as we've already
        // emitted that progress message in `initial_progress`.
        if !batch.upper.less_equal(&self.as_of) {
            messages.extend(self.progress_message(&batch.upper));
        }

        FormattedBatch {
            messages,
            finished: batch.upper.is_empty(),
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
            } => Err(AdapterError::SubscribeFellBehind {
                buffered_bytes,
                max_buffered_bytes,
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
mod tests {
    use crate::active_compute_sink::SubscribeBacklogAccounting;

    /// The backlog excludes the message being drained, and zero-payload
    /// messages (footprint = overhead only) still accumulate against it.
    #[mz_ore::test]
    fn test_subscribe_backlog_accounting() {
        let mut acc = SubscribeBacklogAccounting::default();
        assert_eq!(acc.backlog_size(), 0);

        // A single large message is fully tolerated: nothing is queued behind it.
        acc.push(10_000);
        assert_eq!(acc.backlog_size(), 0);

        // Near-empty messages (only per-message overhead) still build backlog, so
        // a flood of frontier-only advances cannot grow without bound.
        acc.push(1_024);
        acc.push(1_024);
        assert_eq!(acc.backlog_size(), 2_048);

        // Draining the oldest message advances the tolerated front.
        acc.pop();
        assert_eq!(acc.backlog_size(), 1_024);

        acc.pop();
        acc.pop();
        assert_eq!(acc.backlog_size(), 0);
    }

    /// A client that drains each message before the next is sent never
    /// accumulates backlog, however many messages flow and however large they
    /// are. This is the property that keeps a well-behaved subscribe from ever
    /// being retired, so it is asserted after every step rather than at the end.
    #[mz_ore::test]
    fn test_subscribe_backlog_keeping_up_client() {
        let mut acc = SubscribeBacklogAccounting::default();
        for i in 0..1_000 {
            acc.push(1_024 + i * 4_096);
            assert_eq!(acc.backlog_size(), 0, "message {i} built backlog");
            acc.pop();
            assert_eq!(acc.backlog_size(), 0, "message {i} left backlog behind");
        }
    }
}
